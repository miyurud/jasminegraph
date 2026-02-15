/**
Copyright 2021 JasmineGraph Team
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

#include "SheepTriangleCountExecutor.h"
#include "TriangleCountExecutor.h"

#include <time.h>
#include <unistd.h>

#include "../../../../../globals.h"
#include "../../../../k8s/K8sWorkerController.h"
#include "../../../../scale/scaler.h"
#include "../../../../util/telemetry/OpenTelemetryUtil.h"

using namespace std::chrono;

Logger sheepTriangleCount_logger;

// Use the same static variables from TriangleCountExecutor for consistency
extern std::mutex processStatusMutex;
extern std::mutex responseVectorMutex;
extern bool isStatCollect;
extern time_t last_exec_time;

SheepTriangleCountExecutor::SheepTriangleCountExecutor() {}

SheepTriangleCountExecutor::SheepTriangleCountExecutor(SQLiteDBInterface *db, PerformanceSQLiteDBInterface *perfDb,
                                                       JobRequest jobRequest) {
    this->sqlite = db;
    this->perfDB = perfDb;
    this->request = jobRequest;
}

int SheepTriangleCountExecutor::getUid() {
    static int counter = 0;
    return counter++;
}

void SheepTriangleCountExecutor::execute() {
    // Start automatic OpenTelemetry tracing for sheep triangle count execution
    std::string graphId = request.getParameter(Conts::PARAM_KEYS::GRAPH_ID);
    OTEL_TRACE_FUNCTION();

    schedulerMutex.lock();
    time_t curr_time = time(NULL);
    // 8 seconds = upper bound to the time to send performance metrics after allocating task to a worker
    if (curr_time < last_exec_time + 8) {
        sleep(last_exec_time + 9 - curr_time);  // 9 = 8+1 to ensure it waits more than 8 seconds
    }
    int uniqueId = getUid();
    std::string masterIP = request.getMasterIP();
    std::string canCalibrateString = request.getParameter(Conts::PARAM_KEYS::CAN_CALIBRATE);
    std::string queueTime = request.getParameter(Conts::PARAM_KEYS::QUEUE_TIME);

    bool canCalibrate = Utils::parseBoolean(canCalibrateString);
    int threadPriority = request.getPriority();

    std::string autoCalibrateString = request.getParameter(Conts::PARAM_KEYS::AUTO_CALIBRATION);
    bool autoCalibrate = Utils::parseBoolean(autoCalibrateString);

    if (threadPriority == Conts::HIGH_PRIORITY_DEFAULT_VALUE) {
        highPriorityGraphList.push_back(graphId);
    }

    // Below code is used to update the process details
    processStatusMutex.lock();
    std::chrono::milliseconds startTime = duration_cast<milliseconds>(system_clock::now().time_since_epoch());

    struct ProcessInfo processInformation;
    processInformation.id = uniqueId;
    processInformation.graphId = graphId;
    processInformation.processName = SHEEP_TRIANGLES;  // Use SHEEP_TRIANGLES identifier
    processInformation.priority = threadPriority;
    processInformation.startTimestamp = startTime.count();

    if (!queueTime.empty()) {
        long sleepTime = atol(queueTime.c_str());
        processInformation.sleepTime = sleepTime;
        processData.insert(processInformation);
        processStatusMutex.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime));
    } else {
        processData.insert(processInformation);
        processStatusMutex.unlock();
    }

    sheepTriangleCount_logger.log(
        "###SHEEP-TRIANGLE-COUNT-EXECUTOR### Started with graph ID : " + graphId + " Master IP : " + masterIP, "info");

    long result = 0;
    bool isCompositeAggregation = false;
    Utils::worker aggregatorWorker;
    std::vector<std::future<long>> intermRes;
    std::vector<std::future<int>> statResponse;
    std::vector<std::string> compositeCentralStoreFiles;

    auto begin = chrono::high_resolution_clock::now();

    string sqlStatement =
        "SELECT DISTINCT worker_idworker,partition_idpartition "
        "FROM worker_has_partition INNER JOIN worker ON worker_has_partition.worker_idworker=worker.idworker "
        "WHERE partition_graph_idgraph=" +
        graphId + ";";

    const std::vector<vector<pair<string, string>>> &results = sqlite->runSelect(sqlStatement);

    std::map<string, std::vector<string>> partitionMap;

    for (auto i = results.begin(); i != results.end(); ++i) {
        const std::vector<pair<string, string>> &rowData = *i;

        string workerID = rowData.at(0).second;
        string partitionId = rowData.at(1).second;

        if (partitionMap.find(workerID) == partitionMap.end()) {
            std::vector<string> partitionVec;
            partitionVec.push_back(partitionId);
            partitionMap[workerID] = partitionVec;
        } else {
            partitionMap[workerID].push_back(partitionId);
        }

        sheepTriangleCount_logger.info("###SHEEP-TRIANGLE-COUNT-EXECUTOR### Getting Triangle Count : PartitionId " + partitionId);
    }

    if (results.size() > Conts::COMPOSITE_CENTRAL_STORE_WORKER_THRESHOLD) {
        isCompositeAggregation = true;
    }

    if (jasminegraph_profile == PROFILE_K8S) {
        std::unique_ptr<K8sInterface> k8sInterface(new K8sInterface());
        if (k8sInterface->getJasmineGraphConfig("auto_scaling_enabled") == "true") {
            filter_partitions(partitionMap, sqlite, graphId);
        }
    }

    std::vector<std::vector<string>> fileCombinations;
    if (isCompositeAggregation) {
        std::string aggregatorFilePath =
            Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.aggregatefolder");
        std::vector<std::string> graphFiles = Utils::getListOfFilesInDirectory(aggregatorFilePath);

        std::string compositeFileNameFormat = graphId + "_compositecentralstore_";

        for (auto graphFilesIterator = graphFiles.begin(); graphFilesIterator != graphFiles.end();
             ++graphFilesIterator) {
            std::string graphFileName = *graphFilesIterator;

            if ((graphFileName.find(compositeFileNameFormat) == 0) &&
                (graphFileName.find(".gz") != std::string::npos)) {
                compositeCentralStoreFiles.push_back(graphFileName);
            }
        }
        fileCombinations = AbstractExecutor::getCombinations(compositeCentralStoreFiles);
    }

    for (auto it = partitionMap.begin(); it != partitionMap.end(); it++) {
        string worker = it->first;
        if (used_workers.find(worker) != used_workers.end()) {
            used_workers[worker]++;
        } else {
            used_workers[worker] = 1;
        }
    }

    std::map<std::string, std::string> combinationWorkerMap;
    std::unordered_map<long, std::unordered_map<long, std::unordered_set<long>>> triangleTree;
    std::mutex triangleTreeMutex;
    int partitionCount = 0;

    // Track worker information for better tracing
    std::vector<std::tuple<string, string, string>> workerTaskInfo;  // {workerID, partitionId, host}

    // Capture the master trace context for all workers
    std::string masterTraceContext = OpenTelemetryUtil::getCurrentTraceContext();

    vector<Utils::worker> workerList = Utils::getWorkerList(sqlite);
    int workerListSize = workerList.size();
    for (int i = 0; i < workerListSize; i++) {
        Utils::worker currentWorker = workerList.at(i);
        string host = currentWorker.hostname;
        string workerID = currentWorker.workerID;
        string partitionId;
        int workerPort = atoi(string(currentWorker.port).c_str());
        int workerDataPort = atoi(string(currentWorker.dataPort).c_str());
        sheepTriangleCount_logger.info("worker_" + workerID + " host=" + host + ":" + to_string(workerPort) + ":" +
                                      to_string(workerDataPort));
        const std::vector<string> &partitionList = partitionMap[workerID];
        for (auto partitionIterator = partitionList.begin(); partitionIterator != partitionList.end();
             ++partitionIterator) {
            partitionCount++;
            partitionId = *partitionIterator;
            sheepTriangleCount_logger.info("> partition" + partitionId);

            // Store worker task information for tracing
            workerTaskInfo.push_back(std::make_tuple(workerID, partitionId, host));

            {
                OTEL_TRACE_OPERATION("distribute_to_worker_" + workerID + "_partition_" + partitionId);

                // Use the static method from TriangleCountExecutor to perform the actual work
                intermRes.push_back(std::async(
                    std::launch::async, TriangleCountExecutor::getTriangleCount, atoi(graphId.c_str()), host,
                    workerPort, workerDataPort, atoi(partitionId.c_str()), masterIP, uniqueId,
                    isCompositeAggregation, threadPriority, fileCombinations, &combinationWorkerMap,
                    &triangleTree, &triangleTreeMutex, masterTraceContext));
            }
        }
    }

    PerformanceUtil::init();

    std::string query =
        "SELECT attempt from graph_sla INNER JOIN sla_category where graph_sla.id_sla_category=sla_category.id and "
        "graph_sla.graph_id='" +
        graphId + "' and graph_sla.partition_count='" + std::to_string(partitionCount) +
        "' and sla_category.category='" + Conts::SLA_CATEGORY::LATENCY + "' and sla_category.command='" + SHEEP_TRIANGLES +
        "';";

    const std::vector<vector<pair<string, string>>> &queryResults = perfDB->runSelect(query);

    if (queryResults.size() > 0) {
        std::string attemptString = queryResults[0][0].second;
        int calibratedAttempts = atoi(attemptString.c_str());

        if (calibratedAttempts >= Conts::MAX_SLA_CALIBRATE_ATTEMPTS) {
            canCalibrate = false;
        }
    } else {
        sheepTriangleCount_logger.log("###SHEEP-TRIANGLE-COUNT-EXECUTOR### Inserting initial record for SLA ", "info");
        Utils::updateSLAInformation(perfDB, graphId, partitionCount, 0, SHEEP_TRIANGLES, Conts::SLA_CATEGORY::LATENCY);
        statResponse.push_back(std::async(std::launch::async, AbstractExecutor::collectPerformaceData, perfDB,
                                          graphId.c_str(), SHEEP_TRIANGLES, Conts::SLA_CATEGORY::LATENCY, partitionCount,
                                          masterIP, autoCalibrate));
        isStatCollect = true;
    }

    last_exec_time = time(NULL);
    schedulerMutex.unlock();

    // Collect worker results with automatic tracing
    {
        OTEL_TRACE_OPERATION("collect_worker_results");

        int taskIndex = 0;
        for (auto &&futureCall : intermRes) {
            // Get worker information for this task
            const auto& taskInfo = workerTaskInfo[taskIndex];
            string workerID = std::get<0>(taskInfo);
            string partitionId = std::get<1>(taskInfo);
            string host = std::get<2>(taskInfo);

            {
                OTEL_TRACE_OPERATION("wait_for_worker_" + workerID + "_partition_" + partitionId + "_on_" + host);
                sheepTriangleCount_logger.info("Waiting for result from worker_" + workerID +
                                              " partition_" + partitionId +
                                              " host_" + host +
                                              " uuid=" + to_string(uniqueId));
                long worker_result = futureCall.get();
                sheepTriangleCount_logger.info("Received result " + std::to_string(worker_result) +
                                              " from worker_" + workerID +
                                              " partition_" + partitionId);

                {
                    OTEL_TRACE_OPERATION("aggregate_result_worker_" + workerID + "_partition_" + partitionId);
                    result += worker_result;
                }
            }
            taskIndex++;
        }

        // Cleanup data structures
        {
            OTEL_TRACE_OPERATION("cleanup_worker_data_structures");
            triangleTree.clear();
            combinationWorkerMap.clear();
        }
    }

    if (!isCompositeAggregation) {
        // Restore the master trace context before aggregation
        OpenTelemetryUtil::receiveAndSetTraceContext(masterTraceContext, "central store aggregation");

        OTEL_TRACE_OPERATION("central_store_aggregation");

        // Use the static aggregation method from TriangleCountExecutor
        long aggregatedTriangleCount =
            TriangleCountExecutor::aggregateCentralStoreTriangles(sqlite, graphId, masterIP, threadPriority, partitionMap);
        result += aggregatedTriangleCount;

        workerResponded = true;
        sheepTriangleCount_logger.log(
            "###SHEEP-TRIANGLE-COUNT-EXECUTOR### Getting Triangle Count : Completed: Triangles " + to_string(result), "info");
    }

    schedulerMutex.lock();
    for (auto it = partitionMap.begin(); it != partitionMap.end(); it++) {
        string worker = it->first;
        used_workers[worker]--;
    }
    for (auto it = used_workers.cbegin(); it != used_workers.cend();) {
        if (it->second <= 0) {
            used_workers.erase(it++);
        } else {
            it++;
        }
    }
    schedulerMutex.unlock();

    workerResponded = true;

    JobResponse jobResponse;
    jobResponse.setJobId(request.getJobId());
    jobResponse.addParameter(Conts::PARAM_KEYS::TRIANGLE_COUNT, std::to_string(result));
    responseVector.push_back(jobResponse);

    responseVectorMutex.lock();
    responseMap[request.getJobId()] = jobResponse;
    responseVectorMutex.unlock();

    auto end = chrono::high_resolution_clock::now();
    auto dur = end - begin;
    auto msDuration = std::chrono::duration_cast<std::chrono::milliseconds>(dur).count();

    std::string durationString = std::to_string(msDuration);

    if (canCalibrate || autoCalibrate) {
        Utils::updateSLAInformation(perfDB, graphId, partitionCount, msDuration, SHEEP_TRIANGLES,
                                    Conts::SLA_CATEGORY::LATENCY);
        isStatCollect = false;
    }

    processStatusMutex.lock();
    for (auto processCompleteIterator = processData.begin(); processCompleteIterator != processData.end();
         ++processCompleteIterator) {
        ProcessInfo processInformation = *processCompleteIterator;

        if (processInformation.id == uniqueId) {
            processData.erase(processInformation);
            break;
        }
    }
    processStatusMutex.unlock();
}

// Delegate static methods to TriangleCountExecutor
long SheepTriangleCountExecutor::getSheepTriangleCount(
    int graphId, std::string host, int port, int dataPort, int partitionId, std::string masterIP, int uniqueId,
    bool isCompositeAggregation, int threadPriority, std::vector<std::vector<string>> fileCombinations,
    std::map<std::string, std::string> *combinationWorkerMap_p,
    std::unordered_map<long, std::unordered_map<long, std::unordered_set<long>>> *triangleTree_p,
    std::mutex *triangleTreeMutex_p, const std::string& masterTraceContext) {
    
    return TriangleCountExecutor::getTriangleCount(graphId, host, port, dataPort, partitionId, masterIP, uniqueId,
                                                   isCompositeAggregation, threadPriority, fileCombinations,
                                                   combinationWorkerMap_p, triangleTree_p, triangleTreeMutex_p,
                                                   masterTraceContext);
}

std::string SheepTriangleCountExecutor::copyCompositeCentralStoreToAggregator(std::string aggregatorHostName,
                                                                              std::string aggregatorPort,
                                                                              std::string aggregatorDataPort,
                                                                              std::string fileName,
                                                                              std::string masterIP) {
    return TriangleCountExecutor::copyCompositeCentralStoreToAggregator(aggregatorHostName, aggregatorPort,
                                                                        aggregatorDataPort, fileName, masterIP);
}

std::vector<string> SheepTriangleCountExecutor::countCompositeCentralStoreTriangles(std::string aggregatorHostName,
                                                                                    std::string aggregatorPort,
                                                                                    std::string compositeCentralStoreFileList,
                                                                                    std::string masterIP,
                                                                                    std::string availableFileList,
                                                                                    int threadPriority) {
    return TriangleCountExecutor::countCompositeCentralStoreTriangles(aggregatorHostName, aggregatorPort,
                                                                      compositeCentralStoreFileList, masterIP,
                                                                      availableFileList, threadPriority);
}

std::string SheepTriangleCountExecutor::copyCentralStoreToAggregator(std::string aggregatorHostName,
                                                                     std::string aggregatorPort,
                                                                     std::string aggregatorDataPort,
                                                                     int graphId,
                                                                     int partitionId,
                                                                     std::string masterIP) {
    return TriangleCountExecutor::copyCentralStoreToAggregator(aggregatorHostName, aggregatorPort,
                                                               aggregatorDataPort, graphId, partitionId, masterIP);
}
