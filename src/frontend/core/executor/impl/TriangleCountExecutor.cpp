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

#include "TriangleCountExecutor.h"

using namespace std::chrono;

Logger triangleCount_logger;
std::vector<std::vector<string>> TriangleCountExecutor::fileCombinations;
std::map<std::string, std::string> TriangleCountExecutor::combinationWorkerMap;
std::map<long, std::map<long, std::vector<long>>> TriangleCountExecutor::triangleTree;
bool isStatCollect = false;

std::mutex fileCombinationMutex;
std::mutex processStatusMutex;
std::mutex responseVectorMutex;

TriangleCountExecutor::TriangleCountExecutor() {}

TriangleCountExecutor::TriangleCountExecutor(SQLiteDBInterface *db, PerformanceSQLiteDBInterface *perfDb,
                                             JobRequest jobRequest) {
    this->sqlite = db;
    this->perfDB = perfDb;
    this->request = jobRequest;
}

void TriangleCountExecutor::execute() {
    int uniqueId = getUid();
    std::string masterIP = request.getMasterIP();
    std::string graphId = request.getParameter(Conts::PARAM_KEYS::GRAPH_ID);
    std::string canCalibrateString = request.getParameter(Conts::PARAM_KEYS::CAN_CALIBRATE);
    std::string queueTime = request.getParameter(Conts::PARAM_KEYS::QUEUE_TIME);
    std::string graphSLAString = request.getParameter(Conts::PARAM_KEYS::GRAPH_SLA);

    bool canCalibrate = Utils::parseBoolean(canCalibrateString);
    int threadPriority = request.getPriority();

    std::string autoCalibrateString = request.getParameter(Conts::PARAM_KEYS::AUTO_CALIBRATION);
    bool autoCalibrate = Utils::parseBoolean(autoCalibrateString);

    if (threadPriority == Conts::HIGH_PRIORITY_DEFAULT_VALUE) {
        highPriorityGraphList.push_back(graphId);
    }

    // Below code is used to update the process details
    processStatusMutex.lock();
    std::set<ProcessInfo>::iterator processIterator;
    bool processInfoExists = false;
    std::chrono::milliseconds startTime = duration_cast<milliseconds>(system_clock::now().time_since_epoch());

    struct ProcessInfo processInformation;
    processInformation.id = uniqueId;
    processInformation.graphId = graphId;
    processInformation.processName = TRIANGLES;
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

    triangleCount_logger.log(
        "###TRIANGLE-COUNT-EXECUTOR### Started with graph ID : " + graphId + " Master IP : " + masterIP, "info");

    long result = 0;
    bool isCompositeAggregation = false;
    Utils::worker aggregatorWorker;
    vector<Utils::worker> workerList = Utils::getWorkerList(sqlite);
    int workerListSize = workerList.size();
    int partitionCount = 0;
    std::vector<std::future<long>> intermRes;
    std::vector<std::future<int>> statResponse;
    std::vector<std::future<string>> remoteCopyRes;
    PlacesToNodeMapper placesToNodeMapper;
    std::vector<std::string> compositeCentralStoreFiles;
    int slaStatCount = 0;

    auto begin = chrono::high_resolution_clock::now();

    string sqlStatement =
        "SELECT worker_idworker, name,ip,user,server_port,server_data_port,partition_idpartition "
        "FROM worker_has_partition INNER JOIN worker ON worker_has_partition.worker_idworker=worker.idworker "
        "WHERE partition_graph_idgraph=" +
        graphId + ";";

    std::vector<vector<pair<string, string>>> results = sqlite->runSelect(sqlStatement);

    if (results.size() > Conts::COMPOSITE_CENTRAL_STORE_WORKER_THRESHOLD) {
        isCompositeAggregation = true;
    }

    if (isCompositeAggregation) {
        std::string aggregatorFilePath =
            Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.aggregatefolder");
        std::vector<std::string> graphFiles = Utils::getListOfFilesInDirectory(aggregatorFilePath);

        std::vector<std::string>::iterator graphFilesIterator;
        std::string compositeFileNameFormat = graphId + "_compositecentralstore_";

        for (graphFilesIterator = graphFiles.begin(); graphFilesIterator != graphFiles.end(); ++graphFilesIterator) {
            std::string graphFileName = *graphFilesIterator;

            if ((graphFileName.find(compositeFileNameFormat) == 0) &&
                (graphFileName.find(".gz") != std::string::npos)) {
                compositeCentralStoreFiles.push_back(graphFileName);
            }
        }
        fileCombinations = getCombinations(compositeCentralStoreFiles);
    }

    std::map<string, std::vector<string>> partitionMap;

    for (std::vector<vector<pair<string, string>>>::iterator i = results.begin(); i != results.end(); ++i) {
        std::vector<pair<string, string>> rowData = *i;

        string workerID = rowData.at(0).second;
        string name = rowData.at(1).second;
        string host = rowData.at(2).second;
        string user = rowData.at(3).second;
        string serverPort = rowData.at(4).second;
        string serverDataPort = rowData.at(5).second;
        string partitionId = rowData.at(6).second;

        if (partitionMap.find(workerID) == partitionMap.end()) {
            std::vector<string> partitionVec;
            partitionVec.push_back(partitionId);
            partitionMap[workerID] = partitionVec;
        } else {
            std::vector<string> partitionVec = partitionMap.find(workerID)->second;
            partitionVec.push_back(partitionId);
        }

        triangleCount_logger.log("###TRIANGLE-COUNT-EXECUTOR### Getting Triangle Count : Host " + host +
                                     " Server Port " + serverPort + " PartitionId " + partitionId,
                                 "info");
    }

    for (auto &&futureCall : remoteCopyRes) {
        futureCall.wait();
    }

    for (int i = 0; i < workerListSize; i++) {
        Utils::worker currentWorker = workerList.at(i);
        string host = currentWorker.hostname;
        string workerID = currentWorker.workerID;
        string partitionId;

        std::vector<string> partitionList = partitionMap[workerID];

        std::vector<string>::iterator partitionIterator;

        for (partitionIterator = partitionList.begin(); partitionIterator != partitionList.end(); ++partitionIterator) {
            partitionCount++;
            int workerPort = atoi(string(currentWorker.port).c_str());
            int workerDataPort = atoi(string(currentWorker.dataPort).c_str());

            partitionId = *partitionIterator;
            intermRes.push_back(std::async(
                std::launch::async, TriangleCountExecutor::getTriangleCount, atoi(graphId.c_str()), host, workerPort,
                workerDataPort, atoi(partitionId.c_str()), masterIP, uniqueId, isCompositeAggregation, threadPriority));
        }
    }

    PerformanceUtil::init();

    std::string query =
        "SELECT attempt from graph_sla INNER JOIN sla_category where graph_sla.id_sla_category=sla_category.id and "
        "graph_sla.graph_id='" +
        graphId + "' and graph_sla.partition_count='" + std::to_string(partitionCount) +
        "' and sla_category.category='" + Conts::SLA_CATEGORY::LATENCY +
    "' and sla_category.command='" + TRIANGLES + "';";

    std::vector<vector<pair<string, string>>> queryResults = perfDB->runSelect(query);

    if (queryResults.size() > 0) {
        std::string attemptString = queryResults[0][0].second;
        int calibratedAttempts = atoi(attemptString.c_str());

        if (calibratedAttempts >= Conts::MAX_SLA_CALIBRATE_ATTEMPTS) {
            canCalibrate = false;
        }
    } else {
        triangleCount_logger.log("###TRIANGLE-COUNT-EXECUTOR### Inserting initial record for SLA ", "info");
        Utils::updateSLAInformation(perfDB, graphId, partitionCount, 0, TRIANGLES, Conts::SLA_CATEGORY::LATENCY);
        statResponse.push_back(std::async(std::launch::async, collectPerformaceData, perfDB,
                                          graphId.c_str(), TRIANGLES, Conts::SLA_CATEGORY::LATENCY, partitionCount,
                                          masterIP, autoCalibrate));
        isStatCollect = true;
    }

    for (auto &&futureCall : intermRes) {
        result += futureCall.get();
    }

    if (!isCompositeAggregation) {
        long aggregatedTriangleCount =
            TriangleCountExecutor::aggregateCentralStoreTriangles(sqlite, graphId, masterIP, threadPriority);
        result += aggregatedTriangleCount;
        workerResponded = true;
        triangleCount_logger.log(
            "###TRIANGLE-COUNT-EXECUTOR### Getting Triangle Count : Completed: Triangles " + to_string(result), "info");
    }

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
        Utils::updateSLAInformation(perfDB, graphId, partitionCount, msDuration, TRIANGLES,
                                    Conts::SLA_CATEGORY::LATENCY);
        isStatCollect = false;
    }

    processStatusMutex.lock();
    std::set<ProcessInfo>::iterator processCompleteIterator;
    for (processCompleteIterator = processData.begin(); processCompleteIterator != processData.end();
         ++processCompleteIterator) {
        ProcessInfo processInformation = *processCompleteIterator;

        if (processInformation.id == uniqueId) {
            processData.erase(processInformation);
            break;
        }
    }
    processStatusMutex.unlock();

    triangleTree.clear();
    combinationWorkerMap.clear();
}

long TriangleCountExecutor::getTriangleCount(int graphId, std::string host, int port, int dataPort, int partitionId,
                                             std::string masterIP, int uniqueId, bool isCompositeAggregation,
                                             int threadPriority) {
    int sockfd;
    char data[301];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;
    long triangleCount;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot create socket" << std::endl;
        return 0;
    }

    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }

    triangleCount_logger.log("###TRIANGLE-COUNT-EXECUTOR### Get Host By Name : " + host, "info");

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        triangleCount_logger.error("ERROR, no host named " + host);
        return 0;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
        return 0;
    }

    bzero(data, 301);
    int result_wr =
        write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if (result_wr < 0) {
        triangleCount_logger.log("Error writing to socket", "error");
    }

    triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = Utils::trim_copy(response);

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if (result_wr < 0) {
            triangleCount_logger.log("Error writing to socket", "error");
        }

        triangleCount_logger.log("Sent : " + masterIP, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        } else {
            triangleCount_logger.log("Received : " + response, "error");
        }
        result_wr = write(sockfd, JasmineGraphInstanceProtocol::TRIANGLES.c_str(),
                          JasmineGraphInstanceProtocol::TRIANGLES.size());

        if (result_wr < 0) {
            triangleCount_logger.log("Error writing to socket", "error");
        }

        triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::TRIANGLES, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = Utils::trim_copy(response);

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, std::to_string(graphId).c_str(), std::to_string(graphId).size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }

            triangleCount_logger.log("Sent : Graph ID " + std::to_string(graphId), "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = Utils::trim_copy(response);
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, std::to_string(partitionId).c_str(), std::to_string(partitionId).size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }

            triangleCount_logger.log("Sent : Partition ID " + std::to_string(partitionId), "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = Utils::trim_copy(response);
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, std::to_string(threadPriority).c_str(), std::to_string(threadPriority).size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }

            triangleCount_logger.log("Sent : Thread Priority " + std::to_string(threadPriority), "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            triangleCount_logger.log("Got response : |" + response + "|", "info");
            response = Utils::trim_copy(response);
            triangleCount = atol(response.c_str());
        }

        if (isCompositeAggregation) {
            triangleCount_logger.log("###COMPOSITE### Started Composite aggregation ", "info");
            static std::vector<std::vector<string>>::iterator combinationsIterator;

            for (int combinationIndex = 0; combinationIndex < fileCombinations.size(); ++combinationIndex) {
                std::vector<string> fileList = fileCombinations.at(combinationIndex);
                std::vector<string>::iterator fileListIterator;
                std::vector<string>::iterator listIterator;
                std::set<string> partitionIdSet;
                std::set<string> partitionSet;
                std::map<int, int> tempWeightMap;
                std::set<string>::iterator partitionSetIterator;
                std::set<string> transferRequireFiles;
                std::string combinationKey = "";
                std::string availableFiles = "";
                std::string transferredFiles = "";
                bool isAggregateValid = false;

                for (listIterator = fileList.begin(); listIterator != fileList.end(); ++listIterator) {
                    std::string fileName = *listIterator;

                    size_t lastIndex = fileName.find_last_of(".");
                    string rawFileName = fileName.substr(0, lastIndex);

                    std::vector<std::string> fileNameParts = Utils::split(rawFileName, '_');

                    /*Partition numbers are extracted from  the file name. The starting index of partition number is 2.
                     * Therefore the loop starts with 2*/
                    for (int index = 2; index < fileNameParts.size(); ++index) {
                        partitionSet.insert(fileNameParts[index]);
                    }
                }

                if (partitionSet.find(std::to_string(partitionId)) == partitionSet.end()) {
                    continue;
                }

                if (proceedOrNot(partitionSet, partitionId)) {
                } else {
                    continue;
                }

                for (fileListIterator = fileList.begin(); fileListIterator != fileList.end(); ++fileListIterator) {
                    std::string fileName = *fileListIterator;
                    bool isTransferRequired = true;

                    combinationKey = fileName + ":" + combinationKey;

                    size_t lastindex = fileName.find_last_of(".");
                    string rawFileName = fileName.substr(0, lastindex);

                    std::vector<std::string> fileNameParts = Utils::split(rawFileName, '_');

                    for (int index = 2; index < fileNameParts.size(); ++index) {
                        if (fileNameParts[index] == std::to_string(partitionId)) {
                            isTransferRequired = false;
                        }
                        partitionIdSet.insert(fileNameParts[index]);
                    }

                    if (isTransferRequired) {
                        transferRequireFiles.insert(fileName);
                        transferredFiles = fileName + ":" + transferredFiles;
                    } else {
                        availableFiles = fileName + ":" + availableFiles;
                    }
                }

                std::string adjustedCombinationKey = combinationKey.substr(0, combinationKey.size() - 1);
                std::string adjustedAvailableFiles = availableFiles.substr(0, availableFiles.size() - 1);
                std::string adjustedTransferredFile = transferredFiles.substr(0, transferredFiles.size() - 1);

                fileCombinationMutex.lock();
                if (combinationWorkerMap.find(combinationKey) == combinationWorkerMap.end()) {
                    if (partitionIdSet.find(std::to_string(partitionId)) != partitionIdSet.end()) {
                        combinationWorkerMap[combinationKey] = std::to_string(partitionId);
                        isAggregateValid = true;
                    }
                }
                fileCombinationMutex.unlock();

                if (isAggregateValid) {
                    std::set<string>::iterator transferRequireFileIterator;

                    for (transferRequireFileIterator = transferRequireFiles.begin();
                         transferRequireFileIterator != transferRequireFiles.end(); ++transferRequireFileIterator) {
                        std::string transferFileName = *transferRequireFileIterator;
                        std::string fileAccessible = isFileAccessibleToWorker(
                            std::to_string(graphId), std::string(), host, std::to_string(port), masterIP,
                            JasmineGraphInstanceProtocol::FILE_TYPE_CENTRALSTORE_COMPOSITE, transferFileName);

                        if (fileAccessible.compare("false") == 0) {
                            copyCompositeCentralStoreToAggregator(host, std::to_string(port), std::to_string(dataPort),
                                                                  transferFileName, masterIP);
                        }
                    }

                    std::string compositeTriangles =
                        countCompositeCentralStoreTriangles(host, std::to_string(port), adjustedTransferredFile,
                                                            masterIP, adjustedAvailableFiles, threadPriority);

                    triangleCount_logger.log("###COMPOSITE### Retrieved Composite triangle list ", "debug");

                    std::vector<std::string> triangles = Utils::split(compositeTriangles, ':');

                    if (triangles.size() > 0) {
                        triangleCount += updateTriangleTreeAndGetTriangleCount(triangles);
                    }
                }
                updateMap(partitionId);
            }
        }

        triangleCount_logger.info("###COMPOSITE### Returning Total Triangles from executer ");

        return triangleCount;

    } else {
        triangleCount_logger.log("There was an error in the upload process and the response is :: " + response,
                                 "error");
    }
    return 0;
}

bool TriangleCountExecutor::proceedOrNot(std::set<string> partitionSet, int partitionId) {
    const std::lock_guard<std::mutex> lock(aggregateWeightMutex);

    std::set<string>::iterator partitionSetIterator;
    std::map<int, int> tempWeightMap;
    for (partitionSetIterator = partitionSet.begin(); partitionSetIterator != partitionSet.end();
         ++partitionSetIterator) {
        std::string partitionIdString = *partitionSetIterator;
        int currentPartitionId = atoi(partitionIdString.c_str());
        tempWeightMap[currentPartitionId] = aggregateWeightMap[currentPartitionId];
    }

    int currentWorkerWeight = tempWeightMap[partitionId];
    pair<int, int> entryWithMinValue = make_pair(partitionId, currentWorkerWeight);

    map<int, int>::iterator currentEntry;

    for (currentEntry = aggregateWeightMap.begin(); currentEntry != aggregateWeightMap.end(); ++currentEntry) {
        if (entryWithMinValue.second > currentEntry->second) {
            entryWithMinValue = make_pair(currentEntry->first, currentEntry->second);
        }
    }

    bool result = false;
    if (entryWithMinValue.first == partitionId) {
        int currentWeight = aggregateWeightMap[entryWithMinValue.first];
        currentWeight++;
        aggregateWeightMap[entryWithMinValue.first] = currentWeight;
        triangleCount_logger.log("###COMPOSITE### Aggregator Initiated : Partition ID: " + std::to_string(partitionId) +
                                     " Weight : " + std::to_string(currentWeight),
                                 "info");
        result = true;
    }

    aggregateWeightMutex.unlock();
    return result;
}

void TriangleCountExecutor::updateMap(int partitionId) {
    const std::lock_guard<std::mutex> lock(aggregateWeightMutex);

    int currentWeight = aggregateWeightMap[partitionId];
    currentWeight--;
    aggregateWeightMap[partitionId] = currentWeight;
    triangleCount_logger.log("###COMPOSITE### Aggregator Completed : Partition ID: " + std::to_string(partitionId) +
                                 " Weight : " + std::to_string(currentWeight),
                             "info");
}

int TriangleCountExecutor::updateTriangleTreeAndGetTriangleCount(std::vector<std::string> triangles) {
    const std::lock_guard<std::mutex> lock1(triangleTreeMutex);
    std::vector<std::string>::iterator triangleIterator;
    int aggregateCount = 0;

    triangleCount_logger.log("###COMPOSITE### Triangle Tree locked ", "debug");

    for (triangleIterator = triangles.begin(); triangleIterator != triangles.end(); ++triangleIterator) {
        std::string triangle = *triangleIterator;

        if (!triangle.empty() && triangle != "NILL") {
            std::vector<std::string> triangleVertexList = Utils::split(triangle, ',');

            long vertexOne = std::atol(triangleVertexList.at(0).c_str());
            long vertexTwo = std::atol(triangleVertexList.at(1).c_str());
            long vertexThree = std::atol(triangleVertexList.at(2).c_str());

            std::map<long, std::vector<long>> itemRes = triangleTree[vertexOne];

            std::map<long, std::vector<long>>::iterator itemResIterator = itemRes.find(vertexTwo);

            if (itemResIterator != itemRes.end()) {
                std::vector<long> list = itemRes[vertexTwo];

                if (std::find(list.begin(), list.end(), vertexThree) == list.end()) {
                    triangleTree[vertexOne][vertexTwo].push_back(vertexThree);
                    aggregateCount++;
                }
            } else {
                triangleTree[vertexOne][vertexTwo].push_back(vertexThree);
                aggregateCount++;
            }
        }
    }

    return aggregateCount;
}

long TriangleCountExecutor::aggregateCentralStoreTriangles(SQLiteDBInterface *sqlite, std::string graphId,
                                                           std::string masterIP, int threadPriority) {
    std::vector<std::vector<string>> workerCombinations = getWorkerCombination(sqlite, graphId);
    std::map<string, int> workerWeightMap;
    std::vector<std::vector<string>>::iterator workerCombinationsIterator;
    std::vector<std::future<string>> triangleCountResponse;
    std::string result = "";
    long aggregatedTriangleCount = 0;

    for (workerCombinationsIterator = workerCombinations.begin();
         workerCombinationsIterator != workerCombinations.end(); ++workerCombinationsIterator) {
        std::vector<string> workerCombination = *workerCombinationsIterator;
        std::map<string, int>::iterator workerWeightMapIterator;
        std::vector<std::future<string>> remoteGraphCopyResponse;
        int minimumWeight = 0;
        std::string minWeightWorker;
        std::string partitionIdList = "";

        std::vector<string>::iterator workerCombinationIterator;
        std::vector<string>::iterator aggregatorCopyCombinationIterator;

        for (workerCombinationIterator = workerCombination.begin();
             workerCombinationIterator != workerCombination.end(); ++workerCombinationIterator) {
            std::string workerId = *workerCombinationIterator;

            workerWeightMapIterator = workerWeightMap.find(workerId);

            if (workerWeightMapIterator != workerWeightMap.end()) {
                int weight = workerWeightMap.at(workerId);

                if (minimumWeight == 0 || minimumWeight > weight) {
                    minimumWeight = weight + 1;
                    minWeightWorker = workerId;
                }
            } else {
                minimumWeight = 1;
                minWeightWorker = workerId;
            }
        }

        string aggregatorSqlStatement =
            "SELECT ip,server_port,server_data_port,partition_idpartition "
            "FROM worker_has_partition INNER JOIN worker ON worker_has_partition.worker_idworker=worker.idworker "
            "WHERE partition_graph_idgraph=" +
            graphId + " and idworker=" + minWeightWorker + ";";

        std::vector<vector<pair<string, string>>> result = sqlite->runSelect(aggregatorSqlStatement);

        vector<pair<string, string>> aggregatorData = result.at(0);

        std::string aggregatorIp = aggregatorData.at(0).second;
        std::string aggregatorPort = aggregatorData.at(1).second;
        std::string aggregatorDataPort = aggregatorData.at(2).second;
        std::string aggregatorPartitionId = aggregatorData.at(3).second;

        for (aggregatorCopyCombinationIterator = workerCombination.begin();
             aggregatorCopyCombinationIterator != workerCombination.end(); ++aggregatorCopyCombinationIterator) {
            std::string workerId = *aggregatorCopyCombinationIterator;

            if (workerId != minWeightWorker) {
                string sqlStatement =
                    "SELECT partition_idpartition "
                    "FROM worker_has_partition INNER JOIN worker ON "
                    "worker_has_partition.worker_idworker=worker.idworker "
                    "WHERE partition_graph_idgraph=" +
                    graphId + " and idworker=" + workerId + ";";

                std::vector<vector<pair<string, string>>> result = sqlite->runSelect(sqlStatement);

                vector<pair<string, string>> workerData = result.at(0);

                std::string partitionId = workerData.at(0).second;

                partitionIdList += partitionId + ",";

                std::string centralStoreAvailable = isFileAccessibleToWorker(
                    graphId, partitionId, aggregatorIp, aggregatorPort, masterIP,
                    JasmineGraphInstanceProtocol::FILE_TYPE_CENTRALSTORE_AGGREGATE, std::string());

                if (centralStoreAvailable.compare("false") == 0) {
                    remoteGraphCopyResponse.push_back(
                        std::async(std::launch::async, TriangleCountExecutor::copyCentralStoreToAggregator,
                                   aggregatorIp, aggregatorPort, aggregatorDataPort, atoi(graphId.c_str()),
                                   atoi(partitionId.c_str()), masterIP));
                }
            }
        }

        for (auto &&futureCallCopy : remoteGraphCopyResponse) {
            futureCallCopy.get();
        }

        std::string adjustedPartitionIdList = partitionIdList.substr(0, partitionIdList.size() - 1);
        workerWeightMap[minWeightWorker] = minimumWeight;

        triangleCountResponse.push_back(std::async(
            std::launch::async, TriangleCountExecutor::countCentralStoreTriangles, aggregatorPort, aggregatorIp,
            aggregatorPartitionId, adjustedPartitionIdList, graphId, masterIP, threadPriority));
    }

    for (auto &&futureCall : triangleCountResponse) {
        result = result + ":" + futureCall.get();
    }

    std::vector<std::string> triangles = Utils::split(result, ':');
    std::vector<std::string>::iterator triangleIterator;
    std::set<std::string> uniqueTriangleSet;

    for (triangleIterator = triangles.begin(); triangleIterator != triangles.end(); ++triangleIterator) {
        std::string triangle = *triangleIterator;

        if (!triangle.empty() && triangle != "NILL") {
            uniqueTriangleSet.insert(triangle);
        }
    }

    aggregatedTriangleCount = uniqueTriangleSet.size();

    return aggregatedTriangleCount;
}

string TriangleCountExecutor::isFileAccessibleToWorker(std::string graphId, std::string partitionId,
                                                       std::string aggregatorHostName, std::string aggregatorPort,
                                                       std::string masterIP, std::string fileType,
                                                       std::string fileName) {
    int sockfd;
    char data[301];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;
    string isFileAccessible = "false";

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot create socket" << std::endl;
        return 0;
    }

    server = gethostbyname(aggregatorHostName.c_str());
    if (server == NULL) {
        triangleCount_logger.error("ERROR, no host named " + aggregatorHostName);
        return 0;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(atoi(aggregatorPort.c_str()));
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
        // TODO::exit
        return 0;
    }

    bzero(data, 301);
    int result_wr =
        write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if (result_wr < 0) {
        triangleCount_logger.log("Error writing to socket", "error");
    }

    triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = Utils::trim_copy(response);

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if (result_wr < 0) {
            triangleCount_logger.log("Error writing to socket", "error");
        }

        triangleCount_logger.log("Sent : " + masterIP, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        } else {
            triangleCount_logger.log("Received : " + response, "error");
        }
        result_wr = write(sockfd, JasmineGraphInstanceProtocol::CHECK_FILE_ACCESSIBLE.c_str(),
                          JasmineGraphInstanceProtocol::CHECK_FILE_ACCESSIBLE.size());

        if (result_wr < 0) {
            triangleCount_logger.log("Error writing to socket", "error");
        }

        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = Utils::trim_copy(response);

        if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_TYPE) == 0) {
            result_wr = write(sockfd, fileType.c_str(), fileType.size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = Utils::trim_copy(response);

            if (fileType.compare(JasmineGraphInstanceProtocol::FILE_TYPE_CENTRALSTORE_AGGREGATE) == 0) {
                if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
                    result_wr = write(sockfd, graphId.c_str(), graphId.size());

                    if (result_wr < 0) {
                        triangleCount_logger.log("Error writing to socket", "error");
                    }

                    bzero(data, 301);
                    read(sockfd, data, 300);
                    response = (data);
                    response = Utils::trim_copy(response);

                    if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
                        result_wr = write(sockfd, partitionId.c_str(), partitionId.size());

                        if (result_wr < 0) {
                            triangleCount_logger.log("Error writing to socket", "error");
                        }

                        bzero(data, 301);
                        read(sockfd, data, 300);
                        response = (data);
                        isFileAccessible = Utils::trim_copy(response);
                    }
                }
            } else if (fileType.compare(JasmineGraphInstanceProtocol::FILE_TYPE_CENTRALSTORE_COMPOSITE) == 0) {
                if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
                    size_t lastindex = fileName.find_last_of(".");
                    string rawname = fileName.substr(0, lastindex);
                    result_wr = write(sockfd, rawname.c_str(), rawname.size());

                    if (result_wr < 0) {
                        triangleCount_logger.log("Error writing to socket", "error");
                    }

                    bzero(data, 301);
                    read(sockfd, data, 300);
                    response = (data);
                    isFileAccessible = Utils::trim_copy(response);
                }
            }
        }
    }

    return isFileAccessible;
}

std::string TriangleCountExecutor::copyCompositeCentralStoreToAggregator(std::string aggregatorHostName,
                                                                         std::string aggregatorPort,
                                                                         std::string aggregatorDataPort,
                                                                         std::string fileName, std::string masterIP) {
    int sockfd;
    char data[301];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;
    std::string aggregatorFilePath = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.aggregatefolder");
    std::string aggregateStoreFile = aggregatorFilePath + "/" + fileName;

    int fileSize = Utils::getFileSize(aggregateStoreFile);
    std::string fileLength = to_string(fileSize);

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot create socket" << std::endl;
        return 0;
    }

    if (aggregatorHostName.find('@') != std::string::npos) {
        aggregatorHostName = Utils::split(aggregatorHostName, '@')[1];
    }

    server = gethostbyname(aggregatorHostName.c_str());
    if (server == NULL) {
        triangleCount_logger.error("ERROR, no host named " + aggregatorHostName);
        return 0;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(atoi(aggregatorPort.c_str()));
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
        // TODO::exit
        return 0;
    }

    bzero(data, 301);
    int result_wr =
        write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if (result_wr < 0) {
        triangleCount_logger.log("Error writing to socket", "error");
    }

    triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = Utils::trim_copy(response);

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if (result_wr < 0) {
            triangleCount_logger.log("Error writing to socket", "error");
        }

        triangleCount_logger.log("Sent : " + masterIP, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        } else {
            triangleCount_logger.log("Received : " + response, "error");
        }
        result_wr = write(sockfd, JasmineGraphInstanceProtocol::SEND_COMPOSITE_CENTRALSTORE_TO_AGGREGATOR.c_str(),
                          JasmineGraphInstanceProtocol::SEND_COMPOSITE_CENTRALSTORE_TO_AGGREGATOR.size());

        if (result_wr < 0) {
            triangleCount_logger.log("Error writing to socket", "error");
        }

        triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_COMPOSITE_CENTRALSTORE_TO_AGGREGATOR,
                                 "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = Utils::trim_copy(response);

        if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_NAME) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME, "info");
            result_wr = write(sockfd, fileName.c_str(), fileName.size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }

            triangleCount_logger.log("Sent : File Name " + fileName, "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = Utils::trim_copy(response);

            if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_LEN) == 0) {
                triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");
                result_wr = write(sockfd, fileLength.c_str(), fileLength.size());

                if (result_wr < 0) {
                    triangleCount_logger.log("Error writing to socket", "error");
                }

                triangleCount_logger.log("Sent : File Length: " + fileLength, "info");

                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);
                response = Utils::trim_copy(response);

                if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_CONT) == 0) {
                    triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_CONT, "info");
                    triangleCount_logger.log("Going to send file through service", "info");
                    JasmineGraphServer::sendFileThroughService(aggregatorHostName,
                                                               std::atoi(aggregatorDataPort.c_str()), fileName,
                                                               aggregateStoreFile, masterIP);
                }
            }
        }

        int count = 0;

        while (true) {
            result_wr = write(sockfd, JasmineGraphInstanceProtocol::FILE_RECV_CHK.c_str(),
                              JasmineGraphInstanceProtocol::FILE_RECV_CHK.size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }

            triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::FILE_RECV_CHK, "info");
            triangleCount_logger.log("Checking if file is received", "info");
            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);

            if (response.compare(JasmineGraphInstanceProtocol::FILE_RECV_WAIT) == 0) {
                triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_RECV_WAIT, "info");
                triangleCount_logger.log("Checking file status : " + to_string(count), "info");
                count++;
                sleep(1);
                continue;
            } else if (response.compare(JasmineGraphInstanceProtocol::FILE_ACK) == 0) {
                triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_ACK, "info");
                triangleCount_logger.log("File transfer completed for file : " + aggregateStoreFile, "info");
                break;
            }
        }

        // Next we wait till the batch upload completes
        while (true) {
            result_wr = write(sockfd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.c_str(),
                              JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }

            triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);

            if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT) == 0) {
                triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT, "info");
                sleep(1);
                continue;
            } else if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK) == 0) {
                triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK, "info");
                triangleCount_logger.log("CentralStore partition file upload completed", "info");
                break;
            }
        }
    } else {
        triangleCount_logger.log("There was an error in the upload process and the response is :: " + response,
                                 "error");
    }
    return response;
}

string TriangleCountExecutor::countCompositeCentralStoreTriangles(std::string aggregatorHostName,
                                                                  std::string aggregatorPort,
                                                                  std::string compositeCentralStoreFileList,
                                                                  std::string masterIP, std::string availableFileList,
                                                                  int threadPriority) {
    int sockfd;
    char data[301];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot create socket" << std::endl;
        return 0;
    }

    server = gethostbyname(aggregatorHostName.c_str());
    if (server == NULL) {
        triangleCount_logger.error("ERROR, no host named " + aggregatorHostName);
        return 0;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(atoi(aggregatorPort.c_str()));
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
        // TODO::exit
        return 0;
    }

    bzero(data, 301);
    int result_wr =
        write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if (result_wr < 0) {
        triangleCount_logger.log("Error writing to socket", "error");
    }

    triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = Utils::trim_copy(response);

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if (result_wr < 0) {
            triangleCount_logger.log("Error writing to socket", "error");
        }

        triangleCount_logger.log("Sent : " + masterIP, "info");
        triangleCount_logger.log("Port : " + aggregatorPort, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        } else {
            triangleCount_logger.log("Received : " + response, "error");
        }
        result_wr = write(sockfd, JasmineGraphInstanceProtocol::AGGREGATE_COMPOSITE_CENTRALSTORE_TRIANGLES.c_str(),
                          JasmineGraphInstanceProtocol::AGGREGATE_COMPOSITE_CENTRALSTORE_TRIANGLES.size());

        if (result_wr < 0) {
            triangleCount_logger.log("Error writing to socket", "error");
        }

        triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::AGGREGATE_COMPOSITE_CENTRALSTORE_TRIANGLES,
                                 "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = Utils::trim_copy(response);

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, availableFileList.c_str(), availableFileList.size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }

            triangleCount_logger.log("Sent : Available File List " + availableFileList, "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = Utils::trim_copy(response);
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");

            std::vector<std::string> chunksVector;

            for (unsigned i = 0; i < compositeCentralStoreFileList.length(); i += INSTANCE_DATA_LENGTH - 10) {
                std::string chunk = compositeCentralStoreFileList.substr(i, INSTANCE_DATA_LENGTH - 10);
                if (i + INSTANCE_DATA_LENGTH - 10 < compositeCentralStoreFileList.length()) {
                    chunk += "/SEND";
                } else {
                    chunk += "/CMPT";
                }
                chunksVector.push_back(chunk);
            }

            for (int loopCount = 0; loopCount < chunksVector.size(); loopCount++) {
                if (loopCount == 0) {
                    std::string chunk = chunksVector.at(loopCount);
                    write(sockfd, chunk.c_str(), chunk.size());
                } else {
                    bzero(data, 301);
                    read(sockfd, data, 300);
                    string chunkStatus = (data);
                    std::string chunk = chunksVector.at(loopCount);
                    write(sockfd, chunk.c_str(), chunk.size());
                }
            }

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = Utils::trim_copy(response);
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, std::to_string(threadPriority).c_str(), std::to_string(threadPriority).size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }

            triangleCount_logger.log("Sent : Thread Priority " + std::to_string(threadPriority), "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = Utils::trim_copy(response);
            string status = response.substr(response.size() - 5);
            std::string result = response.substr(0, response.size() - 5);

            while (status.compare("/SEND") == 0) {
                result_wr = write(sockfd, status.c_str(), status.size());

                if (result_wr < 0) {
                    triangleCount_logger.log("Error writing to socket", "error");
                }
                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);
                response = Utils::trim_copy(response);
                status = response.substr(response.size() - 5);
                std::string triangleResponse = response.substr(0, response.size() - 5);
                result = result + triangleResponse;
            }
            response = result;
        }

        triangleCount_logger.log("Aggregate Response Received", "info");

    } else {
        triangleCount_logger.log("There was an error in the upload process and the response is :: " + response,
                                 "error");
    }
    return response;
}

std::vector<std::vector<string>> TriangleCountExecutor::getWorkerCombination(SQLiteDBInterface *sqlite,
                                                                             std::string graphId) {
    std::set<string> workerIdSet;

    string sqlStatement =
        "SELECT worker_idworker "
        "FROM worker_has_partition INNER JOIN worker ON worker_has_partition.worker_idworker=worker.idworker "
        "WHERE partition_graph_idgraph=" +
        graphId + ";";

    std::vector<vector<pair<string, string>>> results = sqlite->runSelect(sqlStatement);

    for (std::vector<vector<pair<string, string>>>::iterator i = results.begin(); i != results.end(); ++i) {
        std::vector<pair<string, string>> rowData = *i;

        string workerId = rowData.at(0).second;

        workerIdSet.insert(workerId);
    }

    std::vector<string> workerIdVector(workerIdSet.begin(), workerIdSet.end());

    std::vector<std::vector<string>> workerIdCombination = getCombinations(workerIdVector);

    return workerIdCombination;
}

std::string TriangleCountExecutor::copyCentralStoreToAggregator(std::string aggregatorHostName,
                                                                std::string aggregatorPort,
                                                                std::string aggregatorDataPort, int graphId,
                                                                int partitionId, std::string masterIP) {
    int sockfd;
    char data[301];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;
    std::string aggregatorFilePath = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.aggregatefolder");
    std::string fileName = std::to_string(graphId) + "_centralstore_" + std::to_string(partitionId) + ".gz";
    std::string centralStoreFile = aggregatorFilePath + "/" + fileName;

    int fileSize = Utils::getFileSize(centralStoreFile);
    std::string fileLength = to_string(fileSize);

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot create socket" << std::endl;
        return 0;
    }

    server = gethostbyname(aggregatorHostName.c_str());
    if (server == NULL) {
        triangleCount_logger.error("ERROR, no host named " + aggregatorHostName);
        return 0;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(atoi(aggregatorPort.c_str()));
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
        // TODO::exit
        return 0;
    }

    bzero(data, 301);
    int result_wr =
        write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if (result_wr < 0) {
        triangleCount_logger.log("Error writing to socket", "error");
    }

    triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = Utils::trim_copy(response);

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if (result_wr < 0) {
            triangleCount_logger.log("Error writing to socket", "error");
        }

        triangleCount_logger.log("Sent : " + masterIP, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        } else {
            triangleCount_logger.log("Received : " + response, "error");
        }
        result_wr = write(sockfd, JasmineGraphInstanceProtocol::SEND_CENTRALSTORE_TO_AGGREGATOR.c_str(),
                          JasmineGraphInstanceProtocol::SEND_CENTRALSTORE_TO_AGGREGATOR.size());

        if (result_wr < 0) {
            triangleCount_logger.log("Error writing to socket", "error");
        }

        triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_CENTRALSTORE_TO_AGGREGATOR, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = Utils::trim_copy(response);

        if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_NAME) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME, "info");
            result_wr = write(sockfd, fileName.c_str(), fileName.size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }

            triangleCount_logger.log("Sent : File Name " + fileName, "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = Utils::trim_copy(response);

            if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_LEN) == 0) {
                triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");
                result_wr = write(sockfd, fileLength.c_str(), fileLength.size());

                if (result_wr < 0) {
                    triangleCount_logger.log("Error writing to socket", "error");
                }

                triangleCount_logger.log("Sent : File Length: " + fileLength, "info");

                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);
                response = Utils::trim_copy(response);

                if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_CONT) == 0) {
                    triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_CONT, "info");
                    triangleCount_logger.log("Going to send file through service", "info");
                    JasmineGraphServer::sendFileThroughService(aggregatorHostName,
                                                               std::atoi(aggregatorDataPort.c_str()), fileName,
                                                               centralStoreFile, masterIP);
                }
            }
        }

        int count = 0;

        while (true) {
            result_wr = write(sockfd, JasmineGraphInstanceProtocol::FILE_RECV_CHK.c_str(),
                              JasmineGraphInstanceProtocol::FILE_RECV_CHK.size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }

            triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::FILE_RECV_CHK, "info");
            triangleCount_logger.log("Checking if file is received", "info");
            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);

            if (response.compare(JasmineGraphInstanceProtocol::FILE_RECV_WAIT) == 0) {
                triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_RECV_WAIT, "info");
                triangleCount_logger.log("Checking file status : " + to_string(count), "info");
                count++;
                sleep(1);
                continue;
            } else if (response.compare(JasmineGraphInstanceProtocol::FILE_ACK) == 0) {
                triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_ACK, "info");
                triangleCount_logger.log("File transfer completed for file : " + centralStoreFile, "info");
                break;
            }
        }

        // Next we wait till the batch upload completes
        while (true) {
            result_wr = write(sockfd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.c_str(),
                              JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }

            triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);

            if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT) == 0) {
                triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT, "info");
                sleep(1);
                continue;
            } else if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK) == 0) {
                triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK, "info");
                triangleCount_logger.log("CentralStore partition file upload completed", "info");
                break;
            }
        }
    } else {
        triangleCount_logger.log("There was an error in the upload process and the response is :: " + response,
                                 "error");
    }
    return response;
}

string TriangleCountExecutor::countCentralStoreTriangles(std::string aggregatorPort, std::string host,
                                                         std::string partitionId, std::string partitionIdList,
                                                         std::string graphId, std::string masterIP,
                                                         int threadPriority) {
    int sockfd;
    char data[301];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot create socket" << std::endl;
        return 0;
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        triangleCount_logger.error("ERROR, no host named " + host);
        return 0;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(atoi(aggregatorPort.c_str()));
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
        // TODO::exit
        return 0;
    }

    bzero(data, 301);
    int result_wr =
        write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if (result_wr < 0) {
        triangleCount_logger.log("Error writing to socket", "error");
    }

    triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = Utils::trim_copy(response);

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if (result_wr < 0) {
            triangleCount_logger.log("Error writing to socket", "error");
        }

        triangleCount_logger.log("Sent : " + masterIP, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        } else {
            triangleCount_logger.log("Received : " + response, "error");
        }
        result_wr = write(sockfd, JasmineGraphInstanceProtocol::AGGREGATE_CENTRALSTORE_TRIANGLES.c_str(),
                          JasmineGraphInstanceProtocol::AGGREGATE_CENTRALSTORE_TRIANGLES.size());

        if (result_wr < 0) {
            triangleCount_logger.log("Error writing to socket", "error");
        }

        triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::AGGREGATE_CENTRALSTORE_TRIANGLES, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = Utils::trim_copy(response);

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, graphId.c_str(), graphId.size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }

            triangleCount_logger.log("Sent : Graph ID " + graphId, "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = Utils::trim_copy(response);
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, partitionId.c_str(), partitionId.size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }

            triangleCount_logger.log("Sent : Partition ID " + partitionId, "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = Utils::trim_copy(response);
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, partitionIdList.c_str(), partitionIdList.size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }

            triangleCount_logger.log("Sent : Partition ID List : " + partitionId, "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = Utils::trim_copy(response);
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, std::to_string(threadPriority).c_str(), std::to_string(threadPriority).size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }

            triangleCount_logger.log("Sent : Thread Priority " + std::to_string(threadPriority), "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = Utils::trim_copy(response);
            string status = response.substr(response.size() - 5);
            std::string result = response.substr(0, response.size() - 5);

            while (status == "/SEND") {
                result_wr = write(sockfd, status.c_str(), status.size());

                if (result_wr < 0) {
                    triangleCount_logger.log("Error writing to socket", "error");
                }
                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);
                response = Utils::trim_copy(response);
                status = response.substr(response.size() - 5);
                std::string triangleResponse = response.substr(0, response.size() - 5);
                result = result + triangleResponse;
            }
            response = result;
        }

    } else {
        triangleCount_logger.log("There was an error in the upload process and the response is :: " + response,
                                 "error");
    }
    return response;
}

int TriangleCountExecutor::getUid() {
    static std::atomic<std::uint32_t> uid{0};
    return ++uid;
}
