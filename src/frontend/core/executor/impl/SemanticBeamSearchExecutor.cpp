/**
Copyright 2025 JasmineGraph Team
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
#include "SemanticBeamSearchExecutor.h"

#include "../../../../server/JasmineGraphServer.h"

Logger semantic_beam_search_logger_executor;

SemanticBeamSearchExecutor::SemanticBeamSearchExecutor() {}

SemanticBeamSearchExecutor::SemanticBeamSearchExecutor(
    SQLiteDBInterface* db, PerformanceSQLiteDBInterface* perfDb,
    JobRequest jobRequest) {
    this->sqlite = db;
    this->perfDB = perfDb;
    this->request = jobRequest;
}

void SemanticBeamSearchExecutor::execute() {
    int uniqueId = getUid();
    std::string masterIP = request.getMasterIP();
    std::string graphId = request.getParameter(Conts::PARAM_KEYS::GRAPH_ID);
    std::string queryString =
        request.getParameter(Conts::PARAM_KEYS::CYPHER_QUERY::QUERY_STRING);
    int numberOfPartitions =
        std::stoi(request.getParameter(Conts::PARAM_KEYS::NO_OF_PARTITIONS));
    std::string canCalibrateString =
        request.getParameter(Conts::PARAM_KEYS::CAN_CALIBRATE);
    std::string autoCalibrateString =
        request.getParameter(Conts::PARAM_KEYS::AUTO_CALIBRATION);
    std::string queueTime = request.getParameter(Conts::PARAM_KEYS::QUEUE_TIME);
    int connFd =
        std::stoi(request.getParameter(Conts::PARAM_KEYS::CONN_FILE_DESCRIPTOR));
    bool* loop_exit = reinterpret_cast<bool*>(static_cast<std::uintptr_t>(
        std::stoull(request.getParameter(Conts::PARAM_KEYS::LOOP_EXIT_POINTER))));
    const auto& workerList = JasmineGraphServer::getWorkers(numberOfPartitions);
    bool canCalibrate = Utils::parseBoolean(canCalibrateString);
    bool autoCalibrate = Utils::parseBoolean(autoCalibrateString);
    std::vector<std::future<void>> intermRes;
    std::vector<std::future<int>> statResponse;
    std::string workerListString;

    auto begin = chrono::high_resolution_clock::now();

    int counter = 0;

    for (const auto& worker : workerList) {
        counter++;
        workerListString += worker.hostname + ":" +
                            std::to_string(worker.port) + ":" +
                            std::to_string(worker.dataPort);

        if (counter < numberOfPartitions) {
            workerListString += ",";
        }
    }

    std::vector<std::unique_ptr<SharedBuffer>> bufferPool;
    bufferPool.reserve(numberOfPartitions);  // Pre-allocate space for pointers
    for (size_t i = 0; i < numberOfPartitions; ++i) {
        bufferPool.emplace_back(std::make_unique<SharedBuffer>(MASTER_BUFFER_SIZE));
    }
    std::vector<std::thread> readThreads;
    int count = 0;

    std::vector<std::thread> workerThreads;
    count = 0;
    for (auto worker : workerList) {
        workerThreads.emplace_back(doSemanticBeamSearch, worker.hostname,
                                   worker.port, masterIP, std::stoi(graphId), count,
                                   queryString, std::ref(*bufferPool[count]),
                                   numberOfPartitions, workerListString);
        count++;
    }
    vector<json> results;
    int closeFlag = 0;
    int result_wr;
    for (size_t i = 0; i < bufferPool.size(); ++i) {
        readThreads.emplace_back([&, i]() {
      semantic_beam_search_logger_executor.info(
          "Starting read thread for bufferPool[" + std::to_string(i) + "]");
      while (true) {
        std::string data = bufferPool[i]->get();
        semantic_beam_search_logger_executor.info(
            "Fetched data from bufferPool[" + std::to_string(i) + "]: " + data);
        if (data == "-1") {
          break;
        }
        results.push_back(json::parse(data));
      } });
    }
    for (auto& t : readThreads) {
        if (t.joinable()) {
            t.join();
        }
    }

    for (auto& thread : workerThreads) {
        if (thread.joinable()) {
            thread.join();
        }
    }

    // sort based score and trim to top k
    std::sort(results.begin(), results.end(), [](const json& a, const json& b) { return a["score"] > b["score"]; });

    // trim to top k
    int k = 10;
    if ((int)results.size() > k)
        results.resize(k);

    // write to socket
    count = 0;
    for (const auto& res : results) {
        std::string data = res.dump();
        count++;
        result_wr = write(connFd, data.c_str(), data.length());
        result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(),
                          Conts::CARRIAGE_RETURN_NEW_LINE.size());
        if (result_wr < 0) {
            semantic_beam_search_logger_executor.error("Error writing to socket");
            *loop_exit = true;
            break;
        }
    }
    semantic_beam_search_logger_executor.info(
        "###CYPHER-QUERY-EXECUTOR### Executing Query : Fetching Results");

    semantic_beam_search_logger_executor.info(
        "###CYPHER-QUERY-EXECUTOR### Executing Query : Completed");

    workerResponded = true;
    JobResponse jobResponse;
    jobResponse.setJobId(request.getJobId());
    responseVector.push_back(jobResponse);

    responseVectorMutex.lock();
    responseMap[request.getJobId()] = jobResponse;
    responseVectorMutex.unlock();

    auto end = chrono::high_resolution_clock::now();
    auto dur = end - begin;
    auto msDuration =
        std::chrono::duration_cast<std::chrono::milliseconds>(dur).count();

    std::string durationString = std::to_string(msDuration);

    if (canCalibrate || autoCalibrate) {
        Utils::updateSLAInformation(perfDB, graphId, numberOfPartitions, msDuration,
                                    CYPHER, Conts::SLA_CATEGORY::LATENCY);
        isStatCollect = false;
    }

    processStatusMutex.lock();
    for (auto processCompleteIterator = processData.begin();
         processCompleteIterator != processData.end();
         ++processCompleteIterator) {
        ProcessInfo processInformation = *processCompleteIterator;
        if (processInformation.id == uniqueId) {
            processData.erase(processInformation);
            break;
        }
    }
    processStatusMutex.unlock();
}

void SemanticBeamSearchExecutor::doSemanticBeamSearch(
    std::string host, int port, std::string masterIP, int graphID,
    int partitionId, std::string query, SharedBuffer& sharedBuffer,
    int noOfPartitions, const std::string& workerListString) {
    Utils::sendSbsQueryPlanToWorker(host, port, masterIP, graphID, partitionId, query, sharedBuffer, workerListString);
}

int SemanticBeamSearchExecutor::getUid() {
    static std::atomic<std::uint32_t> uid{0};
    return ++uid;
}
