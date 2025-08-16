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

#include "PageRankExecutor.h"

#define DATA_BUFFER_SIZE (FRONTEND_DATA_LENGTH + 1)
using namespace std::chrono;

Logger pageRank_logger;

PageRankExecutor::PageRankExecutor() {}

PageRankExecutor::PageRankExecutor(SQLiteDBInterface *db, PerformanceSQLiteDBInterface *perfDb, JobRequest jobRequest) {
    this->sqlite = db;
    this->perfDB = perfDb;
    this->request = jobRequest;
}

void PageRankExecutor::execute() {
    int uniqueId = getUid();
    std::string masterIP = request.getMasterIP();
    std::string graphId = request.getParameter(Conts::PARAM_KEYS::GRAPH_ID);
    std::string canCalibrateString = request.getParameter(Conts::PARAM_KEYS::CAN_CALIBRATE);
    std::string queueTime = request.getParameter(Conts::PARAM_KEYS::QUEUE_TIME);
    std::string graphSLAString = request.getParameter(Conts::PARAM_KEYS::GRAPH_SLA);
    std::string alphaString = request.getParameter(Conts::PARAM_KEYS::ALPHA);
    std::string iterationString = request.getParameter(Conts::PARAM_KEYS::ITERATION);

    double alpha = stod(alphaString);
    int iterations = stoi(iterationString);

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
    processInformation.processName = PAGE_RANK;
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

    pageRank_logger.info("###PAGERANK-EXECUTOR### Started with graph ID : " + graphId + " Master IP : " + masterIP);

    int partitionCount = 0;
    std::vector<std::future<void>> intermRes;
    std::vector<std::future<int>> statResponse;

    auto begin = chrono::high_resolution_clock::now();

    std::unordered_map<std::string, JasmineGraphServer::workerPartitions> graphPartitionedHosts =
        JasmineGraphServer::getGraphPartitionedHosts(graphId);
    string host;
    int port;
    int dataPort;
    std::string workerList;

    for (auto workerIter = graphPartitionedHosts.begin(); workerIter != graphPartitionedHosts.end(); workerIter++) {
        JasmineGraphServer::workerPartitions workerPartition = workerIter->second;
        host = workerIter->first;
        port = workerPartition.port;
        dataPort = workerPartition.dataPort;

        for (auto partitionIterator = workerPartition.partitionID.begin();
             partitionIterator != workerPartition.partitionID.end(); partitionIterator++) {
            std::string partition = *partitionIterator;
            workerList.append(host + ":" + std::to_string(port) + ":" + partition + ",");
        }
    }

    workerList.pop_back();
    pageRank_logger.info("Worker list " + workerList);

    for (auto workerIter = graphPartitionedHosts.begin(); workerIter != graphPartitionedHosts.end(); workerIter++) {
        JasmineGraphServer::workerPartitions workerPartition = workerIter->second;
        host = workerIter->first;
        port = workerPartition.port;
        dataPort = workerPartition.dataPort;

        for (auto partitionIterator = workerPartition.partitionID.begin();
             partitionIterator != workerPartition.partitionID.end(); partitionIterator++) {
            std::string partition = *partitionIterator;
            intermRes.push_back(std::async(std::launch::async, PageRankExecutor::doPageRank, graphId, alpha, iterations,
                                           partition, host, port, dataPort, workerList));
        }
    }

    PerformanceUtil::init();

    std::string query =
        "SELECT attempt from graph_sla INNER JOIN sla_category where graph_sla.id_sla_category=sla_category.id and "
        "graph_sla.graph_id='" +
        graphId + "' and graph_sla.partition_count='" + std::to_string(partitionCount) +
        "' and sla_category.category='" + Conts::SLA_CATEGORY::LATENCY + "' and sla_category.command='" + PAGE_RANK +
        "';";

    std::vector<vector<pair<string, string>>> queryResults = perfDB->runSelect(query);

    if (queryResults.size() > 0) {
        std::string attemptString = queryResults[0][0].second;
        int calibratedAttempts = atoi(attemptString.c_str());

        if (calibratedAttempts >= Conts::MAX_SLA_CALIBRATE_ATTEMPTS) {
            canCalibrate = false;
        }
    } else {
        pageRank_logger.info("###PAGERANK-EXECUTOR### Inserting initial record for SLA ");
        Utils::updateSLAInformation(perfDB, graphId, partitionCount, 0, PAGE_RANK, Conts::SLA_CATEGORY::LATENCY);
        statResponse.push_back(std::async(std::launch::async, AbstractExecutor::collectPerformaceData, perfDB,
                                          graphId.c_str(), PAGE_RANK, Conts::SLA_CATEGORY::LATENCY, partitionCount,
                                          masterIP, autoCalibrate));
        isStatCollect = true;
    }

    for (auto &&futureCall : intermRes) {
        futureCall.get();
    }

    pageRank_logger.info("###PAGERANK-EXECUTOR### Getting PageRank : Completed");

    workerResponded = true;
    JobResponse jobResponse;
    jobResponse.setJobId(request.getJobId());
    responseVector.push_back(jobResponse);

    responseVectorMutex.lock();
    responseMap[request.getJobId()] = jobResponse;
    responseVectorMutex.unlock();

    auto end = chrono::high_resolution_clock::now();
    auto dur = end - begin;
    auto msDuration = std::chrono::duration_cast<std::chrono::milliseconds>(dur).count();

    std::string durationString = std::to_string(msDuration);

    if (canCalibrate || autoCalibrate) {
        Utils::updateSLAInformation(perfDB, graphId, partitionCount, msDuration, PAGE_RANK,
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

int PageRankExecutor::getUid() {
    static std::atomic<std::uint32_t> uid{0};
    return ++uid;
}

void PageRankExecutor::doPageRank(std::string graphID, double alpha, int iterations, string partition, string host,
                                  int port, int dataPort, std::string workerList) {
    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }

    int sockfd;
    char data[DATA_BUFFER_SIZE];
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        pageRank_logger.error("Cannot create socket");
        return;
    }
    server = gethostbyname(host.c_str());
    if (server == NULL) {
        pageRank_logger.error("ERROR, no host named " + host);
        return;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        pageRank_logger.error("Error connecting to socket");
        return;
    }

    if (!Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::PAGE_RANK)) {
        pageRank_logger.error("Error writing to socket");
        return;
    }
    pageRank_logger.info("Sent : " + JasmineGraphInstanceProtocol::PAGE_RANK);

    string response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
        pageRank_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
    } else {
        pageRank_logger.error("Error reading from socket");
        return;
    }

    if (!Utils::send_str_wrapper(sockfd, graphID)) {
        pageRank_logger.error("Error writing to socket");
        return;
    }
    pageRank_logger.info("Sent : Graph ID " + graphID);

    response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
        pageRank_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
    } else {
        pageRank_logger.error("Error reading from socket");
        return;
    }

    if (!Utils::send_str_wrapper(sockfd, partition)) {
        pageRank_logger.error("Error writing to socket");
        return;
    }
    pageRank_logger.info("Sent : Partition ID " + partition);

    response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
        pageRank_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
    } else {
        pageRank_logger.error("Error reading from socket");
        return;
    }

    if (!Utils::send_str_wrapper(sockfd, workerList)) {
        pageRank_logger.error("Error writing to socket");
    }
    pageRank_logger.info("Sent : Host List ");

    response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
        pageRank_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
    } else {
        pageRank_logger.error("Error reading from socket");
        return;
    }

    long graphVertexCount = JasmineGraphServer::getGraphVertexCount(graphID);
    if (!Utils::send_str_wrapper(sockfd, std::to_string(graphVertexCount))) {
        pageRank_logger.error("Error writing to socket");
    }
    pageRank_logger.info("graph vertex count: " + std::to_string(graphVertexCount));

    response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
        pageRank_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
    } else {
        pageRank_logger.error("Error reading from socket");
        return;
    }

    if (!Utils::send_str_wrapper(sockfd, std::to_string(alpha))) {
        pageRank_logger.error("Error writing to socket");
        return;
    }
    pageRank_logger.info("PageRank alpha value sent : " + std::to_string(alpha));

    response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
        pageRank_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
    } else {
        pageRank_logger.error("Error reading from socket");
        return;
    }

    if (!Utils::send_str_wrapper(sockfd, std::to_string(iterations))) {
        pageRank_logger.error("Error writing to socket");
        return;
    }

    response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
        pageRank_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
    } else {
        pageRank_logger.error("Error reading from socket");
        return;
    }

    return;
}
