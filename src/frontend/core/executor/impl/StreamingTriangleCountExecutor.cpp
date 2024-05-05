/**
Copyright 2020-2024 JasmineGraph Team
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**/

#include "StreamingTriangleCountExecutor.h"

#define DATA_BUFFER_SIZE (FRONTEND_DATA_LENGTH + 1)

std::map<int, int> StreamingTriangleCountExecutor::local_socket_map; // port:socket
std::map<int, int> StreamingTriangleCountExecutor::central_socket_map; // port:socket

Logger streaming_triangleCount_logger;
std::unordered_map<long, std::unordered_map<long, std::unordered_set<long>>>
        StreamingTriangleCountExecutor::triangleTree;
long StreamingTriangleCountExecutor::triangleCount;

void saveLocalValues(StreamingSQLiteDBInterface stramingdb, std::string graphID, std::string partitionID,
                      NativeStoreTriangleResult result);
void saveCentralValues(StreamingSQLiteDBInterface stramingdb, std::string graphID, std::string trianglesValue);
NativeStoreTriangleResult retrieveLocalValues(StreamingSQLiteDBInterface stramingdb,
                                              std::string graphID, std::string partitionID);
std::string retrieveCentralValues(StreamingSQLiteDBInterface stramingdb, std::string graphID);
std::string getCentralRelationCount(StreamingSQLiteDBInterface stramingdb,
                                    std::string graphID, std::string partitionID);

StreamingTriangleCountExecutor::StreamingTriangleCountExecutor() {}

StreamingTriangleCountExecutor::StreamingTriangleCountExecutor(SQLiteDBInterface *db, JobRequest jobRequest) {
    this->sqlite = db;
    this->request = jobRequest;
    streamingDB = *new StreamingSQLiteDBInterface();
}

void StreamingTriangleCountExecutor::execute() {
    std::string masterIP = request.getMasterIP();
    std::string graphId = request.getParameter(Conts::PARAM_KEYS::GRAPH_ID);
    std::string mode = request.getParameter(Conts::PARAM_KEYS::MODE);
    std::string partitions = request.getParameter(Conts::PARAM_KEYS::PARTITION);

    streamingDB.init();

    streaming_triangleCount_logger.info(
            "###STREAMING-TRIANGLE-COUNT-EXECUTOR### Started with graph ID : " + graphId +
            " Master IP : " + masterIP);

    vector<Utils::worker> workerList = Utils::getWorkerList(sqlite);
    int partitionCount = stoi(partitions);
    std::vector<std::future<long>> intermRes;
    long result = 0;

    streaming_triangleCount_logger.info("###STREAMING-TRIANGLE-COUNT-EXECUTOR### Completed central store counting");

    for (int i = 0; i < partitionCount; i++) {
        Utils::worker currentWorker = workerList.at(i);
        string host = currentWorker.hostname;

        int workerPort = atoi(string(currentWorker.port).c_str());
        int workerDataPort = atoi(string(currentWorker.dataPort).c_str());

        intermRes.push_back(std::async(
                std::launch::async, StreamingTriangleCountExecutor::getTriangleCount, atoi(graphId.c_str()),
                host, workerPort, workerDataPort, i, masterIP, mode, streamingDB));
    }

    if (partitionCount > 2) {
        long aggregatedTriangleCount = StreamingTriangleCountExecutor::aggregateCentralStoreTriangles(
                sqlite, streamingDB, graphId, masterIP, mode, partitionCount);
        if (mode == "0") {
            saveCentralValues(streamingDB, graphId, std::to_string(aggregatedTriangleCount));
            result += aggregatedTriangleCount;
        } else {
            long old_result = stol(retrieveCentralValues(streamingDB, graphId));
            saveCentralValues(streamingDB, graphId, std::to_string(aggregatedTriangleCount + old_result));
            result += (aggregatedTriangleCount + old_result);
        }
    }

    for (auto &&futureCall : intermRes) {
        result += futureCall.get();
    }

    streaming_triangleCount_logger.info("###STREAMING-TRIANGLE-COUNT-EXECUTOR### Completed local counting");

    streaming_triangleCount_logger.info(
            "###STREAMING-TRIANGLE-COUNT-EXECUTOR### Getting Triangle Count : Completed: Triangles " +
            to_string(result));

    JobResponse jobResponse;
    jobResponse.setJobId(request.getJobId());
    jobResponse.addParameter(Conts::PARAM_KEYS::STREAMING_TRIANGLE_COUNT, std::to_string(result));
    jobResponse.setEndTime(chrono::high_resolution_clock::now());
    responseVector.push_back(jobResponse);

    responseMap[request.getJobId()] = jobResponse;
}

long StreamingTriangleCountExecutor::getTriangleCount(int graphId, std::string host, int port,
                                                      int dataPort, int partitionId, std::string masterIP,
                                                      std::string runMode, StreamingSQLiteDBInterface streamingDB) {
    NativeStoreTriangleResult oldResult{1, 1, 0};

    if (runMode == "1") {
       oldResult = retrieveLocalValues(streamingDB, std::to_string(graphId),
                                       std::to_string(partitionId));
    }
    int sockfd;
    if (local_socket_map.find(port) == local_socket_map.end()) {
        struct sockaddr_in serv_addr;
        struct hostent *server;

        local_socket_map[port] = socket(AF_INET, SOCK_STREAM, 0);

        if (sockfd < 0) {
            std::cerr << "Cannot accept connection" << std::endl;
            return 0;
        }

        if (host.find('@') != std::string::npos) {
            host = Utils::split(host, '@')[1];
        }

        streaming_triangleCount_logger.info("###STREAMING-TRIANGLE-COUNT-EXECUTOR### Get Host By Name : " + host);

        server = gethostbyname(host.c_str());
        if (server == NULL) {
            std::cerr << "ERROR, no host named " << server << std::endl;
        }

        bzero((char *)&serv_addr, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
        serv_addr.sin_port = htons(port);

        if (Utils::connect_wrapper(local_socket_map[port], (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
            std::cerr << "ERROR connecting" << std::endl;
        }
    }
    sockfd = local_socket_map[port];
    char data[DATA_BUFFER_SIZE];

    if (!Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE)) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return 0;
    }
    streaming_triangleCount_logger.info("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE);

    string response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) != 0) {
        streaming_triangleCount_logger.error("There was an error in the upload process and the response is : " +
                                             response);
        return 0;
    }
    streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK);

    if (!Utils::send_str_wrapper(sockfd, masterIP)) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return 0;
    }
    streaming_triangleCount_logger.info("Sent : " + masterIP);

    response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) != 0) {
        streaming_triangleCount_logger.error("Received : " + response + " instead of : " +
        JasmineGraphInstanceProtocol::HOST_OK);
        return 0;
    }
    streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);

    if (!Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::INITIATE_STREAMING_TRIAN)) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return 0;
    }
    streaming_triangleCount_logger.info("Sent : " + JasmineGraphInstanceProtocol::INITIATE_STREAMING_TRIAN);

    response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        streaming_triangleCount_logger.error("Received : " + response + " instead of : " +
                                             JasmineGraphInstanceProtocol::HOST_OK);
        return 0;
    }
    streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);

    if (!Utils::send_str_wrapper(sockfd, std::to_string(graphId))) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return 0;
    }
    streaming_triangleCount_logger.info("Sent : Graph ID " + std::to_string(graphId));

    response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        streaming_triangleCount_logger.error("Received : " + response + " instead of : " +
                                             JasmineGraphInstanceProtocol::HOST_OK);
        return 0;
    }
    streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);

    if (!Utils::send_str_wrapper(sockfd, std::to_string(partitionId))) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return 0;
    }
    streaming_triangleCount_logger.info("Sent : Partition ID " + std::to_string(partitionId));

    response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        streaming_triangleCount_logger.error("Received : " + response + " instead of : " +
                                             JasmineGraphInstanceProtocol::HOST_OK);
        return 0;
    }
    streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);

    if (!Utils::send_str_wrapper(sockfd, std::to_string(oldResult.localRelationCount))) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return 0;
    }
    streaming_triangleCount_logger.info("Sent : local relation count " +
    std::to_string(oldResult.localRelationCount));

    response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        streaming_triangleCount_logger.error("Received : " + response + " instead of : " +
                                             JasmineGraphInstanceProtocol::HOST_OK);
        return 0;
    }
    streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);

    if (!Utils::send_str_wrapper(sockfd, std::to_string(oldResult.centralRelationCount))) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return 0;
    }
    streaming_triangleCount_logger.info("Sent : Central relation count " +
                std::to_string(oldResult.centralRelationCount));

    response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        streaming_triangleCount_logger.error("Received : " + response + " instead of : " +
                                             JasmineGraphInstanceProtocol::HOST_OK);
        return 0;
    }
    streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);

    if (!Utils::send_str_wrapper(sockfd, runMode)) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return 0;
    }
    streaming_triangleCount_logger.info("Sent :  mode " + runMode);

    string local_relation_count = Utils::read_str_trim_wrapper(sockfd, data, INSTANCE_DATA_LENGTH);
    streaming_triangleCount_logger.info("Received Local relation count: " + local_relation_count);

    if (!Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::OK)) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return 0;
    }

    string central_relation_count = Utils::read_str_trim_wrapper(sockfd, data, INSTANCE_DATA_LENGTH);
    streaming_triangleCount_logger.info("Received Central relation count: " + central_relation_count);

    if (!Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::OK)) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return 0;
    }

    string triangles = Utils::read_str_trim_wrapper(sockfd, data, INSTANCE_DATA_LENGTH);
    streaming_triangleCount_logger.info("Received result: " + triangles);

    NativeStoreTriangleResult newResult{ std::stol(local_relation_count),
                                         std::stol(central_relation_count),
                                         std::stol(triangles) + oldResult.result};
    saveLocalValues(streamingDB, std::to_string(graphId),
                    std::to_string(partitionId), newResult);
    return newResult.result;
}

long StreamingTriangleCountExecutor::aggregateCentralStoreTriangles(
        SQLiteDBInterface *sqlite, StreamingSQLiteDBInterface streamingdb, std::string graphId, std::string masterIP,
                                                                    std::string runMode, int partitionCount) {
    std::vector<std::vector<string>> workerCombinations = getWorkerCombination(sqlite, graphId, partitionCount);
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
        string aggregatorHost = "";
        std::string partitionIdList = "";
        std::string centralCountList = "";

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
                "SELECT ip,user,server_port,server_data_port "
                "FROM worker "
                "WHERE idworker=" + minWeightWorker + ";";

        std::vector<vector<pair<string, string>>> result = sqlite->runSelect(aggregatorSqlStatement);

        vector<pair<string, string>> aggregatorData = result.at(0);

        std::string aggregatorIp = aggregatorData.at(0).second;
        std::string aggregatorUser = aggregatorData.at(1).second;
        std::string aggregatorPort = aggregatorData.at(2).second;
        std::string aggregatorDataPort = aggregatorData.at(3).second;
        std::string aggregatorPartitionId = minWeightWorker;

        if ((aggregatorIp.find("localhost") != std::string::npos) || aggregatorIp == masterIP) {
            aggregatorHost = aggregatorIp;
        } else {
            aggregatorHost = aggregatorUser + "@" + aggregatorIp;
        }

        for (aggregatorCopyCombinationIterator = workerCombination.begin();
             aggregatorCopyCombinationIterator != workerCombination.end(); ++aggregatorCopyCombinationIterator) {
            std::string workerId = *aggregatorCopyCombinationIterator;
            if (workerId != minWeightWorker) {
                std::string partitionId = workerId;
                partitionIdList += partitionId + ",";
                centralCountList += getCentralRelationCount(streamingdb, graphId, partitionId) + ",";
            }
        }
        centralCountList += getCentralRelationCount(streamingdb, graphId, aggregatorPartitionId);
        std::string adjustedPartitionIdList = partitionIdList.substr(0, partitionIdList.size() - 1);

        workerWeightMap[minWeightWorker] = minimumWeight;

        triangleCountResponse.push_back(std::async(
                std::launch::async, StreamingTriangleCountExecutor::countCentralStoreTriangles, aggregatorHost,
                aggregatorPort, aggregatorHost, aggregatorPartitionId, adjustedPartitionIdList, centralCountList,
                graphId, masterIP, 5, runMode));
    }

    for (auto &&futureCall : triangleCountResponse) {
        result = result + ":" + futureCall.get();
    }

    std::vector<std::string> triangles = Utils::split(result, ':');
    std::vector<std::string>::iterator triangleIterator;
    std::set<std::string> uniqueTriangleSet;

    long currentSize = triangleCount;

    for (triangleIterator = triangles.begin(); triangleIterator != triangles.end(); ++triangleIterator) {
        std::string triangle = *triangleIterator;

        if (!triangle.empty() && triangle != "NILL") {
            if (runMode == "0") {
                uniqueTriangleSet.insert(triangle);
                continue;
            }

            std::vector<std::string> triangleList = Utils::split(triangle, ',');
            long varOne = std::stol(triangleList[0]);
            long varTwo = std::stol(triangleList[1]);
            long varThree = std::stol(triangleList[2]);

            auto &itemRes = triangleTree[varOne];
            auto itemResIterator = itemRes.find(varTwo);
            if (itemResIterator != itemRes.end()) {
                auto &set2 = itemRes[varTwo];
                auto set2Iter = set2.find(varThree);
                if (set2Iter == set2.end()) {
                    set2.insert(varThree);
                    triangleCount++;
                }
            } else {
                triangleTree[varOne][varTwo].insert(varThree);
                triangleCount++;
            }
        }
    }

    if (runMode == "0") {
        return uniqueTriangleSet.size();
    }
    return triangleCount - currentSize;
}

string StreamingTriangleCountExecutor::countCentralStoreTriangles(
        std::string aggregatorHostName, std::string aggregatorPort,
        std::string host, std::string partitionId,
        std::string partitionIdList, std::string centralCountList,
        std::string graphId, std::string masterIP,
        int threadPriority, std::string runMode) {
    int port = stoi(aggregatorPort);
    int sockfd;
    if (central_socket_map.find(port) == central_socket_map.end()) {
        struct sockaddr_in serv_addr;
        struct hostent *server;

        central_socket_map[port] = socket(AF_INET, SOCK_STREAM, 0);

        if (central_socket_map[port] < 0) {
            std::cerr << "Cannot accept connection" << std::endl;
            return 0;
        }

        if (host.find('@') != std::string::npos) {
            host = Utils::split(host, '@')[1];
        }

        streaming_triangleCount_logger.info("###STREAMING-TRIANGLE-COUNT-EXECUTOR### Get Host By Name : " + host);

        server = gethostbyname(host.c_str());
        if (server == NULL) {
            std::cerr << "ERROR, no host named " << server << std::endl;
        }

        bzero((char *)&serv_addr, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
        serv_addr.sin_port = htons(port);

        if (Utils::connect_wrapper(central_socket_map[port], (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
            std::cerr << "ERROR connecting" << std::endl;
        }
    }
    sockfd = central_socket_map[port];

    char data[DATA_BUFFER_SIZE];
    std::string result = "";

    if (!Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE)) {
        streaming_triangleCount_logger.error("Error writing to socket");
    }
    streaming_triangleCount_logger.info("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE);

    string response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) != 0) {
        streaming_triangleCount_logger.error("Received : " + response + " instead of : " +
                                             JasmineGraphInstanceProtocol::HANDSHAKE_OK);
        return result;
    }
    streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);

    if (!Utils::send_str_wrapper(sockfd, masterIP)) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return result;
    }
    streaming_triangleCount_logger.info("Sent : " + masterIP);

    response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) != 0) {
        streaming_triangleCount_logger.error("Received : " + response + " instead of : " +
                                             JasmineGraphInstanceProtocol::HOST_OK);
        return result;
    }
    streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);

    if (!Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::AGGREGATE_STREAMING_CENTRALSTORE_TRIANGLES)) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return result;
    }
    streaming_triangleCount_logger.info("Sent : " +
        JasmineGraphInstanceProtocol::AGGREGATE_STREAMING_CENTRALSTORE_TRIANGLES);

    response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        streaming_triangleCount_logger.error("Received : " + response + " instead of : " +
                                             JasmineGraphInstanceProtocol::HOST_OK);
        return result;
    }
    streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);

    if (!Utils::send_str_wrapper(sockfd, graphId)) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return result;
    }
    streaming_triangleCount_logger.info("Sent : Graph ID " + graphId);

    response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        streaming_triangleCount_logger.error("Received : " + response + " instead of : " +
                                             JasmineGraphInstanceProtocol::HOST_OK);
        return result;
    }
    streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);

    if (!Utils::send_str_wrapper(sockfd, partitionId)) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return result;
    }
    streaming_triangleCount_logger.info("Sent : Partition ID " + partitionId);

    response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        streaming_triangleCount_logger.error("Received : " + response + " instead of : " +
                                             JasmineGraphInstanceProtocol::HOST_OK);
        return result;
    }
    streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);

    if (!Utils::send_str_wrapper(sockfd, partitionIdList)) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return result;
    }
    streaming_triangleCount_logger.info("Sent : Partition ID List : " + partitionIdList);

    response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        streaming_triangleCount_logger.error("Received : " + response + " instead of : " +
                                             JasmineGraphInstanceProtocol::HOST_OK);
        return result;
    }
    streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);

    if (!Utils::send_str_wrapper(sockfd, centralCountList)) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return result;
    }
    streaming_triangleCount_logger.info("Sent : Central count list : " + centralCountList);

    response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        streaming_triangleCount_logger.error("Received : " + response + " instead of : " +
                                             JasmineGraphInstanceProtocol::HOST_OK);
        return result;
    }
    streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);

    if (!Utils::send_str_wrapper(sockfd, std::to_string(threadPriority))) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return result;
    }
    streaming_triangleCount_logger.info("Sent : Priority: " + threadPriority);

    response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        streaming_triangleCount_logger.error("Received : " + response + " instead of : " +
                                             JasmineGraphInstanceProtocol::HOST_OK);
        return result;
    }
    streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);

    if (!Utils::send_str_wrapper(sockfd, runMode)) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return result;
    }
    streaming_triangleCount_logger.info("Sent : mode " + runMode);

    response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    response = Utils::trim_copy(response, " \f\n\r\t\v");
    string status = response.substr(response.size() - 5);
    result = response.substr(0, response.size() - 5);

    while (status == "/SEND") {
        if (!Utils::send_str_wrapper(sockfd, status)) {
            streaming_triangleCount_logger.error("Error writing to socket");
        }
        response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
        response = Utils::trim_copy(response, " \f\n\r\t\v");
        status = response.substr(response.size() - 5);
        std::string triangleResponse = response.substr(0, response.size() - 5);
        result = result + triangleResponse;
    }
    response = result;

    return response;
}

std::vector<std::vector<string>> StreamingTriangleCountExecutor::getWorkerCombination(
        SQLiteDBInterface *sqlite, std::string graphId, int partitionCount) {
    std::set<string> workerIdSet;

    for (int i = 0; i < partitionCount; i++) {
        workerIdSet.insert(std::to_string(i));
    }

    std::vector<string> workerIdVector(workerIdSet.begin(), workerIdSet.end());

    std::vector<std::vector<string>> workerIdCombination = AbstractExecutor::getCombinations(workerIdVector);

    return workerIdCombination;
}

void saveLocalValues(StreamingSQLiteDBInterface sqlite, std::string graphID, std::string partitionId,
                      NativeStoreTriangleResult result) {
        // Row doesn't exist, insert a new row
        std::string insertQuery = "INSERT OR REPLACE into streaming_partition (partition_id, local_edges, "
                                  "triangles, central_edges, graph_id) VALUES ("
                                  + partitionId + ", "
                                  + std::to_string(result.localRelationCount) + ", "
                                  + std::to_string(result.result) + ", "
                                  + std::to_string(result.centralRelationCount) + ", "
                                  + graphID + ")";

        int newGraphID = sqlite.runInsert(insertQuery);

        if (newGraphID == -1) {
            streaming_triangleCount_logger.error("Streaming local values insertion failed.");
        }
}

void saveCentralValues(StreamingSQLiteDBInterface sqlite, std::string graphID, std::string trianglesValue) {
    // Row doesn't exist, insert a new row
    std::string insertQuery = "INSERT OR REPLACE INTO central_store (triangles, graph_id) VALUES ("
                              + trianglesValue + ", "
                              + graphID + ")";

    int newGraphID = sqlite.runInsert(insertQuery);

    if (newGraphID == -1) {
        streaming_triangleCount_logger.error("Streaming central values insertion failed.");
    }
}

NativeStoreTriangleResult retrieveLocalValues(StreamingSQLiteDBInterface sqlite, std::string graphID,
                                              std::string partitionId) {
    std::string sqlStatement = "SELECT local_edges, central_edges, triangles FROM streaming_partition "
                               "WHERE partition_id = " + partitionId + " AND graph_id = " + graphID;

    std::vector<std::vector<std::pair<std::string, std::string>>> result = sqlite.runSelect(sqlStatement);

    if (result.empty()) {
        streaming_triangleCount_logger.error("No matching row found for the given partitionID and graphID.");
    return {};  // Return an empty vector
    }

    std::vector<std::pair<std::string, std::string>> aggregatorData = result.at(0);

    NativeStoreTriangleResult new_result;
    new_result.localRelationCount = std::stol(aggregatorData.at(0).second);
    new_result.centralRelationCount = std::stol(aggregatorData.at(1).second);
    new_result.result = std::stol(aggregatorData.at(2).second);

    return  new_result;
}

std::string retrieveCentralValues(StreamingSQLiteDBInterface sqlite, std::string graphID) {
    std::string sqlStatement = "SELECT triangles FROM central_store "
                               "WHERE graph_id = " + graphID;

    std::vector<std::vector<std::pair<std::string, std::string>>> result = sqlite.runSelect(sqlStatement);

    if (result.empty()) {
        streaming_triangleCount_logger.error("No matching row found for the given partitionID and graphID.");
        return {};  // Return an empty vector
    }

    std::vector<std::pair<std::string, std::string>> aggregatorData = result.at(0);

    std::string count = aggregatorData.at(0).second;

    return count;
}

std::string getCentralRelationCount(StreamingSQLiteDBInterface sqlite, std::string graphId, std::string partitionId) {
    std::string sqlStatement = "SELECT central_edges FROM streaming_partition "
                               "WHERE graph_id = " + graphId + " and partition_id= " + partitionId;

    std::vector<std::vector<std::pair<std::string, std::string>>> result = sqlite.runSelect(sqlStatement);

    if (result.empty()) {
        streaming_triangleCount_logger.error("No matching row found for the given partitionID and graphID.");
        return {};  // Return an empty vector
    }

    std::vector<std::pair<std::string, std::string>> aggregatorData = result.at(0);

    std::string count = aggregatorData.at(0).second;

    return count;
}
