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

Logger streaming_triangleCount_logger;

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

    for (int i = 0; i < partitionCount; i++) {
        Utils::worker currentWorker = workerList.at(i);
        string host = currentWorker.hostname;

        int workerPort = atoi(string(currentWorker.port).c_str());
        int workerDataPort = atoi(string(currentWorker.dataPort).c_str());

        intermRes.push_back(std::async(
                std::launch::async, StreamingTriangleCountExecutor::getTriangleCount, atoi(graphId.c_str()),
                host, workerPort, workerDataPort, i, masterIP, mode, streamingDB));
    }

    for (auto &&futureCall : intermRes) {
        result += futureCall.get();
    }

    streaming_triangleCount_logger.info("###STREAMING-TRIANGLE-COUNT-EXECUTOR### Completed local counting");
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

    streaming_triangleCount_logger.info(
            "###STREAMING-TRIANGLE-COUNT-EXECUTOR### Getting Triangle Count : Completed: Triangles " +
            to_string(result));

    JobResponse jobResponse;
    jobResponse.setJobId(request.getJobId());
    jobResponse.addParameter(Conts::PARAM_KEYS::STREAMING_TRIANGLE_COUNT, std::to_string(result));
    responseVector.push_back(jobResponse);

    responseMap[request.getJobId()] = jobResponse;
}

long StreamingTriangleCountExecutor::getTriangleCount(int graphId, std::string host, int port,
                                                      int dataPort, int partitionId, std::string masterIP,
                                                      std::string runMode, StreamingSQLiteDBInterface streamingDB) {
    NativeStoreTriangleResult oldResult;
    NativeStoreTriangleResult newResult;
    oldResult.localRelationCount = 1;
    oldResult.centralRelationCount = 1;
    oldResult.result = 0;

    if (runMode == "1") {
       oldResult = retrieveLocalValues(streamingDB, std::to_string(graphId),
                                       std::to_string(partitionId));
    }

    int sockfd;
    char data[FRONTEND_DATA_LENGTH + 1];
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

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
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
    }

    bzero(data, FRONTEND_DATA_LENGTH + 1);
    int result_wr =
            write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(),
                  JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if (result_wr < 0) {
        streaming_triangleCount_logger.error("Error writing to socket");
    }

    streaming_triangleCount_logger.info("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE);
    bzero(data, FRONTEND_DATA_LENGTH + 1);
    read(sockfd, data, FRONTEND_DATA_LENGTH);
    string response = (data);

    response = Utils::trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK);
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if (result_wr < 0) {
            streaming_triangleCount_logger.error("Error writing to socket");
        }

        streaming_triangleCount_logger.info("Sent : " + masterIP);
        bzero(data, FRONTEND_DATA_LENGTH + 1);
        read(sockfd, data, FRONTEND_DATA_LENGTH);
        response = (data);

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::HOST_OK);
        } else {
            streaming_triangleCount_logger.error("Received : " + response);
        }
        result_wr = write(sockfd, JasmineGraphInstanceProtocol::INITIATE_STREAMING_TRIAN.c_str(),
                          JasmineGraphInstanceProtocol::INITIATE_STREAMING_TRIAN.size());

        if (result_wr < 0) {
            streaming_triangleCount_logger.error("Error writing to socket");
        }

        streaming_triangleCount_logger.info("Sent : " + JasmineGraphInstanceProtocol::INITIATE_STREAMING_TRIAN);
        bzero(data, FRONTEND_DATA_LENGTH + 1);
        read(sockfd, data, FRONTEND_DATA_LENGTH);
        response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
            result_wr = write(sockfd, std::to_string(graphId).c_str(), std::to_string(graphId).size());

            if (result_wr < 0) {
                streaming_triangleCount_logger.error("Error writing to socket");
            }

            streaming_triangleCount_logger.info("Sent : Graph ID " + std::to_string(graphId));

            bzero(data, FRONTEND_DATA_LENGTH + 1);
            read(sockfd, data, FRONTEND_DATA_LENGTH);
            response = (data);
            response = Utils::trim_copy(response, " \f\n\r\t\v");
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
            result_wr = write(sockfd, std::to_string(partitionId).c_str(), std::to_string(partitionId).size());

            if (result_wr < 0) {
                streaming_triangleCount_logger.error("Error writing to socket");
            }

            streaming_triangleCount_logger.info("Sent : Partition ID " + std::to_string(partitionId));

            bzero(data, FRONTEND_DATA_LENGTH + 1);
            read(sockfd, data, FRONTEND_DATA_LENGTH);
            response = (data);
            response = Utils::trim_copy(response, " \f\n\r\t\v");
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
            result_wr = write(sockfd, std::to_string(oldResult.localRelationCount).c_str(),
                              std::to_string(oldResult.localRelationCount).size());

            if (result_wr < 0) {
                streaming_triangleCount_logger.error("Error writing to socket");
            }

            streaming_triangleCount_logger.info("Sent : local relation count " +
            std::to_string(oldResult.localRelationCount));

            bzero(data, FRONTEND_DATA_LENGTH + 1);
            read(sockfd, data, FRONTEND_DATA_LENGTH);
            response = (data);
            response = Utils::trim_copy(response, " \f\n\r\t\v");
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
            result_wr = write(sockfd, std::to_string(oldResult.centralRelationCount).c_str(),
                              std::to_string(oldResult.centralRelationCount).size());

            if (result_wr < 0) {
                streaming_triangleCount_logger.error("Error writing to socket");
            }

            streaming_triangleCount_logger.info("Sent : Central relation count " +
                        std::to_string(oldResult.centralRelationCount));

            bzero(data, FRONTEND_DATA_LENGTH + 1);
            read(sockfd, data, FRONTEND_DATA_LENGTH);
            response = (data);
            response = Utils::trim_copy(response, " \f\n\r\t\v");
        }

        int mode = stoi(runMode);
        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
            result_wr = write(sockfd, std::to_string(mode).c_str(), std::to_string(mode).size());

            if (result_wr < 0) {
                streaming_triangleCount_logger.error("Error writing to socket");
            }

            streaming_triangleCount_logger.info("Sent :  mode " + std::to_string(mode));

            string local_relation_count = Utils::read_str_trim_wrapper(sockfd, data, INSTANCE_DATA_LENGTH);
            newResult.localRelationCount = std::stol(local_relation_count);
            streaming_triangleCount_logger.info("Received Local relation count: " + local_relation_count);
        }

        if (Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::OK)) {
            string central_relation_count = Utils::read_str_trim_wrapper(sockfd, data, INSTANCE_DATA_LENGTH);
            newResult.centralRelationCount = std::stol(central_relation_count);
            streaming_triangleCount_logger.info("Received Central relation count: " + central_relation_count);
        }
        if (Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::OK)) {
            string triangles = Utils::read_str_trim_wrapper(sockfd, data, INSTANCE_DATA_LENGTH);

            newResult.result = std::stol(triangles) + oldResult.result;

            streaming_triangleCount_logger.info("Received result: " + triangles);
        }
        saveLocalValues(streamingDB, std::to_string(graphId),
                        std::to_string(partitionId), newResult);
        return newResult.result;

    } else {
        streaming_triangleCount_logger.error("There was an error in the upload process and the response is :: " +
        response);
    }
    return 0;
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

    for (triangleIterator = triangles.begin(); triangleIterator != triangles.end(); ++triangleIterator) {
        std::string triangle = *triangleIterator;

        if (!triangle.empty() && triangle != "NILL") {
            uniqueTriangleSet.insert(triangle);
        }
    }

    aggregatedTriangleCount = uniqueTriangleSet.size();

    return aggregatedTriangleCount;
}

string StreamingTriangleCountExecutor::countCentralStoreTriangles(
        std::string aggregatorHostName, std::string aggregatorPort,
        std::string host, std::string partitionId,
        std::string partitionIdList, std::string centralCountList,
        std::string graphId, std::string masterIP,
        int threadPriority, std::string runMode) {
    int sockfd;
    char data[FRONTEND_DATA_LENGTH + 1];
    struct sockaddr_in serv_addr;
    struct hostent *server;
    int mode = stoi(runMode);

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot create socket" << std::endl;
        return 0;
    }

    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        std::cerr << "ERROR, no host named " << server << std::endl;
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

    bzero(data, FRONTEND_DATA_LENGTH + 1);
    int result_wr =
            write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(),
                  JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if (result_wr < 0) {
        streaming_triangleCount_logger.error("Error writing to socket");
    }

    streaming_triangleCount_logger.info("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE);
    bzero(data, FRONTEND_DATA_LENGTH + 1);
    read(sockfd, data, FRONTEND_DATA_LENGTH);
    string response = (data);

    response = Utils::trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK);
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if (result_wr < 0) {
            streaming_triangleCount_logger.error("Error writing to socket");
        }

        streaming_triangleCount_logger.info("Sent : " + masterIP);
        bzero(data, FRONTEND_DATA_LENGTH + 1);
        read(sockfd, data, FRONTEND_DATA_LENGTH);
        response = (data);

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::HOST_OK);
        } else {
            streaming_triangleCount_logger.error("Received : " + response);
        }
        result_wr = write(sockfd, JasmineGraphInstanceProtocol::AGGREGATE_STREAMING_CENTRALSTORE_TRIANGLES.c_str(),
                          JasmineGraphInstanceProtocol::AGGREGATE_STREAMING_CENTRALSTORE_TRIANGLES.size());

        if (result_wr < 0) {
            streaming_triangleCount_logger.error("Error writing to socket");
        }

        streaming_triangleCount_logger.info("Sent : " +
                    JasmineGraphInstanceProtocol::AGGREGATE_CENTRALSTORE_TRIANGLES);
        bzero(data, FRONTEND_DATA_LENGTH + 1);
        read(sockfd, data, FRONTEND_DATA_LENGTH);
        response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
            result_wr = write(sockfd, graphId.c_str(), graphId.size());

            if (result_wr < 0) {
                streaming_triangleCount_logger.error("Error writing to socket");
            }

            streaming_triangleCount_logger.info("Sent : Graph ID " + graphId);

            bzero(data, FRONTEND_DATA_LENGTH + 1);
            read(sockfd, data, FRONTEND_DATA_LENGTH);
            response = (data);
            response = Utils::trim_copy(response, " \f\n\r\t\v");
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
            result_wr = write(sockfd, partitionId.c_str(), partitionId.size());

            if (result_wr < 0) {
                streaming_triangleCount_logger.error("Error writing to socket");
            }

            streaming_triangleCount_logger.info("Sent : Partition ID " + partitionId);

            bzero(data, FRONTEND_DATA_LENGTH + 1);
            read(sockfd, data, FRONTEND_DATA_LENGTH);
            response = (data);
            response = Utils::trim_copy(response, " \f\n\r\t\v");
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
            result_wr = write(sockfd, partitionIdList.c_str(), partitionIdList.size());

            if (result_wr < 0) {
                streaming_triangleCount_logger.error("Error writing to socket");
            }

            streaming_triangleCount_logger.info("Sent : Partition ID List : " + partitionId);

            bzero(data, FRONTEND_DATA_LENGTH + 1);
            read(sockfd, data, FRONTEND_DATA_LENGTH);
            response = (data);
            response = Utils::trim_copy(response, " \f\n\r\t\v");
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
            result_wr = write(sockfd, centralCountList.c_str(), centralCountList.size());

            if (result_wr < 0) {
                streaming_triangleCount_logger.error("Error writing to socket");
            }

            streaming_triangleCount_logger.info("Sent : Central count list : " + centralCountList);

            bzero(data, FRONTEND_DATA_LENGTH + 1);
            read(sockfd, data, FRONTEND_DATA_LENGTH);
            response = (data);
            response = Utils::trim_copy(response, " \f\n\r\t\v");
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
            result_wr = write(sockfd, std::to_string(threadPriority).c_str(), std::to_string(threadPriority).size());

            if (result_wr < 0) {
                streaming_triangleCount_logger.error("Error writing to socket");
            }

            streaming_triangleCount_logger.info("Sent : Priority: " + threadPriority);

            bzero(data, FRONTEND_DATA_LENGTH + 1);
            read(sockfd, data, FRONTEND_DATA_LENGTH);
            response = (data);
            response = Utils::trim_copy(response, " \f\n\r\t\v");
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
            result_wr = write(sockfd, std::to_string(mode).c_str(), std::to_string(mode).size());

            if (result_wr < 0) {
                streaming_triangleCount_logger.error("Error writing to socket");
            }

            streaming_triangleCount_logger.info("Sent : mode " + std::to_string(mode));

            bzero(data, FRONTEND_DATA_LENGTH + 1);
            read(sockfd, data, FRONTEND_DATA_LENGTH);
            response = (data);
            response = Utils::trim_copy(response, " \f\n\r\t\v");
            string status = response.substr(response.size() - 5);
            std::string result = response.substr(0, response.size() - 5);

            while (status == "/SEND") {
                result_wr = write(sockfd, status.c_str(), status.size());

                if (result_wr < 0) {
                    streaming_triangleCount_logger.error("Error writing to socket");
                }
                bzero(data, FRONTEND_DATA_LENGTH + 1);
                read(sockfd, data, FRONTEND_DATA_LENGTH);
                response = (data);
                response = Utils::trim_copy(response, " \f\n\r\t\v");
                status = response.substr(response.size() - 5);
                std::string triangleResponse = response.substr(0, response.size() - 5);
                result = result + triangleResponse;
            }
            response = result;
        }

    } else {
        streaming_triangleCount_logger.error("There was an error in the upload process and the response is :: " +
        response);
    }
    streaming_triangleCount_logger.info(response);
    return response;
}

std::vector<std::vector<string>> StreamingTriangleCountExecutor::getWorkerCombination(SQLiteDBInterface *sqlite,
                                                                                      std::string graphId, int partitionCount) {
    std::set<string> workerIdSet;

    for (int i = 0; i < partitionCount; i++) {
        workerIdSet.insert(std::to_string(i));
    }

    std::vector<string> workerIdVector(workerIdSet.begin(), workerIdSet.end());

    std::vector<std::vector<string>> workerIdCombination = getCombinations(workerIdVector);

    return workerIdCombination;
}

void saveLocalValues(StreamingSQLiteDBInterface sqlite, std::string graphID, std::string partitionId,
                      NativeStoreTriangleResult result) {
        // Row doesn't exist, insert a new row
        std::string insertQuery = "INSERT OR REPLACE into streaming_partition (partition_id, local_edges, "
                                  "triangles, central_edges, graph_id) VALUES ("
                                  + partitionId + ", "
                                  + std::to_string(result.localRelationCount) + ", "
                                  + std::to_string(result.centralRelationCount) + ", "
                                  + std::to_string(result.result) + ", "
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
