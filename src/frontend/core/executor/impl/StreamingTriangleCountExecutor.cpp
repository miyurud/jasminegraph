//
// Created by ashokkumar on 27/12/23.
//

#include "StreamingTriangleCountExecutor.h"

using namespace std::chrono;

Logger streaming_triangleCount_logger;
std::vector<std::vector<string>> StreamingTriangleCountExecutor::fileCombinations;
std::map<std::string, std::string> StreamingTriangleCountExecutor::combinationWorkerMap;
//std::map<long, std::map<long, std::vector<long>>> StreamingTriangleCountExecutor::triangleTree;
//bool isStatCollect = false;

//std::mutex fileCombinationMutex;
//std::mutex processStatusMutex;
//std::mutex responseVectorMutex;

StreamingTriangleCountExecutor::StreamingTriangleCountExecutor() {}

StreamingTriangleCountExecutor::StreamingTriangleCountExecutor(SQLiteDBInterface db, PerformanceSQLiteDBInterface perfDb,
                                             JobRequest jobRequest) {
    this->sqlite = db;
    this->perfDB = perfDb;
    this->request = jobRequest;
}

void StreamingTriangleCountExecutor::execute() {
    int uniqueId = getUid();
    std::string masterIP = request.getMasterIP();
    std::string graphId = request.getParameter(Conts::PARAM_KEYS::GRAPH_ID);

    streaming_triangleCount_logger.log(
            "###streaming-TRIANGLE-COUNT-EXECUTOR### Started with graph ID : " + graphId + " Master IP : " + masterIP, "info");

    vector<Utils::worker> workerList = Utils::getWorkerList(sqlite);
    int workerListSize = workerList.size();
    int partitionCount = 2;
    int currentPartition = 0;
    std::vector<std::future<long>> intermRes;
    long result = 0;

    for (int i = 0; i < partitionCount; i++) {
        Utils::worker currentWorker = workerList.at(i);
        string host = currentWorker.hostname;

        int workerPort = atoi(string(currentWorker.port).c_str());
        int workerDataPort = atoi(string(currentWorker.dataPort).c_str());

        intermRes.push_back(std::async(
                std::launch::async, StreamingTriangleCountExecutor::getTriangleCount, atoi(graphId.c_str()),
                host, workerPort, workerDataPort, currentPartition, masterIP));
        currentPartition++;
    }

    for (auto &&futureCall : intermRes) {
        result += futureCall.get();
    }


    long aggregatedTriangleCount = 0;
            //TriangleCountExecutor::aggregateCentralStoreTriangles(sqlite, graphId, masterIP, threadPriority);
    result += aggregatedTriangleCount;
    streaming_triangleCount_logger.log(
            "###TRIANGLE-COUNT-EXECUTOR### Getting Triangle Count : Completed: Triangles " + to_string(result), "info");

    JobResponse jobResponse;
    jobResponse.setJobId(request.getJobId());
    jobResponse.addParameter(Conts::PARAM_KEYS::STREAMING_TRIANGLE_COUNT, std::to_string(result));
    responseVector.push_back(jobResponse);

    //responseVectorMutex.lock();
    responseMap[request.getJobId()] = jobResponse;
    //responseVectorMutex.unlock();

}

//std::vector<std::vector<string>> StreamingTriangleCountExecutor::getCombinations(std::vector<string> inputVector) {
//    std::vector<std::vector<string>> combinationsList;
//    std::vector<std::vector<int>> combinations;
//
//    // Below algorithm will get all the combinations of 3 workers for given set of workers
//    std::string bitmask(3, 1);
//    bitmask.resize(inputVector.size(), 0);
//
//    do {
//        std::vector<int> combination;
//        for (int i = 0; i < inputVector.size(); ++i) {
//            if (bitmask[i]) {
//                combination.push_back(i);
//            }
//        }
//        combinations.push_back(combination);
//    } while (std::prev_permutation(bitmask.begin(), bitmask.end()));
//
//    for (std::vector<std::vector<int>>::iterator combinationsIterator = combinations.begin();
//            combinationsIterator != combinations.end(); ++combinationsIterator) {
//        std::vector<int> combination = *combinationsIterator;
//        std::vector<string> tempWorkerIdCombination;
//
//        for (std::vector<int>::iterator combinationIterator = combination.begin();
//             combinationIterator != combination.end(); ++combinationIterator) {
//            int index = *combinationIterator;
//
//            tempWorkerIdCombination.push_back(inputVector.at(index));
//        }
//
//        combinationsList.push_back(tempWorkerIdCombination);
//    }
//
//    return combinationsList;
//}

long StreamingTriangleCountExecutor::getTriangleCount(int graphId, std::string host, int port, int dataPort, int partitionId,
                                             std::string masterIP) {
    int sockfd;
    char data[301];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;
    Utils utils;
    long triangleCount;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot accept connection" << std::endl;
        return 0;
    }

    if (host.find('@') != std::string::npos) {
        host = utils.split(host, '@')[1];
    }

    streaming_triangleCount_logger.log("###TRIANGLE-COUNT-EXECUTOR### Get Host By Name : " + host, "info");

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

    bzero(data, 301);
    int result_wr =
            write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if (result_wr < 0) {
        streaming_triangleCount_logger.log("Error writing to socket", "error");
    }

    streaming_triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = utils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        streaming_triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if (result_wr < 0) {
            streaming_triangleCount_logger.log("Error writing to socket", "error");
        }

        streaming_triangleCount_logger.log("Sent : " + masterIP, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            streaming_triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        } else {
            streaming_triangleCount_logger.log("Received : " + response, "error");
        }
        result_wr = write(sockfd, JasmineGraphInstanceProtocol::INITIATE_STREAMING_TRIAN.c_str(),
                          JasmineGraphInstanceProtocol::INITIATE_STREAMING_TRIAN.size());

        if (result_wr < 0) {
            streaming_triangleCount_logger.log("Error writing to socket", "error");
        }

        streaming_triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::INITIATE_STREAMING_TRIAN, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            streaming_triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, std::to_string(graphId).c_str(), std::to_string(graphId).size());

            if (result_wr < 0) {
                streaming_triangleCount_logger.log("Error writing to socket", "error");
            }

            streaming_triangleCount_logger.log("Sent : Graph ID " + std::to_string(graphId), "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            streaming_triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, std::to_string(partitionId).c_str(), std::to_string(partitionId).size());

            if (result_wr < 0) {
                streaming_triangleCount_logger.log("Error writing to socket", "error");
            }

            streaming_triangleCount_logger.log("Sent : Partition ID " + std::to_string(partitionId), "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            streaming_triangleCount_logger.log("Got response : |" + response + "|", "info");
            response = utils.trim_copy(response, " \f\n\r\t\v");
            triangleCount = atol(response.c_str());
        }

        return triangleCount;

    } else {
        streaming_triangleCount_logger.log("There was an error in the upload process and the response is :: " + response,
                            "error");
    }
    return 0;
}


//long StreamingTriangleCountExecutor::aggregateCentralStoreTriangles(SQLiteDBInterface sqlite, std::string graphId,
//                                                           std::string masterIP, int threadPriority) {
//    std::vector<std::vector<string>> workerCombinations = getWorkerCombination(sqlite, graphId);
//    std::map<string, int> workerWeightMap;
//    std::vector<std::vector<string>>::iterator workerCombinationsIterator;
//    std::vector<std::future<string>> triangleCountResponse;
//    std::string result = "";
//    long aggregatedTriangleCount = 0;
//
//    for (workerCombinationsIterator = workerCombinations.begin();
//         workerCombinationsIterator != workerCombinations.end(); ++workerCombinationsIterator) {
//        std::vector<string> workerCombination = *workerCombinationsIterator;
//        std::map<string, int>::iterator workerWeightMapIterator;
//        std::vector<std::future<string>> remoteGraphCopyResponse;
//        int minimumWeight = 0;
//        std::string minWeightWorker;
//        string aggregatorHost = "";
//        std::string partitionIdList = "";
//
//        std::vector<string>::iterator workerCombinationIterator;
//        std::vector<string>::iterator aggregatorCopyCombinationIterator;
//
//        for (workerCombinationIterator = workerCombination.begin();
//             workerCombinationIterator != workerCombination.end(); ++workerCombinationIterator) {
//            std::string workerId = *workerCombinationIterator;
//
//            workerWeightMapIterator = workerWeightMap.find(workerId);
//
//            if (workerWeightMapIterator != workerWeightMap.end()) {
//                int weight = workerWeightMap.at(workerId);
//
//                if (minimumWeight == 0 || minimumWeight > weight) {
//                    minimumWeight = weight + 1;
//                    minWeightWorker = workerId;
//                }
//            } else {
//                minimumWeight = 1;
//                minWeightWorker = workerId;
//            }
//        }
//
//        string aggregatorSqlStatement =
//                "SELECT ip,user,server_port,server_data_port,partition_idpartition "
//                "FROM worker_has_partition INNER JOIN worker ON worker_has_partition.worker_idworker=worker.idworker "
//                "WHERE partition_graph_idgraph=" +
//                graphId + " and idworker=" + minWeightWorker + ";";
//
//        std::vector<vector<pair<string, string>>> result = sqlite.runSelect(aggregatorSqlStatement);
//
//        vector<pair<string, string>> aggregatorData = result.at(0);
//
//        std::string aggregatorIp = aggregatorData.at(0).second;
//        std::string aggregatorUser = aggregatorData.at(1).second;
//        std::string aggregatorPort = aggregatorData.at(2).second;
//        std::string aggregatorDataPort = aggregatorData.at(3).second;
//        std::string aggregatorPartitionId = aggregatorData.at(4).second;
//
//        if ((aggregatorIp.find("localhost") != std::string::npos) || aggregatorIp == masterIP) {
//            aggregatorHost = aggregatorIp;
//        } else {
//            aggregatorHost = aggregatorUser + "@" + aggregatorIp;
//        }
//
//        for (aggregatorCopyCombinationIterator = workerCombination.begin();
//             aggregatorCopyCombinationIterator != workerCombination.end(); ++aggregatorCopyCombinationIterator) {
//            std::string workerId = *aggregatorCopyCombinationIterator;
//            string host = "";
//
//            if (workerId != minWeightWorker) {
//                string sqlStatement =
//                        "SELECT ip,user,server_port,server_data_port,partition_idpartition "
//                        "FROM worker_has_partition INNER JOIN worker ON "
//                        "worker_has_partition.worker_idworker=worker.idworker "
//                        "WHERE partition_graph_idgraph=" +
//                        graphId + " and idworker=" + workerId + ";";
//
//                std::vector<vector<pair<string, string>>> result = sqlite.runSelect(sqlStatement);
//
//                vector<pair<string, string>> workerData = result.at(0);
//
//                std::string workerIp = workerData.at(0).second;
//                std::string workerUser = workerData.at(1).second;
//                std::string workerPort = workerData.at(2).second;
//                std::string workerDataPort = workerData.at(3).second;
//                std::string partitionId = workerData.at(4).second;
//
//                if ((workerIp.find("localhost") != std::string::npos) || workerIp == masterIP) {
//                    host = workerIp;
//                } else {
//                    host = workerUser + "@" + workerIp;
//                }
//
//                partitionIdList += partitionId + ",";
//
//                std::string centralStoreAvailable = isFileAccessibleToWorker(
//                        graphId, partitionId, aggregatorHost, aggregatorPort, masterIP,
//                        JasmineGraphInstanceProtocol::FILE_TYPE_CENTRALSTORE_AGGREGATE, std::string());
//
//                if (centralStoreAvailable.compare("false") == 0) {
//                    remoteGraphCopyResponse.push_back(
//                            std::async(std::launch::async, TriangleCountExecutor::copyCentralStoreToAggregator,
//                                       aggregatorHost, aggregatorPort, aggregatorDataPort, atoi(graphId.c_str()),
//                                       atoi(partitionId.c_str()), masterIP));
//                }
//            }
//        }
//
//        for (auto &&futureCallCopy : remoteGraphCopyResponse) {
//            futureCallCopy.get();
//        }
//
//        std::string adjustedPartitionIdList = partitionIdList.substr(0, partitionIdList.size() - 1);
//        workerWeightMap[minWeightWorker] = minimumWeight;
//
//        triangleCountResponse.push_back(std::async(
//                std::launch::async, TriangleCountExecutor::countCentralStoreTriangles, aggregatorHost, aggregatorPort,
//                aggregatorHost, aggregatorPartitionId, adjustedPartitionIdList, graphId, masterIP, threadPriority));
//    }
//
//    for (auto &&futureCall : triangleCountResponse) {
//        result = result + ":" + futureCall.get();
//    }
//
//    std::vector<std::string> triangles = Utils::split(result, ':');
//    std::vector<std::string>::iterator triangleIterator;
//    std::set<std::string> uniqueTriangleSet;
//
//    for (triangleIterator = triangles.begin(); triangleIterator != triangles.end(); ++triangleIterator) {
//        std::string triangle = *triangleIterator;
//
//        if (!triangle.empty() && triangle != "NILL") {
//            uniqueTriangleSet.insert(triangle);
//        }
//    }
//
//    aggregatedTriangleCount = uniqueTriangleSet.size();
//
//    return aggregatedTriangleCount;
//}
//
//
//
//std::vector<std::vector<string>> StreamingTriangleCountExecutor::getWorkerCombination(SQLiteDBInterface sqlite,
//                                                                             std::string graphId) {
//    std::set<string> workerIdSet;
//
//    string sqlStatement =
//            "SELECT worker_idworker "
//            "FROM worker_has_partition INNER JOIN worker ON worker_has_partition.worker_idworker=worker.idworker "
//            "WHERE partition_graph_idgraph=" +
//            graphId + ";"; // 2 3 0 1
//
//    std::vector<vector<pair<string, string>>> results = sqlite.runSelect(sqlStatement);
//
//    for (std::vector<vector<pair<string, string>>>::iterator i = results.begin(); i != results.end(); ++i) {
//        std::vector<pair<string, string>> rowData = *i;
//
//        string workerId = rowData.at(0).second;
//
//        workerIdSet.insert(workerId);
//    }
//
//    std::vector<string> workerIdVector(workerIdSet.begin(), workerIdSet.end());
//
//    std::vector<std::vector<string>> workerIdCombination = getCombinations(workerIdVector);
//
//    return workerIdCombination;
//}
//
//
//string StreamingTriangleCountExecutor::countCentralStoreTriangles(std::string aggregatorHostName, std::string aggregatorPort,
//                                                         std::string host, std::string partitionId,
//                                                         std::string partitionIdList, std::string graphId,
//                                                         std::string masterIP, int threadPriority) {
//    int sockfd;
//    char data[301];
//    bool loop = false;
//    socklen_t len;
//    struct sockaddr_in serv_addr;
//    struct hostent *server;
//
//    sockfd = socket(AF_INET, SOCK_STREAM, 0);
//
//    if (sockfd < 0) {
//        std::cerr << "Cannot create socket" << std::endl;
//        return 0;
//    }
//
//    if (host.find('@') != std::string::npos) {
//        host = Utils::split(host, '@')[1];
//    }
//
//    server = gethostbyname(host.c_str());
//    if (server == NULL) {
//        std::cerr << "ERROR, no host named " << server << std::endl;
//        return 0;
//    }
//
//    bzero((char *)&serv_addr, sizeof(serv_addr));
//    serv_addr.sin_family = AF_INET;
//    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
//    serv_addr.sin_port = htons(atoi(aggregatorPort.c_str()));
//    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
//        std::cerr << "ERROR connecting" << std::endl;
//        // TODO::exit
//        return 0;
//    }
//
//    bzero(data, 301);
//    int result_wr =
//            write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());
//
//    if (result_wr < 0) {
//        triangleCount_logger.log("Error writing to socket", "error");
//    }
//
//    triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
//    bzero(data, 301);
//    read(sockfd, data, 300);
//    string response = (data);
//
//    response = Utils::trim_copy(response, " \f\n\r\t\v");
//
//    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
//        triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
//        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());
//
//        if (result_wr < 0) {
//            triangleCount_logger.log("Error writing to socket", "error");
//        }
//
//        triangleCount_logger.log("Sent : " + masterIP, "info");
//        bzero(data, 301);
//        read(sockfd, data, 300);
//        response = (data);
//
//        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
//            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
//        } else {
//            triangleCount_logger.log("Received : " + response, "error");
//        }
//        result_wr = write(sockfd, JasmineGraphInstanceProtocol::AGGREGATE_CENTRALSTORE_TRIANGLES.c_str(),
//                          JasmineGraphInstanceProtocol::AGGREGATE_CENTRALSTORE_TRIANGLES.size());
//
//        if (result_wr < 0) {
//            triangleCount_logger.log("Error writing to socket", "error");
//        }
//
//        triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::AGGREGATE_CENTRALSTORE_TRIANGLES, "info");
//        bzero(data, 301);
//        read(sockfd, data, 300);
//        response = (data);
//        response = Utils::trim_copy(response, " \f\n\r\t\v");
//
//        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
//            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
//            result_wr = write(sockfd, graphId.c_str(), graphId.size());
//
//            if (result_wr < 0) {
//                triangleCount_logger.log("Error writing to socket", "error");
//            }
//
//            triangleCount_logger.log("Sent : Graph ID " + graphId, "info");
//
//            bzero(data, 301);
//            read(sockfd, data, 300);
//            response = (data);
//            response = Utils::trim_copy(response, " \f\n\r\t\v");
//        }
//
//        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
//            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
//            result_wr = write(sockfd, partitionId.c_str(), partitionId.size());
//
//            if (result_wr < 0) {
//                triangleCount_logger.log("Error writing to socket", "error");
//            }
//
//            triangleCount_logger.log("Sent : Partition ID " + partitionId, "info");
//
//            bzero(data, 301);
//            read(sockfd, data, 300);
//            response = (data);
//            response = Utils::trim_copy(response, " \f\n\r\t\v");
//        }
//
//        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
//            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
//            result_wr = write(sockfd, partitionIdList.c_str(), partitionIdList.size());
//
//            if (result_wr < 0) {
//                triangleCount_logger.log("Error writing to socket", "error");
//            }
//
//            triangleCount_logger.log("Sent : Partition ID List : " + partitionId, "info");
//
//            bzero(data, 301);
//            read(sockfd, data, 300);
//            response = (data);
//            response = Utils::trim_copy(response, " \f\n\r\t\v");
//        }
//
//        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
//            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
//            result_wr = write(sockfd, std::to_string(threadPriority).c_str(), std::to_string(threadPriority).size());
//
//            if (result_wr < 0) {
//                triangleCount_logger.log("Error writing to socket", "error");
//            }
//
//            triangleCount_logger.log("Sent : Thread Priority " + std::to_string(threadPriority), "info");
//
//            bzero(data, 301);
//            read(sockfd, data, 300);
//            response = (data);
//            response = Utils::trim_copy(response, " \f\n\r\t\v");
//            string status = response.substr(response.size() - 5);
//            std::string result = response.substr(0, response.size() - 5);
//
//            while (status == "/SEND") {
//                result_wr = write(sockfd, status.c_str(), status.size());
//
//                if (result_wr < 0) {
//                    triangleCount_logger.log("Error writing to socket", "error");
//                }
//                bzero(data, 301);
//                read(sockfd, data, 300);
//                response = (data);
//                response = Utils::trim_copy(response, " \f\n\r\t\v");
//                status = response.substr(response.size() - 5);
//                std::string triangleResponse = response.substr(0, response.size() - 5);
//                result = result + triangleResponse;
//            }
//            response = result;
//        }
//
//    } else {
//        triangleCount_logger.log("There was an error in the upload process and the response is :: " + response,
//                                 "error");
//    }
//    return response;
//}

int StreamingTriangleCountExecutor::getUid() {
    static std::atomic<std::uint32_t> uid{0};
    return ++uid;
}

