/**
Copyright 2019 JasmineGraph Team
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

#include "JasminGraphLinkPredictor.h"

#include "../../../server/JasmineGraphInstanceProtocol.h"
#include "../../../util/logger/Logger.h"

Logger predictor_logger;

void JasminGraphLinkPredictor::initiateLinkPrediction(std::string graphID, std::string path, std::string masterIP) {
    JasmineGraphServer *jasmineServer = JasmineGraphServer::getInstance();
    std::unordered_map<std::string, JasmineGraphServer::workerPartitions> graphPartitionedHosts =
        jasmineServer->getGraphPartitionedHosts(graphID);

    std::unordered_map<std::string, JasmineGraphServer::workerPartitions> remainHostMap;
    std::string selectedHostName;
    int selectedHostPort;
    int selectedHostDataPort;
    std::vector<std::string> selectedHostPartitions;
    int selectedHostPartitionsNo;
    int count = 0;

    /*Select the idle worker*/
    //    TODO :: Need to select the idle worker to allocate predicting task.
    //     For this time the first worker of the map is allocated

    for (std::unordered_map<std::string, JasmineGraphServer::workerPartitions>::iterator it =
             (graphPartitionedHosts.begin());
         it != graphPartitionedHosts.end(); ++it) {
        if (count == 0) {
            selectedHostName = it->first;
            selectedHostPort = (graphPartitionedHosts[it->first]).port;
            selectedHostDataPort = (graphPartitionedHosts[it->first]).dataPort;
            selectedHostPartitions = (graphPartitionedHosts[it->first]).partitionID;
            selectedHostPartitionsNo = selectedHostPartitions.size();
        } else {
            remainHostMap[it->first] = it->second;
        }
        count++;
    }
    std::string hostsList = "none|";
    for (std::unordered_map<std::string, JasmineGraphServer::workerPartitions>::iterator it = (remainHostMap.begin());
         it != remainHostMap.end(); ++it) {
        std::string hostDetail = it->first + "," + std::to_string((remainHostMap[it->first]).port) + "," +
                                 std::to_string((remainHostMap[it->first]).dataPort) + ",";
        for (std::vector<std::string>::iterator j = ((remainHostMap[it->first].partitionID).begin());
             j != (remainHostMap[it->first].partitionID).end(); ++j) {
            if (std::next(j) == (remainHostMap[it->first].partitionID).end()) {
                hostDetail = hostDetail + *j;
            } else {
                hostDetail = hostDetail + *j + ",";
            }
        }
        if (std::next(it) == remainHostMap.end()) {
            hostsList += hostDetail;
        } else {
            hostsList += hostDetail + "|";
        }
    }
    std::string vertexCount;
    auto *refToSqlite = new SQLiteDBInterface();
    refToSqlite->init();
    string sqlStatement =
        "SELECT vertexcount FROM graph WHERE "
        "idgraph = " +
        graphID;
    std::vector<vector<pair<string, string>>> v = refToSqlite->runSelect(sqlStatement);
    vertexCount = (v[0][0].second);

    JasminGraphLinkPredictor::sendQueryToWorker(selectedHostName, selectedHostPort, selectedHostDataPort,
                                                selectedHostPartitionsNo, graphID, vertexCount, path, hostsList,
                                                masterIP);
    refToSqlite->finalize();
    delete refToSqlite;
}

int JasminGraphLinkPredictor::sendQueryToWorker(std::string host, int port, int dataPort, int selectedHostPartitionsNo,
                                                std::string graphID, std::string vertexCount, std::string filePath,
                                                std::string hostsList, std::string masterIP) {
    bool result = true;
    std::cout << pthread_self() << " host : " << host << " port : " << port << " DPort : " << dataPort << std::endl;
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

    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        predictor_logger.error("ERROR, no host named " + host);
        return 0;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
        // TODO::exit
        return 0;
    }
    bzero(data, 301);
    write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());
    predictor_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = Utils::trim_copy(response);

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        predictor_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        string server_host = Utils::getJasmineGraphProperty("org.jasminegraph.server.host");
        write(sockfd, server_host.c_str(), server_host.size());
        predictor_logger.log("Sent : " + server_host, "info");

        write(sockfd, JasmineGraphInstanceProtocol::INITIATE_PREDICT.c_str(),
              JasmineGraphInstanceProtocol::INITIATE_PREDICT.size());
        predictor_logger.log("Sent : " + JasmineGraphInstanceProtocol::INITIATE_PREDICT, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = Utils::trim_copy(response);

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            predictor_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            write(sockfd, graphID.c_str(), (graphID).size());
            predictor_logger.log("Sent : Graph ID " + graphID, "info");

            write(sockfd, vertexCount.c_str(), (vertexCount).size());
            predictor_logger.log("Sent : Vertex Count " + vertexCount, "info");

            write(sockfd, to_string(selectedHostPartitionsNo).c_str(), to_string(selectedHostPartitionsNo).size());
            predictor_logger.log("Sent : No of partition in selected host " + to_string(selectedHostPartitionsNo),
                                 "info");

            std::string fileName = Utils::getFileName(filePath);
            int fileSize = Utils::getFileSize(filePath);
            std::string fileLength = to_string(fileSize);

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = Utils::trim_copy(response);
            if (response.compare(JasmineGraphInstanceProtocol::SEND_HOSTS) == 0) {
                predictor_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_HOSTS, "info");
                /*Create a string with host details*/

                write(sockfd, hostsList.c_str(), (hostsList).size());
                predictor_logger.log("Sent : Hosts List " + hostsList, "info");
                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);

                if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_NAME) == 0) {
                    predictor_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME, "info");
                    write(sockfd, fileName.c_str(), fileName.size());
                    predictor_logger.log("Sent : File name " + fileName, "info");
                    bzero(data, 301);
                    read(sockfd, data, 300);
                    response = (data);

                    if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_LEN) == 0) {
                        predictor_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");
                        write(sockfd, fileLength.c_str(), fileLength.size());
                        predictor_logger.log("Sent : File length in bytes " + fileLength, "info");
                        bzero(data, 301);
                        read(sockfd, data, 300);
                        response = (data);

                        if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_CONT) == 0) {
                            predictor_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_CONT, "info");
                            predictor_logger.log("Going to send file through service", "info");
                            Utils::sendFileThroughService(host, dataPort, fileName, filePath);
                        }
                    }
                }
                int count = 0;
                while (true) {
                    write(sockfd, JasmineGraphInstanceProtocol::FILE_RECV_CHK.c_str(),
                          JasmineGraphInstanceProtocol::FILE_RECV_CHK.size());
                    predictor_logger.log("Sent : " + JasmineGraphInstanceProtocol::FILE_RECV_CHK, "info");
                    predictor_logger.log("Checking if file is received", "info");
                    bzero(data, 301);
                    read(sockfd, data, 300);
                    response = (data);

                    if (response.compare(JasmineGraphInstanceProtocol::FILE_RECV_WAIT) == 0) {
                        predictor_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_RECV_WAIT, "info");
                        predictor_logger.log("Checking file status : " + to_string(count), "info");
                        count++;
                        sleep(1);
                        continue;
                    } else if (response.compare(JasmineGraphInstanceProtocol::FILE_ACK) == 0) {
                        predictor_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_ACK, "info");
                        predictor_logger.log("File transfer completed", "info");
                        break;
                    }
                }
            }
        }
    } else {
        predictor_logger.log("There was an error in the :: " + response, "error");
    }
    close(sockfd);
    return 0;
}
