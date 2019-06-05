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
#include "../util/logger/Logger.h"
#include "../util/Utils.h"
#include "../server/JasmineGraphInstanceProtocol.h"

Logger predictor_logger;

int JasminGraphLinkPredictor::initiateLinkPrediction(std::string graphID, std::string path) {
    std::cout << "in initiate predictor" << endl;
    JasmineGraphServer *jasmineServer = new JasmineGraphServer();
    std::map<std::string, JasmineGraphServer::workerPartitions> graphPartitionedHosts = jasmineServer->getGraphPartitionedHosts(
            graphID);

    std::map<std::string, JasmineGraphServer::workerPartitions> remainHostMap;
    std::string selectedHostName;
    int selectedHostPort;
    int selectedHostDataPort;
    std::vector<std::string> selectedHostPartitions;
    int count = 0;

    /*Select the idle worker*/
//    TODO :: Need to select the idle worker to allocate predicting task.
//     For this time the first worker of the map is allocated
    cout << "selecting one host" << endl;
    for (std::map<std::string, JasmineGraphServer::workerPartitions>::iterator it = (graphPartitionedHosts.begin());
         it != graphPartitionedHosts.end(); ++it) {
        if (count == 0) {
            selectedHostName = it->first;
            selectedHostPort = (graphPartitionedHosts[it->first]).port;
            selectedHostDataPort = (graphPartitionedHosts[it->first]).dataPort;
            selectedHostPartitions = (graphPartitionedHosts[it->first]).partitionID;
//            remainHostMap.insert(std::pair<std::string, JasmineGraphServer::workerPartitions>(it->first, it->second));
        } else {
            remainHostMap.insert(std::pair<std::string, JasmineGraphServer::workerPartitions>(it->first, it->second));
        }
        count++;
    }
    std::string hostsList = "none|";
    for (std::map<std::string, JasmineGraphServer::workerPartitions>::iterator it = (remainHostMap.begin());
         it != remainHostMap.end(); ++it) {
        std::string hostDetail =
                it->first + "," + std::to_string((remainHostMap[it->first]).port) + "," +
                std::to_string((remainHostMap[it->first]).dataPort) + ",";
        for (std::vector<std::string>::iterator j = ((remainHostMap[it->first].partitionID).begin());
             j != (remainHostMap[it->first].partitionID).end(); ++j) {

            if (std::next(j) == (remainHostMap[it->first].partitionID).end()) {
                hostDetail = hostDetail + *j;
            } else {
                hostDetail = hostDetail + *j + ",";
            }
        }
//        std::cout << hostDetail << endl;
        if (std::next(it) == remainHostMap.end()) {
            hostsList += hostDetail;
        } else {
            hostsList += hostDetail + "|";
        }
    }
//    std::cout << hostsList << endl;
//    cout << "sending details" << endl;

    this->sendQueryToWorker(selectedHostName, selectedHostPort, selectedHostDataPort, graphID, path, hostsList);
}

int JasminGraphLinkPredictor::sendQueryToWorker(std::string host, int port, int dataPort, std::string graphID,
                                                std::string filePath, std::string hostsList) {
    Utils utils;
    bool result = true;
    std::cout << pthread_self() << " host : " << host << " port : " << port << " DPort : " << dataPort << std::endl;
    int sockfd;
    char data[300];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot accept connection" << std::endl;
        return 0;
    }

    if (host.find('@') != std::string::npos) {
        host = utils.split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        std::cerr << "ERROR, no host named " << server << std::endl;
    }

    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *) server->h_addr,
          (char *) &serv_addr.sin_addr.s_addr,
          server->h_length);
    serv_addr.sin_port = htons(port);
    if (connect(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
        //TODO::exit
    }
    bzero(data, 301);
    write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());
    predictor_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = utils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        predictor_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        string server_host = utils.getJasmineGraphProperty("org.jasminegraph.server.host");
        write(sockfd, server_host.c_str(), server_host.size());
        predictor_logger.log("Sent : " + server_host, "info");

        write(sockfd, JasmineGraphInstanceProtocol::INITIATE_PREDICT.c_str(),
              JasmineGraphInstanceProtocol::INITIATE_PREDICT.size());
        predictor_logger.log("Sent : " + JasmineGraphInstanceProtocol::INITIATE_PREDICT, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            predictor_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            write(sockfd, graphID.c_str(), (graphID).size());
            predictor_logger.log("Sent : Graph ID " + graphID, "info");

            std::string fileName = utils.getFileName(filePath);
            int fileSize = utils.getFileSize(filePath);
            std::string fileLength = to_string(fileSize);

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");
            if (response.compare(JasmineGraphInstanceProtocol::SEND_HOSTS) == 0) {
                predictor_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_HOSTS, "info");
                /*Create a atring with host details*/

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
                            JasmineGraphServer::sendFileThroughService(host, dataPort, fileName, filePath);

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
                        predictor_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_RECV_WAIT,
                                             "info");
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
