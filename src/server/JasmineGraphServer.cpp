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

#include <iostream>
#include <map>
#include "JasmineGraphServer.h"
#include "JasmineGraphInstance.h"
#include "../util/Utils.h"
#include "../partitioner/local/MetisPartitioner.h"

#include "JasmineGraphInstanceProtocol.h"
#include "../util/logger/Logger.h"

Logger server_logger;

static map<string, string> hostIDMap;
static std::vector<JasmineGraphServer::workers> hostWorkerMap;
static map<string, pair<int, int>> hostPortMap;

void *runfrontend(void *dummyPt) {
    JasmineGraphServer *refToServer = (JasmineGraphServer *) dummyPt;
    refToServer->frontend = new JasmineGraphFrontEnd(refToServer->sqlite, refToServer->masterHost);
    refToServer->frontend->run();
}

void *runbackend(void *dummyPt) {
    JasmineGraphServer *refToServer = (JasmineGraphServer *) dummyPt;
    refToServer->backend = new JasmineGraphBackend(refToServer->sqlite);
    refToServer->backend->run();
}


JasmineGraphServer::JasmineGraphServer() {

}

JasmineGraphServer::~JasmineGraphServer() {
    puts("Freeing up server resources.");
    sqlite.finalize();
}

int JasmineGraphServer::run(std::string profile, std::string masterIp, int numberofWorkers, std::string workerIps) {
    server_logger.log("Running the server...", "info");
    Utils utils;

    this->sqlite = *new SQLiteDBInterface();
    this->sqlite.init();
    if (masterIp.empty()) {
        this->masterHost = utils.getJasmineGraphProperty("org.jasminegraph.server.host");
    } else {
        this->masterHost = masterIp;
    }
    this->profile = profile;
    this->numberOfWorkers = numberofWorkers;
    this->workerHosts = workerIps;
    init();
    addHostsToMetaDB();
    updateOperationalGraphList();
    hostIDMap = getLiveHostIDList();
    start_workers();
    return 0;
}

bool JasmineGraphServer::isRunning() {
    return true;
}

void JasmineGraphServer::init() {
    Utils utils;

    pthread_t frontendthread;
    pthread_t backendthread;
    pthread_create(&frontendthread, NULL, runfrontend, this);
    pthread_create(&backendthread, NULL, runbackend, this);
}

void JasmineGraphServer::start_workers() {
    Utils utils;
    int hostListModeNWorkers = 0;
    int numberOfWorkersPerHost;
    std::vector<std::string> hostsList;
    std::string nWorkers;
    if (profile == "native") {
        hostsList = utils.getHostList();
        nWorkers = utils.getJasmineGraphProperty("org.jasminegraph.server.nworkers");
    } else if (profile == "docker") {
        hostsList = getWorkerVector(workerHosts);
    }

    int workerPort = Conts::JASMINEGRAPH_INSTANCE_PORT;
    int workerDataPort = Conts::JASMINEGRAPH_INSTANCE_DATA_PORT;
    if (utils.is_number(nWorkers)) {
        numberOfWorkers = atoi(nWorkers.c_str());
    } else if (numberOfWorkers == 0) {
        server_logger.log("Number of Workers is not specified", "error");
    }

    if (numberOfWorkers > 0 && hostsList.size() > 0) {
        numberOfWorkersPerHost = numberOfWorkers / hostsList.size();
        hostListModeNWorkers = numberOfWorkers % hostsList.size();
    }

    std::vector<std::string>::iterator it;
    it = hostsList.begin();

    for (it = hostsList.begin(); it < hostsList.end(); it++) {
        std::string item = *it;
        int portCount = 0;
        std::vector<int> portVector = workerPortsMap[item];
        std::vector<int> dataPortVector = workerDataPortsMap[item];

        while (portCount < numberOfWorkersPerHost) {
            portVector.push_back(workerPort);
            dataPortVector.push_back(workerDataPort);
            hostWorkerMap.push_back({*it, workerPort, workerDataPort});
            hostPortMap.insert((pair<string, pair<int, int>>(*it, make_pair(workerPort, workerDataPort))));
            workerPort = workerPort + 2;
            workerDataPort = workerDataPort + 2;
            portCount++;
        }

        if (hostListModeNWorkers > 0) {
            portVector.push_back(workerPort);
            dataPortVector.push_back(workerDataPort);
            hostWorkerMap.push_back({*it, workerPort, workerDataPort});
            hostPortMap.insert(((pair<string, pair<int, int>>(*it, make_pair(workerPort, workerDataPort)))));
            workerPort = workerPort + 2;
            workerDataPort = workerDataPort + 2;
            hostListModeNWorkers--;
        }

        workerPortsMap[item] = portVector;
        workerDataPortsMap[item] = dataPortVector;

    }

    int hostListSize = hostsList.size();
    std::vector<std::string>::iterator hostListIterator;
    hostListIterator = hostsList.begin();


    std::thread *myThreads = new std::thread[hostListSize];
    int count = 0;
    server_logger.log("Starting threads for workers", "info");
    for (hostListIterator = hostsList.begin(); hostListIterator < hostsList.end(); hostListIterator++) {
        std::string host = *hostListIterator;
        myThreads[count] = std::thread(startRemoteWorkers,workerPortsMap[host],workerDataPortsMap[host], host, profile, masterHost);
//        myThreads[count].detach();
//        std::cout<<"############JOINED###########"<< std::endl;
        count++;
    }

    for (int threadCount = 0; threadCount < hostListSize; threadCount++) {
        myThreads[threadCount].join();
        std::cout << "############JOINED###########" << std::endl;
    }

}


void JasmineGraphServer::startRemoteWorkers(std::vector<int> workerPortsVector,
                                                    std::vector<int> workerDataPortsVector,string host, string profile, string masterHost) {
    Utils utils;
    std::string executableFile;
    std::string workerPath = utils.getJasmineGraphProperty("org.jasminegraph.worker.path");
    std::string artifactPath = utils.getJasmineGraphProperty("org.jasminegraph.artifact.path");
    std::string jasmineGraphExecutableName = Conts::JASMINEGRAPH_EXECUTABLE;
    if (hasEnding(workerPath, "/")) {
        executableFile = workerPath + jasmineGraphExecutableName;
    } else {
        executableFile = workerPath + "/" + jasmineGraphExecutableName;
    }
    std::string serverStartScript;
    char buffer[128];
    std::string result = "";

    if (artifactPath.empty() || artifactPath.find_first_not_of(' ') == artifactPath.npos) {
        artifactPath = utils.getJasmineGraphHome();
    }

    if (profile == "native") {
        copyArtifactsToWorkers(workerPath,artifactPath,host);
        for (int i =0 ; i < workerPortsVector.size() ; i++) {
            if (host.find("localhost") != std::string::npos) {
                serverStartScript = executableFile+" 2 "+ host +" " + std::to_string(workerPortsVector.at(i)) + " " + std::to_string(workerDataPortsVector.at(i));
            } else {
                serverStartScript =
                        "ssh -p 22 " + host + " " + executableFile + " 2 "+host+" " + std::to_string(workerPortsVector.at(i)) +
                        " " + std::to_string(workerDataPortsVector.at(i));
            }
            popen(serverStartScript.c_str(),"r");
        }
    } else if (profile == "docker") {
        for (int i =0 ; i < workerPortsVector.size() ; i++) {
            if (host.find("localhost") != std::string::npos) {
                serverStartScript = executableFile+" 2 "+ host +" " + std::to_string(workerPortsVector.at(i)) + " " + std::to_string(workerDataPortsVector.at(i));
            } else {
                serverStartScript =
                        "ssh -p 22 " + host + " " + executableFile + " 2 "+host+" " + std::to_string(workerPortsVector.at(i)) +
                        " " + std::to_string(workerDataPortsVector.at(i));
            }
            serverStartScript = "docker -H ssh://root@" + host + " run jasmine_graph:latest -p " +
                                std::to_string(workerPortsVector.at(i)) + ":" +
                                std::to_string(workerPortsVector.at(i)) + " -p " +
                                std::to_string(workerDataPortsVector.at(i)) + ":" +
                                std::to_string(workerDataPortsVector.at(i)) + " --MODE 2 --HOST_NAME " + host +
                                " --MASTERIP " + masterHost + " --SERVER_PORT " +
                                std::to_string(workerPortsVector.at(i)) + " --SERVER_DATA_PORT " +
                                std::to_string(workerDataPortsVector.at(i));
            server_logger.log(serverStartScript, "info");
            popen(serverStartScript.c_str(),"r");
        }
    }


}

int JasmineGraphServer::shutdown_workers() {
    std::cout << "Shutting the workers down" << std::endl;
    std::vector<workers, std::allocator<workers>>::iterator mapIterator;
    for (mapIterator = hostWorkerMap.begin(); mapIterator < hostWorkerMap.end(); mapIterator++) {
        workers worker = *mapIterator;
        Utils utils;
        bool result = true;
        std::cout << pthread_self() << " host : " << worker.hostname << " port : " << worker.port << " DPort : "
                  << worker.dataPort << std::endl;
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

        std::string host = worker.hostname;
        int port = worker.port;

        if (worker.hostname.find('@') != std::string::npos) {
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
        write(sockfd, JasmineGraphInstanceProtocol::SHUTDOWN.c_str(), JasmineGraphInstanceProtocol::SHUTDOWN.size());
        server_logger.log("Sent : " + JasmineGraphInstanceProtocol::SHUTDOWN, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        string response = (data);

        response = utils.trim_copy(response, " \f\n\r\t\v");
        server_logger.log("Response : " + response, "info");
    }
}

void JasmineGraphServer::uploadGraphLocally(int graphID, const string graphType, vector<std::map<int,string>> fullFileList, std::string masterIP) {
    std::cout << "Uploading the graph locally.." << std::endl;
    std::map<int, string> partitionFileList = fullFileList[0];
    std::map<int, string> centralStoreFileList = fullFileList[1];
    std::map<int, string> centralStoreDuplFileList = fullFileList[2];
    std::map<int, string> attributeFileList;
    std::map<int, string> centralStoreAttributeFileList;
    Utils utils;
    if (masterHost.empty()) {
        masterHost = utils.getJasmineGraphProperty("org.jasminegraph.server.host");;
    }
    int total_threads = partitionFileList.size() + centralStoreFileList.size() + centralStoreDuplFileList.size();
    if (graphType == Conts::GRAPH_WITH_ATTRIBUTES) {
        attributeFileList = fullFileList[3];
        total_threads += attributeFileList.size();
        centralStoreAttributeFileList = fullFileList[4];
        total_threads += centralStoreAttributeFileList.size();
    }
    int count = 0;
    int file_count = 0;
    std::thread *workerThreads = new std::thread[total_threads];
    while (count < total_threads) {
        std::vector<workers, std::allocator<workers>>::iterator mapIterator;
        for (mapIterator = hostWorkerMap.begin(); mapIterator < hostWorkerMap.end(); mapIterator++) {
            workers worker = *mapIterator;
            if (count == total_threads) {
                break;
            }
            workerThreads[count] = std::thread(batchUploadFile, worker.hostname, worker.port, worker.dataPort, graphID,
                                               partitionFileList[file_count], masterHost);
            count++;
            sleep(1);
            workerThreads[count] = std::thread(batchUploadCentralStore, worker.hostname, worker.port, worker.dataPort,
                                               graphID, centralStoreFileList[file_count], masterHost);
            count++;
            sleep(1);
            workerThreads[count] = std::thread(batchUploadCentralStore, worker.hostname, worker.port, worker.dataPort,
                                               graphID, centralStoreDuplFileList[file_count], masterHost);
            count++;
            sleep(1);
            if (graphType == Conts::GRAPH_WITH_ATTRIBUTES) {
                workerThreads[count] = std::thread(batchUploadAttributeFile, worker.hostname, worker.port,
                                                   worker.dataPort, graphID, attributeFileList[file_count], masterHost);
                count++;
                sleep(1);
                workerThreads[count] = std::thread(batchUploadCentralAttributeFile, worker.hostname, worker.port,
                                                   worker.dataPort, graphID, centralStoreAttributeFileList[file_count], masterHost);
                count++;
                sleep(1);
            }
            file_count++;
        }
    }

    for (int threadCount = 0; threadCount < count; threadCount++) {
        workerThreads[threadCount].join();
        std::cout << "Thread " << threadCount << " joined" << std::endl;
    }

    std::time_t time = chrono::system_clock::to_time_t(chrono::system_clock::now());
    string uploadEndTime = ctime(&time);

    //The following function updates the 'host_has_partition' table and 'graph' table only
    updateMetaDB(hostWorkerMap, partitionFileList, graphID, uploadEndTime);

}

bool JasmineGraphServer::batchUploadFile(std::string host, int port, int dataPort, int graphID, std::string filePath, std::string masterIP) {
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
    server_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = utils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        server_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        write(sockfd, masterIP.c_str(), masterIP.size());
        server_logger.log("Sent : " + masterIP, "info");


        write(sockfd, JasmineGraphInstanceProtocol::BATCH_UPLOAD.c_str(),
              JasmineGraphInstanceProtocol::BATCH_UPLOAD.size());
        server_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");
        //std::cout << response << std::endl;
        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            //std::cout << graphID << std::endl;
            write(sockfd, std::to_string(graphID).c_str(), std::to_string(graphID).size());
            server_logger.log("Sent : Graph ID " + std::to_string(graphID), "info");
            std::string fileName = utils.getFileName(filePath);
            int fileSize = utils.getFileSize(filePath);
            std::string fileLength = to_string(fileSize);

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");

            if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_NAME) == 0) {
                server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME, "info");
                //std::cout << fileName << std::endl;
                write(sockfd, fileName.c_str(), fileName.size());
                server_logger.log("Sent : File name " + fileName, "info");
                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);
                //response = utils.trim_copy(response, " \f\n\r\t\v");

                if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_LEN) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");
                    write(sockfd, fileLength.c_str(), fileLength.size());
                    server_logger.log("Sent : File length in bytes " + fileLength, "info");
                    bzero(data, 301);
                    read(sockfd, data, 300);
                    response = (data);
                    //response = utils.trim_copy(response, " \f\n\r\t\v");

                    if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_CONT) == 0) {
                        server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_CONT, "info");
                        server_logger.log("Going to send file through service", "info");
                        sendFileThroughService(host, dataPort, fileName, filePath, masterIP);
                    }
                }
            }
            int count = 0;

            while (true) {
                write(sockfd, JasmineGraphInstanceProtocol::FILE_RECV_CHK.c_str(),
                      JasmineGraphInstanceProtocol::FILE_RECV_CHK.size());
                server_logger.log("Sent : " + JasmineGraphInstanceProtocol::FILE_RECV_CHK, "info");
                server_logger.log("Checking if file is received", "info");
                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);
                if (response.compare(JasmineGraphInstanceProtocol::FILE_RECV_WAIT) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_RECV_WAIT, "info");
                    server_logger.log("Checking file status : " + to_string(count), "info");
                    count++;
                    sleep(1);
                    continue;
                } else if (response.compare(JasmineGraphInstanceProtocol::FILE_ACK) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_ACK, "info");
                    server_logger.log("File transfer completed", "info");
                    break;
                }
            };
            //Next we wait till the batch upload completes
            while (true) {
                write(sockfd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.c_str(),
                      JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.size());
                server_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);

                if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT, "info");
                    sleep(1);
                    continue;
                } else if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK, "info");
                    server_logger.log("Batch upload completed", "info");
                    break;
                }
            }
        }
    } else {
        server_logger.log("There was an error in the upload process and the response is :: " + response, "error");
    }

    close(sockfd);
    return 0;
}

bool JasmineGraphServer::batchUploadCentralStore(std::string host, int port, int dataPort, int graphID,
                                                 std::string filePath, std::string masterIP) {
    Utils utils;
    //std::cout << pthread_self() << " host : " << host << " port : " << port << " DPort : " << dataPort << std::endl;
    int sockfd;
    char data[300];
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
    }

    bzero(data, 301);
    write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());
    server_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = utils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        server_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        write(sockfd, masterIP.c_str(), masterIP.size());
        server_logger.log("Sent : " + masterIP, "info");

        write(sockfd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_CENTRAL.c_str(),
              JasmineGraphInstanceProtocol::BATCH_UPLOAD_CENTRAL.size());
        server_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CENTRAL, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");
        //std::cout << response << std::endl;
        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            //std::cout << graphID << std::endl;
            write(sockfd, std::to_string(graphID).c_str(), std::to_string(graphID).size());
            server_logger.log("Sent : Graph ID " + std::to_string(graphID), "info");
            std::string fileName = utils.getFileName(filePath);
            int fileSize = utils.getFileSize(filePath);
            std::string fileLength = to_string(fileSize);

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");

            if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_NAME) == 0) {
                server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME, "info");
                //std::cout << fileName << std::endl;
                write(sockfd, fileName.c_str(), fileName.size());
                server_logger.log("Sent : File name " + fileName, "info");
                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);
                //response = utils.trim_copy(response, " \f\n\r\t\v");

                if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_LEN) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");
                    write(sockfd, fileLength.c_str(), fileLength.size());
                    server_logger.log("Sent : File length in bytes " + fileLength, "info");
                    bzero(data, 301);
                    read(sockfd, data, 300);
                    response = (data);
                    //response = utils.trim_copy(response, " \f\n\r\t\v");

                    if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_CONT) == 0) {
                        server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_CONT, "info");
                        server_logger.log("Going to send file through service", "info");
                        sendFileThroughService(host, dataPort, fileName, filePath, masterIP);
                    }
                }
            }
            int count = 0;

            while (true) {
                write(sockfd, JasmineGraphInstanceProtocol::FILE_RECV_CHK.c_str(),
                      JasmineGraphInstanceProtocol::FILE_RECV_CHK.size());
                server_logger.log("Sent : " + JasmineGraphInstanceProtocol::FILE_RECV_CHK, "info");
                server_logger.log("Checking if file is received", "info");
                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);
                //response = utils.trim_copy(response, " \f\n\r\t\v");

                if (response.compare(JasmineGraphInstanceProtocol::FILE_RECV_WAIT) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_RECV_WAIT, "info");
                    server_logger.log("Checking file status : " + to_string(count), "info");
                    count++;
                    sleep(1);
                    continue;
                } else if (response.compare(JasmineGraphInstanceProtocol::FILE_ACK) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_ACK, "info");
                    server_logger.log("File transfer completed", "info");
                    break;
                }
            }

            //Next we wait till the batch upload completes
            while (true) {
                write(sockfd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.c_str(),
                      JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.size());
                server_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);

                if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT, "info");
                    sleep(1);
                    continue;
                } else if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK, "info");
                    server_logger.log("CentralStore partition file upload completed", "info");
                    break;
                }
            }
        }
    } else {
        server_logger.log("There was an error in the upload process and the response is :: " + response, "error");
    }

    close(sockfd);
    return 0;
}

bool JasmineGraphServer::batchUploadAttributeFile(std::string host, int port, int dataPort, int graphID,
                                                 std::string filePath, std::string masterIP) {
    Utils utils;
    int sockfd;
    char data[300];
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
    }

    bzero(data, 301);
    write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());
    server_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = utils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        server_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        write(sockfd, masterIP.c_str(), masterIP.size());
        server_logger.log("Sent : " + masterIP, "info");

        write(sockfd, JasmineGraphInstanceProtocol::UPLOAD_RDF_ATTRIBUTES.c_str(),
              JasmineGraphInstanceProtocol::UPLOAD_RDF_ATTRIBUTES.size());
        server_logger.log("Sent : " + JasmineGraphInstanceProtocol::UPLOAD_RDF_ATTRIBUTES, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");
        //std::cout << response << std::endl;
        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            //std::cout << graphID << std::endl;
            write(sockfd, std::to_string(graphID).c_str(), std::to_string(graphID).size());
            server_logger.log("Sent : Graph ID " + std::to_string(graphID), "info");
            std::string fileName = utils.getFileName(filePath);
            int fileSize = utils.getFileSize(filePath);
            std::string fileLength = to_string(fileSize);

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");

            if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_NAME) == 0) {
                server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME, "info");
                //std::cout << fileName << std::endl;
                write(sockfd, fileName.c_str(), fileName.size());
                server_logger.log("Sent : File name " + fileName, "info");
                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);
                //response = utils.trim_copy(response, " \f\n\r\t\v");

                if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_LEN) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");
                    write(sockfd, fileLength.c_str(), fileLength.size());
                    server_logger.log("Sent : File length in bytes " + fileLength, "info");
                    bzero(data, 301);
                    read(sockfd, data, 300);
                    response = (data);
                    //response = utils.trim_copy(response, " \f\n\r\t\v");

                    if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_CONT) == 0) {
                        server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_CONT, "info");
                        server_logger.log("Going to send file through service", "info");
                        sendFileThroughService(host, dataPort, fileName, filePath, masterIP);
                    }
                }
            }
            int count = 0;

            while (true) {
                write(sockfd, JasmineGraphInstanceProtocol::FILE_RECV_CHK.c_str(),
                      JasmineGraphInstanceProtocol::FILE_RECV_CHK.size());
                server_logger.log("Sent : " + JasmineGraphInstanceProtocol::FILE_RECV_CHK, "info");
                server_logger.log("Checking if file is received", "info");
                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);
                //response = utils.trim_copy(response, " \f\n\r\t\v");

                if (response.compare(JasmineGraphInstanceProtocol::FILE_RECV_WAIT) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_RECV_WAIT, "info");
                    server_logger.log("Checking file status : " + to_string(count), "info");
                    count++;
                    sleep(1);
                    continue;
                } else if (response.compare(JasmineGraphInstanceProtocol::FILE_ACK) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_ACK, "info");
                    server_logger.log("File transfer completed", "info");
                    break;
                }
            }

            //Next we wait till the batch upload completes
            while (true) {
                write(sockfd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.c_str(),
                      JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.size());
                server_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);

                if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT, "info");
                    sleep(1);
                    continue;
                } else if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK, "info");
                    server_logger.log("Attribute file upload completed", "info");
                    break;
                }
            }
        }
    } else {
        server_logger.log("There was an error in the upload process and the response is :: " + response, "error");
    }

    close(sockfd);
    return 0;
}

bool JasmineGraphServer::batchUploadCentralAttributeFile(std::string host, int port, int dataPort, int graphID,
                                                  std::string filePath, std::string masterIP) {
    Utils utils;
    int sockfd;
    char data[300];
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
    }

    bzero(data, 301);
    write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());
    server_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = utils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        server_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        write(sockfd, masterIP.c_str(), masterIP.size());
        server_logger.log("Sent : " + masterIP, "info");

        write(sockfd, JasmineGraphInstanceProtocol::UPLOAD_RDF_ATTRIBUTES_CENTRAL.c_str(),
              JasmineGraphInstanceProtocol::UPLOAD_RDF_ATTRIBUTES_CENTRAL.size());
        server_logger.log("Sent : " + JasmineGraphInstanceProtocol::UPLOAD_RDF_ATTRIBUTES_CENTRAL, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");
        //std::cout << response << std::endl;
        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            //std::cout << graphID << std::endl;
            write(sockfd, std::to_string(graphID).c_str(), std::to_string(graphID).size());
            server_logger.log("Sent : Graph ID " + std::to_string(graphID), "info");
            std::string fileName = utils.getFileName(filePath);
            int fileSize = utils.getFileSize(filePath);
            std::string fileLength = to_string(fileSize);

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");

            if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_NAME) == 0) {
                server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME, "info");
                //std::cout << fileName << std::endl;
                write(sockfd, fileName.c_str(), fileName.size());
                server_logger.log("Sent : File name " + fileName, "info");
                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);
                //response = utils.trim_copy(response, " \f\n\r\t\v");

                if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_LEN) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");
                    write(sockfd, fileLength.c_str(), fileLength.size());
                    server_logger.log("Sent : File length in bytes " + fileLength, "info");
                    bzero(data, 301);
                    read(sockfd, data, 300);
                    response = (data);
                    //response = utils.trim_copy(response, " \f\n\r\t\v");

                    if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_CONT) == 0) {
                        server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_CONT, "info");
                        server_logger.log("Going to send file through service", "info");
                        sendFileThroughService(host, dataPort, fileName, filePath, masterIP);
                    }
                }
            }
            int count = 0;

            while (true) {
                write(sockfd, JasmineGraphInstanceProtocol::FILE_RECV_CHK.c_str(),
                      JasmineGraphInstanceProtocol::FILE_RECV_CHK.size());
                server_logger.log("Sent : " + JasmineGraphInstanceProtocol::FILE_RECV_CHK, "info");
                server_logger.log("Checking if file is received", "info");
                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);
                //response = utils.trim_copy(response, " \f\n\r\t\v");

                if (response.compare(JasmineGraphInstanceProtocol::FILE_RECV_WAIT) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_RECV_WAIT, "info");
                    server_logger.log("Checking file status : " + to_string(count), "info");
                    count++;
                    sleep(1);
                    continue;
                } else if (response.compare(JasmineGraphInstanceProtocol::FILE_ACK) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_ACK, "info");
                    server_logger.log("File transfer completed", "info");
                    break;
                }
            }

            //Next we wait till the batch upload completes
            while (true) {
                write(sockfd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.c_str(),
                      JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.size());
                server_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);

                if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT, "info");
                    sleep(1);
                    continue;
                } else if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK, "info");
                    server_logger.log("Attribute file upload completed", "info");
                    break;
                }
            }
        }
    } else {
        server_logger.log("There was an error in the upload process and the response is :: " + response, "error");
    }

    close(sockfd);
    return 0;
}

bool JasmineGraphServer::sendFileThroughService(std::string host, int dataPort, std::string fileName,
                                                std::string filePath, std::string masterIP) {
    Utils utils;
    int sockfd;
    char data[300];
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot accept connection" << std::endl;
        return 0;
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        std::cerr << "ERROR, no host named " << server << std::endl;
        exit(0);
    }

    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *) server->h_addr,
          (char *) &serv_addr.sin_addr.s_addr,
          server->h_length);
    serv_addr.sin_port = htons(dataPort);
    if (connect(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting to port " << dataPort << std::endl;
    }

    write(sockfd, fileName.c_str(), fileName.size());

    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);
    response = utils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE) == 0) {
        std::cout << "Sending file " << filePath << " through port " << dataPort << std::endl;

        FILE *fp = fopen(filePath.c_str(), "r");
        if (fp == NULL) {
            //printf("Error opening file\n");
            close(sockfd);
            return 0;
        }

        for (;;) {
            unsigned char buff[1024] = {0};
            int nread = fread(buff, 1, 1024, fp);
            //printf("Bytes read %d \n", nread);

            /* If read was success, send data. */
            if (nread > 0) {
                //printf("Sending \n");
                write(sockfd, buff, nread);
            }

            if (nread < 1024) {
                if (feof(fp))
                    printf("End of file\n");
                if (ferror(fp))
                    printf("Error reading\n");
                break;
            }
        }

        fclose(fp);
        close(sockfd);
    }
}

void JasmineGraphServer::copyArtifactsToWorkers(std::string workerPath, std::string artifactLocation,
                                                std::string remoteWorker) {
    if (artifactLocation.empty() || artifactLocation.find_first_not_of(' ') == artifactLocation.npos) {
        server_logger.log("Received `" + artifactLocation + "` for `artifactLocation` value!", "error");
        throw std::invalid_argument("Received empty string for `artifactLocation` value!");
    }
    std::string pathCheckCommand = "test -e " + workerPath + "&& echo file exists || echo file not found";
    std::string artifactCopyCommand;
    std::string localWorkerArtifactCopyCommand = "cp -r " + artifactLocation + "/* " + workerPath;
    std::string remoteWorkerArtifactCopyCommand =
            "scp -r " + artifactLocation + "/* " + remoteWorker + ":" + workerPath;

    char buffer[128];
    std::string result = "";

    if (remoteWorker.find("localhost") == std::string::npos) {
        std::string remotePathCheckCommand = "ssh -p 22 " + remoteWorker + " " + pathCheckCommand;
        pathCheckCommand = remotePathCheckCommand;
    }

    FILE *input = popen(pathCheckCommand.c_str(), "r");

    if (input) {
        // read the input
        while (!feof(input)) {
            if (fgets(buffer, 128, input) != NULL) {
                result.append(buffer);
            }
        }
        if (!result.empty() && remoteWorker.find("file not found") == std::string::npos) {
            createWorkerPath(remoteWorker, workerPath);
        }
        pclose(input);
    }

    if (remoteWorker.find("localhost") != std::string::npos) {
        artifactCopyCommand = localWorkerArtifactCopyCommand;
    } else {
        artifactCopyCommand = remoteWorkerArtifactCopyCommand;
    }

    FILE *copyInput = popen(artifactCopyCommand.c_str(), "r");

    if (copyInput) {
        // read the input
        while (!feof(copyInput)) {
            if (fgets(buffer, 128, copyInput) != NULL) {
                result.append(buffer);
            }
        }
        if (!result.empty()) {
            std::cout << result << std::endl;
        }
        pclose(copyInput);
    }
}

void JasmineGraphServer::createWorkerPath(std::string workerHost, std::string workerPath) {
    std::string pathCreationCommand = "mkdir -p " + workerPath;
    char buffer[128];
    std::string result = "";

    if (workerHost.find("localhost") == std::string::npos) {
        std::string tmpPathCreation = pathCreationCommand;
        pathCreationCommand = "ssh -p 22 " + workerHost + " " + tmpPathCreation;
    }

    FILE *input = popen(pathCreationCommand.c_str(), "r");

    if (input) {
        // read the input
        while (!feof(input)) {
            if (fgets(buffer, 128, input) != NULL) {
                result.append(buffer);
            }
        }
        if (!result.empty()) {
            std::cout << result << std::endl;
        }
        pclose(input);
    }
}

void JasmineGraphServer::addHostsToMetaDB() {
    Utils utils;
    vector<string> hostsList = utils.getHostList();
    vector<string>::iterator it;
    string sqlStatement = "";
    for (it = hostsList.begin(); it < hostsList.end(); it++) {
        string host = *it;
        string name = host;
        string ip_address = "";
        if (host.find('@') != std::string::npos) {
            vector<string> splitted = utils.split(host, '@');
            ip_address = splitted[1];
        }
        if (!utils.hostExists(name, ip_address, this->sqlite)) {
            sqlStatement = ("INSERT INTO host (name,ip,is_public) VALUES(\"" + name + "\", \"" + ip_address +
                            "\", \"\")");
            this->sqlite.runInsert(sqlStatement);
        }
    }
}

map<string, string> JasmineGraphServer::getLiveHostIDList() {
    Utils utils;
    map<string, string> hostIDMap;
    vector<string> hostsList = utils.getHostList();
    vector<string>::iterator it;
    for (it = hostsList.begin(); it < hostsList.end(); it++) {
        string host = *it;
        std::vector<vector<pair<string, string>>> v = this->sqlite.runSelect(
                "SELECT idhost FROM host WHERE name = '" + host + "';");
        string id = v[0][0].second;
        hostIDMap.insert(make_pair(host, id));
    }
    return hostIDMap;
}

void JasmineGraphServer::updateMetaDB(vector<JasmineGraphServer::workers> hostWorkerMap,
                                      std::map<int, string> partitionFileList, int graphID, string uploadEndTime) {
    SQLiteDBInterface refToSqlite = *new SQLiteDBInterface();
    refToSqlite.init();
    int file_count = 0;
    std::vector<workers, std::allocator<workers>>::iterator mapIterator;
    while (file_count < partitionFileList.size()) {
        for (mapIterator = hostWorkerMap.begin(); mapIterator < hostWorkerMap.end(); mapIterator++) {
            workers worker = *mapIterator;
            size_t lastindex = partitionFileList[file_count].find_last_of('.');
            string rawname = partitionFileList[file_count].substr(0, lastindex);
            string partitionID = rawname.substr(rawname.find_last_of('_') + 1);
            string sqlStatement =
                    "INSERT INTO host_has_partition (host_idhost, partition_idpartition, partition_graph_idgraph) "
                    "VALUES('" + hostIDMap.find(worker.hostname)->second + "','" + partitionID + "','" +
                    to_string(graphID) + "')";

            refToSqlite.runInsertNoIDReturn(sqlStatement);
            file_count++;
        }
    }
    string sqlStatement2 =
            "UPDATE graph SET upload_end_time = '" + uploadEndTime + "' ,graph_status_idgraph_status = '" +
            to_string(Conts::GRAPH_STATUS::OPERATIONAL) + "' WHERE idgraph = '" + to_string(graphID) + "'";
    refToSqlite.runUpdate(sqlStatement2);
}

void JasmineGraphServer::removeGraph(vector<pair<string, string>> hostHasPartition, string graphID, std::string masterIP) {
    std::cout << "Deleting the graph partitions.." << std::endl;
    int count = 0;
    std::thread *deleteThreads = new std::thread[hostHasPartition.size()];
    for (std::vector<pair<string, string>>::iterator j = (hostHasPartition.begin()); j != hostHasPartition.end(); ++j) {
        deleteThreads[count] = std::thread(removePartitionThroughService, j->first, hostPortMap[j->first].first, graphID, j->second, masterIP);
        count++;
        sleep(1);
    }

    for (int threadCount = 0; threadCount < count; threadCount++) {
        deleteThreads[threadCount].join();
        std::cout << "Thread " << threadCount << " joined" << std::endl;
    }
}

int JasmineGraphServer::removePartitionThroughService(string host, int port, string graphID, string partitionID, string masterIP) {
    Utils utils;
    std::cout << pthread_self() << " host : " << host << " port : " << port << std::endl;
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
    server_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = utils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        server_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        write(sockfd, masterIP.c_str(), masterIP.size());
        server_logger.log("Sent : " + masterIP, "info");


        write(sockfd, JasmineGraphInstanceProtocol::DELETE_GRAPH.c_str(),
              JasmineGraphInstanceProtocol::DELETE_GRAPH.size());
        server_logger.log("Sent : " + JasmineGraphInstanceProtocol::DELETE_GRAPH, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");
        //std::cout << response << std::endl;
        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            write(sockfd, graphID.c_str(), graphID.size());
            server_logger.log("Sent : Graph ID " + graphID, "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");

            if (response.compare(JasmineGraphInstanceProtocol::SEND_PARTITION_ID) == 0) {
                server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_PARTITION_ID, "info");
                write(sockfd, partitionID.c_str(), partitionID.size());
                server_logger.log("Sent : Partition ID " + partitionID, "info");

                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);
                response = utils.trim_copy(response, " \f\n\r\t\v");
                server_logger.log("Received last response : " + response, "info");
                return 1;
            } else {
                close(sockfd);
                return 0;
            }
        } else {
            close(sockfd);
            return 0;
        }
    }
    close(sockfd);
    return 0;
}

std::vector<JasmineGraphServer::workers> JasmineGraphServer::getHostWorkerMap() {
    return hostWorkerMap;
}

void JasmineGraphServer::updateOperationalGraphList() {
    Utils utils;
    string hosts = "";
    string graphIDs = "";
    vector<string> hostsList = utils.getHostList();
    vector<string>::iterator it;
    for (it = hostsList.begin(); it < hostsList.end(); it++) {
        string host = *it;
        hosts += ("'" + host + "', ");
    }
    hosts = hosts.substr(0, hosts.size() - 2);
    string sqlStatement = ("SELECT b.partition_graph_idgraph FROM host_has_partition AS b "
                           "JOIN host WHERE host.idhost = b.host_idhost AND host.name IN "
                           "(" + hosts + ") GROUP BY b.partition_graph_idgraph HAVING COUNT(b.partition_idpartition)= "
                                         "(SELECT COUNT(a.partition_idpartition) FROM host_has_partition AS a "
                                         "WHERE a.partition_graph_idgraph = b.partition_graph_idgraph);");
    std::vector<vector<pair<string, string>>> v = this->sqlite.runSelect(sqlStatement);
    for (std::vector<vector<pair<string, string>>>::iterator i = v.begin(); i != v.end(); ++i) {
        for (std::vector<pair<string, string>>::iterator j = (i->begin()); j != i->end(); ++j) {
            graphIDs += (j->second + ", ");
        }
    }
    graphIDs = graphIDs.substr(0, graphIDs.size() - 2);
    string sqlStatement2 = "UPDATE graph SET graph_status_idgraph_status = ("
                           "CASE WHEN idgraph IN (" + graphIDs + ") THEN '" +
                           to_string(Conts::GRAPH_STATUS::OPERATIONAL) + "' ELSE '" +
                           to_string(Conts::GRAPH_STATUS::NONOPERATIONAL) + "' END )";
    this->sqlite.runUpdate(sqlStatement2);
}

std::map<string, JasmineGraphServer::workerPartitions> JasmineGraphServer::getGraphPartitionedHosts(string graphID) {

    vector<pair<string, string>> hostHasPartition;
    SQLiteDBInterface refToSqlite = *new SQLiteDBInterface();
    refToSqlite.init();
    vector<vector<pair<string, string>>> hostPartitionResults = refToSqlite.runSelect(
            "SELECT name, partition_idpartition FROM host_has_partition INNER JOIN host ON host_idhost = idhost WHERE "
            "partition_graph_idgraph = '" + graphID + "'");
    for (vector<vector<pair<string, string>>>::iterator i = hostPartitionResults.begin();
         i != hostPartitionResults.end(); ++i) {
        int count = 0;
        string hostname;
        string partitionID;
        for (std::vector<pair<string, string>>::iterator j = (i->begin()); j != i->end(); ++j) {
            if (count == 0) {
                hostname = j->second;
            } else {
                partitionID = j->second;
                hostHasPartition.push_back(pair<string, string>(hostname, partitionID));
            }
            count++;
        }
    }

    map<string, vector<string>> hostPartitions;
    for (std::vector<pair<string, string>>::iterator j = (hostHasPartition.begin()); j != hostHasPartition.end(); ++j) {
        cout << "HOST ID : " << j->first << " Partition ID : " << j->second << endl;
        string hostname = j->first;
        if (hostPartitions.count(hostname) > 0) {
            hostPartitions[hostname].push_back(j->second);
        } else {
            vector<string> vec;
            vec.push_back(j->second);
            hostPartitions.insert((pair<string, vector<string>>(hostname, vec)));
        }

    }

    map<string, JasmineGraphServer::workerPartitions> graphPartitionedHosts;
    for (map<string, vector<string>>::iterator it = (hostPartitions.begin()); it != hostPartitions.end(); ++it) {
        graphPartitionedHosts.insert((pair<string, JasmineGraphServer::workerPartitions>(it->first,
                                                                                         {hostPortMap[it->first].first,
                                                                                          hostPortMap[it->first].second,
                                                                                          hostPartitions[it->first]})));
    }

    return graphPartitionedHosts;
}

bool JasmineGraphServer::hasEnding(std::string const &fullString, std::string const &ending) {
    if (fullString.length() >= ending.length()) {
        return (0 == fullString.compare(fullString.length() - ending.length(), ending.length(), ending));
    } else {
        return false;
    }
}

std::vector<std::string> JasmineGraphServer::getWorkerVector(std::string workerList) {
    Utils utils;
    std::vector<std::string> workerVector = utils.split(workerList,',');
    return workerVector;
}
