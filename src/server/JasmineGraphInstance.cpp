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

#include "JasmineGraphInstance.h"

#include "../util/Utils.h"
#include "../util/logger/Logger.h"

Logger graphInstance_logger;

void *runInstanceService(void *dummyPt) {
    JasmineGraphInstance *refToInstance = (JasmineGraphInstance *)dummyPt;
    refToInstance->instanceService = new JasmineGraphInstanceService();
    refToInstance->instanceService->run(refToInstance->profile, refToInstance->masterHostName, refToInstance->hostName,
                                        refToInstance->serverPort, refToInstance->serverDataPort);
    return NULL;
}

void *runFileTransferService(void *dummyPt) {
    JasmineGraphInstance *refToInstance = (JasmineGraphInstance *)dummyPt;
    refToInstance->ftpService = new JasmineGraphInstanceFileTransferService();
    refToInstance->ftpService->run(refToInstance->serverDataPort);
    return NULL;
}

int JasmineGraphInstance::start_running(string profile, string hostName, string masterHost, int serverPort,
                                        int serverDataPort, string enableNmon) {
    graphInstance_logger.info("Worker started");

    this->hostName = hostName;
    this->profile = profile;
    this->masterHostName = masterHost;
    this->serverPort = serverPort;
    this->serverDataPort = serverDataPort;
    this->enableNmon = enableNmon;

    startNmonAnalyzer(enableNmon, serverPort);

    pthread_t instanceCommunicatorThread;
    pthread_t instanceFileTransferThread;
    pthread_create(&instanceCommunicatorThread, NULL, runInstanceService, this);
    pthread_create(&instanceFileTransferThread, NULL, runFileTransferService, this);

    std::thread *myThreads = new std::thread[1];
    myThreads[0] = std::thread(StatisticCollector::logLoadAverage, "worker");

    pthread_join(instanceCommunicatorThread, NULL);
    pthread_join(instanceFileTransferThread, NULL);
    return 0;
}

bool JasmineGraphInstance::acknowledgeMaster(string masterHost, string workerIP, string workerPort) {
    int sockfd;
    char data[301];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot create socket" << std::endl;
        return false;
    }

    if (masterHost.find('@') != std::string::npos) {
        masterHost = Utils::split(masterHost, '@')[1];
    }

    graphInstance_logger.log("###INSTANCE### Get Host By Name : " + masterHost, "info");

    server = gethostbyname(masterHost.c_str());
    if (server == NULL) {
        graphInstance_logger.error("ERROR, no host named " + masterHost);
        return false;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(Conts::JASMINEGRAPH_BACKEND_PORT);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
        return false;
    }

    bzero(data, 301);
    int result_wr =
        write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if (result_wr < 0) {
        graphInstance_logger.log("Error writing to socket", "error");
    }

    graphInstance_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = Utils::trim_copy(response);

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        graphInstance_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");

        result_wr = write(sockfd, workerIP.c_str(), workerIP.size());

        if (result_wr < 0) {
            graphInstance_logger.log("Error writing to socket", "error");
        }

        graphInstance_logger.log("Sent : " + workerIP, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = Utils::trim_copy(response);

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            graphInstance_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");

            result_wr = write(sockfd, JasmineGraphInstanceProtocol::ACKNOWLEDGE_MASTER.c_str(),
                              JasmineGraphInstanceProtocol::ACKNOWLEDGE_MASTER.size());

            if (result_wr < 0) {
                graphInstance_logger.log("Error writing to socket", "error");
            }

            graphInstance_logger.log("Sent : " + JasmineGraphInstanceProtocol::ACKNOWLEDGE_MASTER, "info");
            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = Utils::trim_copy(response);

            if (response.compare(JasmineGraphInstanceProtocol::WORKER_INFO_SEND) == 0) {
                std::string workerInfo = workerIP + "|" + workerPort;
                result_wr = write(sockfd, workerInfo.c_str(), workerInfo.size());

                if (result_wr < 0) {
                    graphInstance_logger.log("Error writing to socket", "error");
                }

                graphInstance_logger.log("Sent : " + workerInfo, "info");
                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);

                if (response.compare(JasmineGraphInstanceProtocol::UPDATE_DONE) == 0) {
                    return true;
                }
            }

            if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
                return true;
            }
        }

        return false;
    }
    return false;
}

void JasmineGraphInstance::startNmonAnalyzer(string enableNmon, int serverPort) {
    if (enableNmon == "true") {
        std::string nmonFileLocation = Utils::getJasmineGraphProperty("org.jasminegraph.server.nmon.file.location");
        std::string numberOfSnapshots = Utils::getJasmineGraphProperty("org.jasminegraph.server.nmon.snapshots");
        std::string snapshotGap = Utils::getJasmineGraphProperty("org.jasminegraph.server.nmon.snapshot.gap");
        std::string nmonFileName = nmonFileLocation + "nmon.log." + std::to_string(serverPort);
        /*std::string nmonStartupCommand =
            "nmon_x86_64_ubuntu18 -c " + numberOfSnapshots + " -s " + snapshotGap + " -T -F " + nmonFileName;*/
        std::string nmonStartupCommand =
            "nmon -c " + numberOfSnapshots + " -s " + snapshotGap + " -T -F " + nmonFileName;


        char buffer[BUFFER_SIZE];
        std::string result = "";

        FILE *input = popen(nmonStartupCommand.c_str(), "r");

        if (input) {
            // read the input
            while (!feof(input)) {
                if (fgets(buffer, BUFFER_SIZE, input) != NULL) {
                    result.append(buffer);
                }
            }
            if (!result.empty()) {
                graphInstance_logger.log("Error in performance database backup process", "error");
            }

            pclose(input);
        }
    }
}
