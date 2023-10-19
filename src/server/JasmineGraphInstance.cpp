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
    myThreads[0] = std::thread(logLoadAverage, "worker");

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
    Utils utils;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot accept connection" << std::endl;
        return false;
    }

    if (masterHost.find('@') != std::string::npos) {
        masterHost = utils.split(masterHost, '@')[1];
    }

    graphInstance_logger.log("###INSTANCE### Get Host By Name : " + masterHost, "info");

    server = gethostbyname(masterHost.c_str());
    if (server == NULL) {
        std::cerr << "ERROR, no host named " << server << std::endl;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(Conts::JASMINEGRAPH_BACKEND_PORT);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
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

    response = utils.trim_copy(response, " \f\n\r\t\v");

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
        response = utils.trim_copy(response, " \f\n\r\t\v");

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
            response = utils.trim_copy(response, " \f\n\r\t\v");

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
    Utils utils;
    if (enableNmon == "true") {
        std::string nmonFileLocation = utils.getJasmineGraphProperty("org.jasminegraph.server.nmon.file.location");
        std::string numberOfSnapshots = utils.getJasmineGraphProperty("org.jasminegraph.server.nmon.snapshots");
        std::string snapshotGap = utils.getJasmineGraphProperty("org.jasminegraph.server.nmon.snapshot.gap");
        std::string nmonFileName = nmonFileLocation + "nmon.log." + std::to_string(serverPort);
        std::string nmonStartupCommand =
            "nmon_x86_64_ubuntu18 -c " + numberOfSnapshots + " -s " + snapshotGap + " -T -F " + nmonFileName;

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

bool JasmineGraphInstance::isRunning() { return true; }

bool JasmineGraphInstance::sendFileThroughService(std::string host, int dataPort, std::string fileName,
                                                  std::string filePath) {
    Utils utils;
    int sockfd;
    char data[301];
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot accept connection" << std::endl;
        return false;
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        std::cerr << "ERROR, no host named " << server << std::endl;
        exit(0);
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(dataPort);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting to port " << dataPort << std::endl;
        return false;
    }

    fileName = "jasminegraph-local_trained_model_store/" + fileName;
    write(sockfd, fileName.c_str(), fileName.size());

    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);
    response = utils.trim_copy(response, " \f\n\r\t\v");
    if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE) == 0) {
        std::cout << "Sending file " << filePath << " through port " << dataPort << std::endl;

        FILE *fp = fopen(filePath.c_str(), "r");
        if (fp == NULL) {
            printf("Error opening file\n");
            close(sockfd);
            return false;
        }
        for (;;) {
            unsigned char buff[1024] = {0};
            int nread = fread(buff, 1, 1024, fp);
            // printf("Bytes read %d \n", nread);

            /* If read was success, send data. */
            if (nread > 0) {
                // printf("Sending \n");
                write(sockfd, buff, nread);
            }

            if (nread < 1024) {
                if (feof(fp))
                    // printf("End of file\n");
                    if (ferror(fp)) {
                        printf("Error reading\n");
                        return false;
                    }
                break;
            }
        }

        fclose(fp);
        close(sockfd);
        return true;
    }
    return false;
}

void JasmineGraphInstance::logLoadAverage(std::string name) {
    PerformanceUtil::logLoadAverage();

    int elapsedTime = 0;
    time_t start;
    time_t end;
    PerformanceUtil performanceUtil;
    performanceUtil.init();

    start = time(0);

    while (true) {
        if (isStatCollect) {
            std::this_thread::sleep_for(std::chrono::seconds(60));
            continue;
        }

        time_t elapsed = time(0) - start;
        if (elapsed >= Conts::LOAD_AVG_COLLECTING_GAP) {
            elapsedTime += Conts::LOAD_AVG_COLLECTING_GAP * 1000;
            PerformanceUtil::logLoadAverage();
            start = start + Conts::LOAD_AVG_COLLECTING_GAP;
        } else {
            sleep(Conts::LOAD_AVG_COLLECTING_GAP - elapsed);
        }
    }
}
