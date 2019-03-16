/**
Copyright 2018 JasminGraph Team
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

#include "JasmineGraphInstanceService.h"
#include "../util/Utils.h"

using namespace std;

void *instanceservicesession(void *dummyPt) {
    instanceservicesessionargs *sessionargs = (instanceservicesessionargs *) dummyPt;
    int connFd = sessionargs->connFd;

    std::cout << "New service session started" << std::endl;
    Utils utils;

    utils.createDirectory(utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder"));

    char data[300];
    bool loop = false;
    while (!loop) {
        bzero(data, 301);
        read(connFd, data, 300);

        string line(data);

        Utils utils;
        line = utils.trim_copy(line, " \f\n\r\t\v");

        if (line.compare(JasmineGraphInstanceProtocol::HANDSHAKE) == 0) {
            write(connFd, JasmineGraphInstanceProtocol::HANDSHAKE_OK.c_str(),
                  JasmineGraphInstanceProtocol::HANDSHAKE_OK.size());

            bzero(data, 301);
            read(connFd, data, 300);
            line = (data);
            //line = utils.trim_copy(line, " \f\n\r\t\v");
            string server_hostname = line;
            std::cout << "ServerName : " << server_hostname << std::endl;

        } else if (line.compare(JasmineGraphInstanceProtocol::CLOSE)==0) {
            write(connFd, JasmineGraphInstanceProtocol::CLOSE_ACK.c_str(),
                  JasmineGraphInstanceProtocol::CLOSE_ACK.size());
            close(connFd);
        } else if (line.compare(JasmineGraphInstanceProtocol::SHUTDOWN)==0) {
            write(connFd, JasmineGraphInstanceProtocol::SHUTDOWN_ACK.c_str(),
                  JasmineGraphInstanceProtocol::SHUTDOWN_ACK.size());
            close(connFd);
            break;
        } else if (line.compare(JasmineGraphInstanceProtocol::READY)==0) {
            write(connFd, JasmineGraphInstanceProtocol::OK.c_str(), JasmineGraphInstanceProtocol::OK.size());
        }

            // TODO :: INSERT_EDGES,TRUNCATE,COUNT_VERTICES,COUNT_EDGES,DELETE,LOADPG etc should be implemented

        else if (line.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD)==0) {
            write(connFd, JasmineGraphInstanceProtocol::OK.c_str(), JasmineGraphInstanceProtocol::OK.size());
            bzero(data, 301);
            read(connFd, data, 300);
            string graphID = (data);
            graphID = utils.trim_copy(graphID, " \f\n\r\t\v");

            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_NAME.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_NAME.size());

            bzero(data, 301);
            read(connFd, data, 300);
            string fileName = (data);
            //fileName = utils.trim_copy(fileName, " \f\n\r\t\v");

            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_LEN.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_LEN.size());

            bzero(data, 301);
            read(connFd, data, 300);
            string size = (data);
            int fileSize = atoi(size.c_str());

            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_CONT.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_CONT.size());

            // TODO :: Check with Acacia code

            string fullFilePath =
                    utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + fileName;
            while (utils.fileExists(fullFilePath) && utils.getFileSize(fullFilePath) < fileSize) {
                bzero(data, 301);
                read(connFd, data, 300);
                string response = (data);
                std::cout << "is the file received?? :: " <<  utils.getFileSize(fullFilePath) << std::endl;
                //response = utils.trim_copy(response, " \f\n\r\t\v");

                if (response.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) == 0) {
                    write(connFd, JasmineGraphInstanceProtocol::FILE_RECV_WAIT.c_str(),
                          JasmineGraphInstanceProtocol::FILE_RECV_WAIT.size());
                }
            }

            bzero(data, 301);
            read(connFd, data, 300);
            string response = (data);
            //response = utils.trim_copy(response, " \f\n\r\t\v");

            if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) == 0) {
                write(connFd, JasmineGraphInstanceProtocol::FILE_ACK.c_str(),
                      JasmineGraphInstanceProtocol::FILE_ACK.size());
            }

            std::cout << "File Received" << std::endl;
            loop = true;

            // TODO :: Check with Acacia

            //utils.unzipFile(fullFilePath);

            //TODO:: Check with Acacia

//            while (!utils.fileExists(fullFilePath)) {
//                bzero(data, 301);
//                read(connFd, data, 300);
//                string response = (data);
//                response = utils.trim_copy(response, " \f\n\r\t\v");
//                if (line.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK) == 0) {
//                    write(connFd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT.c_str(),
//                          JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT.size());
//                }
//            }
//
//            write(connFd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK.c_str(),
//                  JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK.size());

        }
        // TODO :: Implement the rest of the protocol
        //else if ()
    }

    cout << "\nClosing thread " << pthread_self() << " and connection" << endl;
    close(connFd);
}

JasmineGraphInstanceService::JasmineGraphInstanceService() {
}

int JasmineGraphInstanceService::run(int serverPort) {

    int listenFd;
    socklen_t len;
    struct sockaddr_in svrAdd;
    struct sockaddr_in clntAdd;

    //create socket
    listenFd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenFd < 0) {
        std::cerr << "Cannot open socket" << std::endl;
        return 0;
    }

    bzero((char *) &svrAdd, sizeof(svrAdd));

    svrAdd.sin_family = AF_INET;
    svrAdd.sin_addr.s_addr = INADDR_ANY;
    svrAdd.sin_port = htons(serverPort);

    int yes = 1;

    if (setsockopt(listenFd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes) == -1) {
        perror("setsockopt");
        exit(1);
    }


    //bind socket
    if (bind(listenFd, (struct sockaddr *) &svrAdd, sizeof(svrAdd)) < 0) {
        std::cerr << "Cannot bind" << std::endl;
        return 0;
    }

    listen(listenFd, 5);

    len = sizeof(clntAdd);

    int connectionCounter = 0;
    pthread_t threadA[5];

    // TODO :: What is the maximum number of connections allowed??
    while (connectionCounter<5) {
        std::cout << "Worker listening on port " << serverPort << std::endl;
        int connFd = accept(listenFd, (struct sockaddr *) &clntAdd, &len);

        if (connFd < 0) {
            std::cerr << "Cannot accept connection" << std::endl;
        } else {
            std::cout << "Connection successful" << std::endl;
            struct instanceservicesessionargs instanceservicesessionargs1;
            instanceservicesessionargs1.connFd = connFd;

            pthread_create(&threadA[connectionCounter], NULL, instanceservicesession,
                           &instanceservicesessionargs1);
            //pthread_detach(threadA[connectionCounter]);
            //pthread_join(threadA[connectionCounter], NULL);
            connectionCounter++;
        }
    }

    for (int i = 0; i < connectionCounter; i++) {
        pthread_join(threadA[i], NULL);
        std::cout << "service Threads joined" << std::endl;
    }
}
