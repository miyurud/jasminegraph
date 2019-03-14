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

#include "JasmineGraphInstanceServiceSession.h"
#include "JasmineGraphInstanceFileTransferServiceSession.h"
#include "../util/Utils.h"

#include <iostream>
#include <unistd.h>

using namespace std;

void JasmineGraphInstanceServiceSession::start_session(int connFd, int serverDataPort) {
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
            line = utils.trim_copy(line, " \f\n\r\t\v");
            server_hostname = line;
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
            fileName = utils.trim_copy(fileName, " \f\n\r\t\v");

            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_LEN.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_LEN.size());

            bzero(data, 301);
            read(connFd, data, 300);
            string size = (data);
            int fileSize = atoi(size.c_str());

            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_CONT.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_CONT.size());

            // TODO :: Check with Acacia code
            JasmineGraphInstanceFileTransferServiceSession *ftpSession = new JasmineGraphInstanceFileTransferServiceSession();
            std::cout << "going to start a ftp session" << std::endl;
            ftpSession->startFileTransferSession(serverDataPort);

            string fullFilePath =
                    utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + fileName;

//            while (utils.fileExists(fullFilePath) && utils.getFileSize(fullFilePath) < fileSize) {
//                bzero(data, 301);
//                read(connFd, data, 300);
//                string response = (data);
//                response = utils.trim_copy(response, " \f\n\r\t\v");
//
//                if (response.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) == 0) {
//                    write(connFd, JasmineGraphInstanceProtocol::FILE_RECV_WAIT.c_str(),
//                          JasmineGraphInstanceProtocol::FILE_RECV_WAIT.size());
//                }
//            }
//
//            bzero(data, 301);
//            read(connFd, data, 300);
//            string response = (data);
//            response = utils.trim_copy(response, " \f\n\r\t\v");
//
//            if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) == 0) {
//                write(connFd, JasmineGraphInstanceProtocol::FILE_ACK.c_str(),
//                      JasmineGraphInstanceProtocol::FILE_ACK.size());
//            }

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
