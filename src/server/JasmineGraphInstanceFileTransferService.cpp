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

#include "JasmineGraphInstanceFileTransferService.h"

#include "../server/JasmineGraphInstanceProtocol.h"
#include "../util/Utils.h"
#include "../util/logger/Logger.h"

using namespace std;
Logger file_service_logger;
pthread_mutex_t thread_lock = PTHREAD_MUTEX_INITIALIZER;

void *filetransferservicesession(void *dummyPt) {
    filetransferservicesessionargs *sessionargs = (filetransferservicesessionargs *)dummyPt;
    int connFd = sessionargs->connFd;
    delete sessionargs;
    char data[INSTANCE_DATA_LENGTH + 1];
    string fileName = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    string filePathWithName =
        Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + fileName;

    Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::SEND_FILE_LEN);
    string fsizeStr = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    int fsize = stoi(fsizeStr);
    Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::SEND_FILE);
    char buffer[4096];
    file_service_logger.info("File transfer started for file: " + fileName);
    std::ofstream file(filePathWithName, std::ios::out | std::ios::binary);
    while (fsize > 0) {
        int bytesReceived = recv(connFd, buffer, sizeof(buffer), 0);
        if (bytesReceived > 0) {
            file.write(buffer, bytesReceived);
            fsize -= bytesReceived;
        } else if (bytesReceived == 0) {
            file_service_logger.error("File transfer failed for file: " + fileName);
            break;
        }
    }
    file.close();
    if (fsize == 0) file_service_logger.info("File transfer completed for file: " + fileName);
    return NULL;
}

JasmineGraphInstanceFileTransferService::JasmineGraphInstanceFileTransferService() {}

void JasmineGraphInstanceFileTransferService::run(int dataPort) {
    int listenFd;
    socklen_t len;
    struct sockaddr_in svrAdd;
    struct sockaddr_in clntAdd;

    // create socket
    listenFd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenFd < 0) {
        std::cerr << "Cannot open socket" << std::endl;
        return;
    }

    bzero((char *)&svrAdd, sizeof(svrAdd));

    svrAdd.sin_family = AF_INET;
    svrAdd.sin_addr.s_addr = INADDR_ANY;
    svrAdd.sin_port = htons(dataPort);

    int yes = 1;
    if (setsockopt(listenFd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes) == -1) {
        perror("setsockopt");
    }

    // bind socket
    if (bind(listenFd, (struct sockaddr *)&svrAdd, sizeof(svrAdd)) < 0) {
        std::cerr << "Cannot bind on port " + dataPort << std::endl;
        return;
    }
    int connFd;
    listen(listenFd, 10);

    len = sizeof(clntAdd);

    while (true) {
        file_service_logger.log("Worker FileTransfer Service listening on port " + to_string(dataPort), "info");
        connFd = accept(listenFd, (struct sockaddr *)&clntAdd, &len);

        if (connFd < 0) {
            file_service_logger.log("Cannot accept connection to port " + to_string(dataPort), "error");
            continue;
        }
        file_service_logger.log("Connection successful to port " + to_string(dataPort), "info");
        filetransferservicesessionargs *sessionargs = new filetransferservicesessionargs;
        sessionargs->connFd = connFd;
        pthread_t pt;
        pthread_create(&pt, NULL, filetransferservicesession, sessionargs);
    }
}
