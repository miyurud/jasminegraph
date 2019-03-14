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

#include "JasmineGraphInstanceFileTransferServiceSession.h"
#include "../util/Utils.h"
#include "JasmineGraphInstanceProtocol.h"

#include <iostream>
#include <unistd.h>

using namespace std;

int JasmineGraphInstanceFileTransferServiceSession::startFileTransferSession(int serverDataPort) {
    std::cout << "New file transfer service session started" << std::endl;
    fprintf(stderr, "New file transfer service session started\n");
    Utils utils;

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
    svrAdd.sin_port = htons(serverDataPort);

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

    while (true) {
        std::cout << "Worker listening on port " << serverDataPort << std::endl;
        int connFd = accept(listenFd, (struct sockaddr *) &clntAdd, &len);

        if (connFd < 0) {
            std::cerr << "Cannot accept connection" << std::endl;
        } else {
            std::cout << "Connection successful" << std::endl;
        }

        char data[300];
        bzero(data, 301);
        read(connFd, data, 300);
        string fileName = (data);
        fileName = utils.trim_copy(fileName, " \f\n\r\t\v");
        string filePathWithName =
                utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + fileName;

        write(connFd, JasmineGraphInstanceProtocol::SEND_FILE.c_str(), JasmineGraphInstanceProtocol::SEND_FILE.size());

        char recvBUFF[256];
        int bytesReceived = 0;
        //memset(recvBUFF, '0', sizeof(recvBUFF));

        FILE *fp;
        fp = fopen(filePathWithName.c_str(), "w");
        if (NULL == fp) {
            printf("Error opening file");
            return 1;
        }

        while((bytesReceived = read(connFd, recvBUFF, 256)) > 0)
        {
            printf("Bytes received %d\n",bytesReceived);
            fwrite(recvBUFF, 1,bytesReceived,fp);
        }
        if(bytesReceived < 0)
        {
            printf("\n Read Error \n");
        }
        close(connFd);
        std::cout << "Connection to the FTP closed" << std::endl;
        return 0;
    }
}
