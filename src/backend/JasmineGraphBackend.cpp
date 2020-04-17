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

#include "JasmineGraphBackend.h"
#include "../util/Utils.h"
#include "../util/Conts.h"
#include "JasmineGraphBackendProtocol.h"
#include "../util/logger/Logger.h"

using namespace std;

Utils utils;
Logger backend_logger;

void *backendservicesesion(void *dummyPt) {
    backendservicesessionargs *sessionargs = (backendservicesessionargs *) dummyPt;
    int connFd = sessionargs->connFd;
    backend_logger.log("Thread No: " + to_string(pthread_self()), "info");
    char data[300];
    bzero(data, 301);
    bool loop = false;
    while (!loop) {
        bzero(data, 301);
        read(connFd, data, 300);

        string line(data);
        backend_logger.log("Command received: " + line , "info");
        line = utils.trim_copy(line, " \f\n\r\t\v");

        if (line.compare(EXIT_BACKEND) == 0) {
            write(connFd, EXIT_ACK.c_str(), EXIT_ACK.size());
            write(connFd, "\r\n", 2);
            break;
        } else if (line.compare(HANDSHAKE) == 0) {
            write(connFd, HANDSHAKE_OK.c_str(), HANDSHAKE_OK.size());
            write(connFd, "\r\n", 2);

            char host[300];
            bzero(host, 301);
            read(connFd, host, 300);
            string hostname(host);
            Utils utils;
            hostname = utils.trim_copy(hostname, " \f\n\r\t\v");
            backend_logger.log("Hostname of the worker: " + hostname , "info");

        } else if (line.compare(RECORD_PERF_STATS)) {

        }
        else {
            backend_logger.log("Message format not recognized", "error");
        }
    }
    backend_logger.log("Closing thread " + to_string(pthread_self()) + " and connection", "info");
    close(connFd);
}

JasmineGraphBackend::JasmineGraphBackend(SQLiteDBInterface db) {
    this->sqlite = db;
}

int JasmineGraphBackend::run() {
    int pId;
    int portNo = Conts::JASMINEGRAPH_BACKEND_PORT;
    int listenFd;
    socklen_t len;
    bool loop = false;
    struct sockaddr_in svrAdd;
    struct sockaddr_in clntAdd;

    //create socket
    listenFd = socket(AF_INET, SOCK_STREAM, 0);

    if (listenFd < 0) {
        backend_logger.log("Cannot open socket", "error");
        return 0;
    }

    bzero((char *) &svrAdd, sizeof(svrAdd));

    svrAdd.sin_family = AF_INET;
    svrAdd.sin_addr.s_addr = INADDR_ANY;
    svrAdd.sin_port = htons(portNo);

    int yes = 1;

    if (setsockopt(listenFd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes) == -1) {
        perror("setsockopt");
        exit(1);
    }


    //bind socket
    if (bind(listenFd, (struct sockaddr *) &svrAdd, sizeof(svrAdd)) < 0) {
        backend_logger.log("Cannot bind on port " + portNo, "error");
        return 0;
    }

    listen(listenFd, 10);
    pthread_t threadA[20];

    len = sizeof(clntAdd);

    int noThread = 0;

    while (noThread < 20) {
        backend_logger.log("Backend Listening", "info");

        //this is where client connects. svr will hang in this mode until client conn
        int connFd = accept(listenFd, (struct sockaddr *) &clntAdd, &len);

        if (connFd < 0) {
            backend_logger.log("Cannot accept connection", "error");
            return 0;
        } else {
            backend_logger.log("Connection successful", "info");
        }

        struct backendservicesessionargs backendservicesessionargs1;
        backendservicesessionargs1.sqlite = this->sqlite;
        backendservicesessionargs1.connFd = connFd;


        pthread_create(&threadA[noThread], NULL, backendservicesesion,
                       &backendservicesessionargs1);

        noThread++;
    }

    for (int i = 0; i < noThread; i++) {
        pthread_join(threadA[i], NULL);
    }
}
