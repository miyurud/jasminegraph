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

#include "../util/Conts.h"
#include "../util/Utils.h"
#include "../util/logger/Logger.h"
#include "JasmineGraphBackendProtocol.h"

using namespace std;

Logger backend_logger;

void *backendservicesesion(void *dummyPt) {
    backendservicesessionargs *sessionargs = (backendservicesessionargs *)dummyPt;
    int connFd = sessionargs->connFd;
    SQLiteDBInterface *sqLiteDbInterface = sessionargs->sqlite;
    delete sessionargs;
    backend_logger.log("Thread No: " + to_string(pthread_self()), "info");
    char data[301];
    bzero(data, 301);
    bool loop = false;
    while (!loop) {
        bzero(data, 301);
        read(connFd, data, 300);

        string line(data);
        backend_logger.log("Command received: " + line, "info");
        line = Utils::trim_copy(line);

        if (line.compare(EXIT_BACKEND) == 0) {
            write(connFd, EXIT_ACK.c_str(), EXIT_ACK.size());
            write(connFd, "\r\n", 2);
            break;
        } else if (line.compare(HANDSHAKE) == 0) {
            write(connFd, HANDSHAKE_OK.c_str(), HANDSHAKE_OK.size());
            write(connFd, "\r\n", 2);

            char host[301];
            bzero(host, 301);
            read(connFd, host, 300);
            string hostname(host);
            hostname = Utils::trim_copy(hostname);
            write(connFd, HOST_OK.c_str(), HOST_OK.size());
            backend_logger.log("Hostname of the worker: " + hostname, "info");

        } else if (line.compare(ACKNOWLEGE_MASTER) == 0) {
            int result_wr = write(connFd, WORKER_INFO_SEND.c_str(), WORKER_INFO_SEND.size());
            if (result_wr < 0) {
                backend_logger.log("Error writing to socket", "error");
            }
            result_wr = write(connFd, "\r\n", 2);
            if (result_wr < 0) {
                backend_logger.log("Error writing to socket", "error");
            }

            // We get the name and the path to graph as a pair separated by |.
            char worker_info_data[301];
            bzero(worker_info_data, 301);
            string name = "";

            read(connFd, worker_info_data, 300);

            string worker_info(worker_info_data);
            worker_info.erase(std::remove(worker_info.begin(), worker_info.end(), '\n'), worker_info.end());
            worker_info.erase(std::remove(worker_info.begin(), worker_info.end(), '\r'), worker_info.end());

            std::vector<std::string> strArr = Utils::split(worker_info, '|');

            std::string updateQuery =
                "update worker set status='started' where ip='" + strArr[0] + "' and server_port='" + strArr[1] + "';";

            sqLiteDbInterface->runUpdate(updateQuery);
            break;

        } else {
            backend_logger.error("Message format not recognized");
            sleep(1);
        }
    }
    backend_logger.log("Closing thread " + to_string(pthread_self()) + " and connection", "info");
    close(connFd);
    return NULL;
}

JasmineGraphBackend::JasmineGraphBackend(SQLiteDBInterface *db, int numberOfWorkers) {
    this->sqlite = db;
    this->workerCount = numberOfWorkers;
}

int JasmineGraphBackend::run() {
    int pId;
    int portNo = Conts::JASMINEGRAPH_BACKEND_PORT;
    int listenFd;
    socklen_t len;
    bool loop = false;
    struct sockaddr_in svrAdd;
    struct sockaddr_in clntAdd;

    // create socket
    listenFd = socket(AF_INET, SOCK_STREAM, 0);

    if (listenFd < 0) {
        backend_logger.log("Cannot open socket", "error");
        return 0;
    }

    bzero((char *)&svrAdd, sizeof(svrAdd));

    svrAdd.sin_family = AF_INET;
    svrAdd.sin_addr.s_addr = INADDR_ANY;
    svrAdd.sin_port = htons(portNo);

    int yes = 1;
    if (setsockopt(listenFd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes) == -1) {
        perror("setsockopt");
    }

    // bind socket
    if (bind(listenFd, (struct sockaddr *)&svrAdd, sizeof(svrAdd)) < 0) {
        backend_logger.log("Cannot bind on port " + portNo, "error");
        return 0;
    }

    listen(listenFd, 10);

    len = sizeof(clntAdd);

    while (true) {
        backend_logger.log("Backend Listening", "info");

        // this is where client connects. svr will hang in this mode until client conn
        int connFd = accept(listenFd, (struct sockaddr *)&clntAdd, &len);

        if (connFd < 0) {
            backend_logger.log("Cannot accept connection", "error");
            continue;
        }
        backend_logger.log("Connection successful", "info");
        backendservicesessionargs *sessionargs = new backendservicesessionargs;
        sessionargs->sqlite = this->sqlite;
        sessionargs->connFd = connFd;
        pthread_t pt;
        pthread_create(&pt, NULL, backendservicesesion, sessionargs);
        pthread_detach(pt);
    }

    return 1;
}
