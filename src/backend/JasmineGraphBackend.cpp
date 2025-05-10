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
    backend_logger.info("Thread No: " + to_string(pthread_self()));
    char data[BACKEND_DATA_LENGTH + 1];
    bool loop = false;
    while (!loop) {
        string line = Utils::read_str_trim_wrapper(connFd, data, BACKEND_DATA_LENGTH);
        backend_logger.info("Command received: " + line);

        if (line.compare(EXIT_BACKEND) == 0) {
            write(connFd, EXIT_ACK.c_str(), EXIT_ACK.size());
            write(connFd, "\r\n", 2);
            break;
        } else if (line.compare(HANDSHAKE) == 0) {
            write(connFd, HANDSHAKE_OK.c_str(), HANDSHAKE_OK.size());
            write(connFd, "\r\n", 2);

            string hostname = Utils::read_str_trim_wrapper(connFd, data, BACKEND_DATA_LENGTH);
            write(connFd, HOST_OK.c_str(), HOST_OK.size());
            backend_logger.info("Hostname of the worker: " + hostname);

        } else if (line.compare(ACKNOWLEGE_MASTER) == 0) {
            int result_wr = write(connFd, WORKER_INFO_SEND.c_str(), WORKER_INFO_SEND.size());
            if (result_wr < 0) {
                backend_logger.error("Error writing to socket");
            }
            result_wr = write(connFd, "\r\n", 2);
            if (result_wr < 0) {
                backend_logger.error("Error writing to socket");
            }

            // We get the name and the path to graph as a pair separated by |.
            string worker_info = Utils::read_str_trim_wrapper(connFd, data, BACKEND_DATA_LENGTH);
            worker_info.erase(std::remove(worker_info.begin(), worker_info.end(), '\n'), worker_info.end());
            worker_info.erase(std::remove(worker_info.begin(), worker_info.end(), '\r'), worker_info.end());

            std::vector<std::string> strArr = Utils::split(worker_info, '|');

            std::string updateQuery =
                "update worker set status='started' where ip='" + strArr[0] + "' and server_port='" + strArr[1] + "';";

            sqLiteDbInterface->runUpdate(updateQuery);
            break;

        } else if (line.compare(WORKER_DETAILS) == 0) {
            if (!Utils::send_str_wrapper(connFd, WORKER_DETAILS_ACK)) {
                loop = true;
                break;
            }

            int content_length = 0;
            backend_logger.info("Waiting for content length");
            int return_status = recv(connFd, &content_length, sizeof(int), 0);
            if (return_status > 0) {
                content_length = ntohl(content_length);
                backend_logger.info("Received content_length for partition ID = " + std::to_string(content_length));
            } else {
                backend_logger.info("Error while reading content length");
                loop = true;
                break;
            }


            if (!Utils::send_str_wrapper(connFd, CONTENT_LENGTH_ACK)) {
                loop = true;
                break;
            }


            std::string partition(content_length, 0);
            return_status = recv(connFd, &partition[0], content_length, 0);
            if (return_status > 0) {
                backend_logger.info("Received partition id: " + partition);
            } else {
                backend_logger.info("Error while reading content length");
                loop = true;
                break;
            }

            std::string selectQuery = "select server_port, ip, server_data_port from worker where idworker = '" +
                    partition + "';";

            if (!sqLiteDbInterface) {
                backend_logger.error("Database interface is null!");
                break;
            }

            auto worker = sqLiteDbInterface->runSelect(selectQuery);

            if (worker.empty() || worker[0].empty()) {
                backend_logger.error("Query returned no results.");
                break;
            }

            std::string workerInfo = worker[0][1].second + "|" + worker[0][0].second + "|" + worker[0][2].second;
            int message_length = workerInfo.length();
            int converted_number = htonl(message_length);
            backend_logger.info("Sending worker info length: " + to_string(converted_number));
            if (!Utils::send_int_wrapper(connFd, &converted_number, sizeof(converted_number))) {
                loop = true;
                break;
            }

            std::string length_ack(CONTENT_LENGTH_ACK.length(), 0);
            return_status = recv(connFd, &length_ack[0], CONTENT_LENGTH_ACK.length(), 0);
            if (return_status > 0) {
                backend_logger.info("Received content length ack: " + length_ack);
            } else {
                backend_logger.info("Error while reading content length ack");
                loop = true;
                break;
            }

            if (!Utils::send_str_wrapper(connFd, workerInfo)) {
                loop = true;
                break;
            }

        } else if (line.compare(PARTITION_ALGORITHM_DETAILS) == 0) {
            if (!Utils::send_str_wrapper(connFd, PARTITION_ALGORITHM_DETAILS_ACK)) {
                loop = true;
                break;
            }

            int content_length = 0;
            backend_logger.info("Waiting for content length");
            int return_status = recv(connFd, &content_length, sizeof(int), 0);
            if (return_status > 0) {
                content_length = ntohl(content_length);
                backend_logger.info("Received content_length of graph ID = " + std::to_string(content_length));
            } else {
                backend_logger.info("Error while reading content length");
                loop = true;
                break;
            }

            if (!Utils::send_str_wrapper(connFd, CONTENT_LENGTH_ACK)) {
                loop = true;
                break;
            }


            std::string graphID(content_length, 0);
            return_status = recv(connFd, &graphID[0], content_length, 0);
            if (return_status > 0) {
                backend_logger.info("Received graph id: " + graphID);
            } else {
                backend_logger.info("Error while reading content length");
                loop = true;
                break;
            }

            std::string selectQuery = "select id_algorithm from graph where idgraph='" + graphID + "';";

            if (!sqLiteDbInterface) {
                backend_logger.error("Database interface is null!");
                break;
            }

            auto partitionAlgorithm = sqLiteDbInterface->runSelect(selectQuery);

            if (partitionAlgorithm.empty() || partitionAlgorithm[0].empty()) {
                backend_logger.error("Query returned no results.");
                break;
            }

            std::string partitionAlgorithmName = partitionAlgorithm[0][0].second;
            int message_length = partitionAlgorithmName.length();
            int converted_number = htonl(message_length);
            backend_logger.info("Sending worker info length: " + to_string(converted_number));
            if (!Utils::send_int_wrapper(connFd, &converted_number, sizeof(converted_number))) {
                loop = true;
                break;
            }

            std::string length_ack(CONTENT_LENGTH_ACK.length(), 0);
            return_status = recv(connFd, &length_ack[0], CONTENT_LENGTH_ACK.length(), 0);
            if (return_status > 0) {
                backend_logger.info("Received content length ack: " + length_ack);
            } else {
                backend_logger.info("Error while reading content length ack");
                loop = true;
                break;
            }

            if (!Utils::send_str_wrapper(connFd, partitionAlgorithmName)) {
                loop = true;
                break;
            }
        } else if (line.compare(DIRECTION_DETAIL) == 0) {
            if (!Utils::send_str_wrapper(connFd, DIRECTION_DETAIL_ACK)) {
                loop = true;
                break;
            }

            int content_length = 0;
            backend_logger.info("Waiting for content length");
            int return_status = recv(connFd, &content_length, sizeof(int), 0);
            if (return_status > 0) {
                content_length = ntohl(content_length);
                backend_logger.info("Received content_length of graph ID = " + std::to_string(content_length));
            } else {
                backend_logger.info("Error while reading content length");
                loop = true;
                break;
            }

            if (!Utils::send_str_wrapper(connFd, CONTENT_LENGTH_ACK)) {
                loop = true;
                break;
            }


            std::string graphID(content_length, 0);
            return_status = recv(connFd, &graphID[0], content_length, 0);
            if (return_status > 0) {
                backend_logger.info("Received graph id: " + graphID);
            } else {
                backend_logger.info("Error while reading content length");
                loop = true;
                break;
            }

            std::string selectQuery = "select is_directed from graph where idgraph='" + graphID + "';";

            if (!sqLiteDbInterface) {
                backend_logger.error("Database interface is null!");
                break;
            }

            auto partitionAlgorithm = sqLiteDbInterface->runSelect(selectQuery);

            if (partitionAlgorithm.empty() || partitionAlgorithm[0].empty()) {
                backend_logger.error("Query returned no results.");
                break;
            }

            std::string partitionAlgorithmName = partitionAlgorithm[0][0].second;
            int message_length = partitionAlgorithmName.length();
            int converted_number = htonl(message_length);
            backend_logger.info("Sending worker info length: " + to_string(converted_number));
            if (!Utils::send_int_wrapper(connFd, &converted_number, sizeof(converted_number))) {
                loop = true;
                break;
            }

            std::string length_ack(CONTENT_LENGTH_ACK.length(), 0);
            return_status = recv(connFd, &length_ack[0], CONTENT_LENGTH_ACK.length(), 0);
            if (return_status > 0) {
                backend_logger.info("Received content length ack: " + length_ack);
            } else {
                backend_logger.info("Error while reading content length ack");
                loop = true;
                break;
            }

            if (!Utils::send_str_wrapper(connFd, partitionAlgorithmName)) {
                loop = true;
                break;
            }
        } else {
            backend_logger.error("Message format not recognized");
            sleep(1);
        }
    }
    backend_logger.info("Closing thread " + to_string(pthread_self()) + " and connection");
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
        backend_logger.error("Cannot open socket");
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
        backend_logger.error("Cannot bind on port " + portNo);
        return 0;
    }

    listen(listenFd, 10);

    len = sizeof(clntAdd);

    while (true) {
        backend_logger.info("Backend Listening");

        // this is where client connects. svr will hang in this mode until client conn
        int connFd = accept(listenFd, (struct sockaddr *)&clntAdd, &len);

        if (connFd < 0) {
            backend_logger.error("Cannot accept connection");
            continue;
        }
        backend_logger.info("Connection successful");
        backendservicesessionargs *sessionargs = new backendservicesessionargs;
        sessionargs->sqlite = this->sqlite;
        sessionargs->connFd = connFd;
        pthread_t pt;
        pthread_create(&pt, NULL, backendservicesesion, sessionargs);
        pthread_detach(pt);
    }

    return 1;
}
