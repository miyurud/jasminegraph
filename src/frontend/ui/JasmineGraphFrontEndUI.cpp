/**
Copyright 2019 JasminGraph Team
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

#include "JasmineGraphFrontEndUI.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <fstream>
#include <nlohmann/json.hpp>
#include <set>
#include <thread>

#include "../../metadb/SQLiteDBInterface.h"
#include "../../nativestore/DataPublisher.h"
#include "../../partitioner/local/JSONParser.h"
#include "../../partitioner/local/MetisPartitioner.h"
#include "../../partitioner/stream/Partitioner.h"
#include "../../performance/metrics/PerformanceUtil.h"
#include "../../server/JasmineGraphServer.h"
#include "../../util/Conts.h"
#include "../../util/kafka/KafkaCC.h"
#include "../../util/logger/Logger.h"
#include "JasmineGraphFrontEndUIProtocol.h"
#include "../core/common/JasmineGraphFrontendCommon.h"
#include "../core/scheduler/JobScheduler.h"

#define MAX_PENDING_CONNECTIONS 10
#define DATA_BUFFER_SIZE (FRONTEND_DATA_LENGTH + 1)

using json = nlohmann::json;
using namespace std;
using namespace std::chrono;

static int connFd;
static volatile int currentFESession;
static bool canCalibrate = true;
Logger ui_frontend_logger;
std::set<ProcessInfo> processdata;
bool JasmineGraphFrontEndUI::strian_exit;
string JasmineGraphFrontEndUI::stream_topic_name;

static void list_command(int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p);

void *uifrontendservicesesion(void *dummyPt) {
    frontendservicesessionargs *sessionargs = (frontendservicesessionargs *)dummyPt;
    std::string masterIP = sessionargs->masterIP;
    int connFd = sessionargs->connFd;
    SQLiteDBInterface *sqlite = sessionargs->sqlite;
    PerformanceSQLiteDBInterface *perfSqlite = sessionargs->perfSqlite;
    JobScheduler *jobScheduler = sessionargs->jobScheduler;
    delete sessionargs;
    if (currentFESession++ > Conts::MAX_FE_SESSIONS) {
        if (!Utils::send_str_wrapper(connFd, "JasmineGraph server is busy. Please try again later.")) {
            ui_frontend_logger.error("Error writing to socket");
        }
        close(connFd);
        currentFESession--;
        return NULL;
    }

    char data[FRONTEND_DATA_LENGTH + 1];
    //  Initiate Thread
    thread input_stream_handler;
    //  Initiate kafka consumer parameters
    std::string partitionCount = Utils::getJasmineGraphProperty("org.jasminegraph.server.npartitions");
    int numberOfPartitions = std::stoi(partitionCount);
    std::string kafka_server_IP;
    cppkafka::Configuration configs;
    KafkaConnector *kstream;
    Partitioner graphPartitioner(numberOfPartitions, 1, spt::Algorithms::HASH);

    vector<DataPublisher *> workerClients;
    bool workerClientsInitialized = false;

    bool loop_exit = false;
    int failCnt = 0;
    while (!loop_exit) {
        string line = Utils::read_str_wrapper(connFd, data, FRONTEND_DATA_LENGTH, true);
        if (line.empty()) {
            failCnt++;
            if (failCnt > 4) {
                break;
            }
            sleep(1);
            continue;
        }
        failCnt = 0;
        line = Utils::trim_copy(line);
        ui_frontend_logger.info("Command received: " + line);
        if (line.empty()) {
            continue;
        }

        if (currentFESession > 1) {
            canCalibrate = false;
        } else {
            canCalibrate = true;
            workerResponded = false;
        }

        if (line.compare(EXIT) == 0) {
            break;
        } else if (line.compare(LIST) == 0) {
            list_command(connFd, sqlite, &loop_exit);
        } else {
            ui_frontend_logger.error("Message format not recognized " + line);
            int result_wr = write(connFd, INVALID_FORMAT.c_str(), INVALID_FORMAT.size());
            if (result_wr < 0) {
                ui_frontend_logger.error("Error writing to socket");
                break;
            }
        }
    }
    if (input_stream_handler.joinable()) {
        input_stream_handler.join();
    }
    ui_frontend_logger.info("Closing thread " + to_string(pthread_self()) + " and connection");
    close(connFd);
    currentFESession--;
    return NULL;
}

JasmineGraphFrontEndUI::JasmineGraphFrontEndUI(SQLiteDBInterface *db, PerformanceSQLiteDBInterface *perfDb,
                                           std::string masterIP, JobScheduler *jobScheduler) {
    this->sqlite = db;
    this->masterIP = masterIP;
    this->perfSqlite = perfDb;
    this->jobScheduler = jobScheduler;
}

int JasmineGraphFrontEndUI::run() {
    int pId;
    int portNo = Conts::JASMINEGRAPH_UI_FRONTEND_PORT;
    int listenFd;
    socklen_t len;
    bool loop = false;
    struct sockaddr_in svrAdd;
    struct sockaddr_in clntAdd;

    // create socket
    listenFd = socket(AF_INET, SOCK_STREAM, 0);

    if (listenFd < 0) {
        ui_frontend_logger.error("Cannot open socket");
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
        ui_frontend_logger.error("Cannot bind on port " + portNo);
        return 0;
    }

    listen(listenFd, MAX_PENDING_CONNECTIONS);

    std::vector<std::thread> threadVector;
    len = sizeof(clntAdd);

    int noThread = 0;

    while (true) {
        ui_frontend_logger.info("Frontend Listening");

        // this is where client connects. svr will hang in this mode until client conn
        connFd = accept(listenFd, (struct sockaddr *)&clntAdd, &len);

        if (connFd < 0) {
            ui_frontend_logger.error("Cannot accept connection");
            continue;
        }
        ui_frontend_logger.info("Connection successful from " + std::string(inet_ntoa(clntAdd.sin_addr)));

        frontendservicesessionargs *sessionargs = new frontendservicesessionargs;
        sessionargs->masterIP = masterIP;
        sessionargs->connFd = connFd;
        sessionargs->sqlite = this->sqlite;
        sessionargs->perfSqlite = this->perfSqlite;
        sessionargs->jobScheduler = this->jobScheduler;
        pthread_t pt;
        pthread_create(&pt, NULL, uifrontendservicesesion, sessionargs);
        pthread_detach(pt);
    }
}

static void list_command(int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p) {
    json result_json = json::array();  // Create a JSON array to hold the result

    // Fetch data from the database
    std::vector<vector<pair<string, string>>> v = JasmineGraphFrontEndCommon::getGraphData(sqlite);

    // Iterate through the result set and construct the JSON array
    for (auto &row : v) {
        json entry;  // JSON object for a single row
        int counter = 0;

        for (auto &column : row) {
            switch (counter) {
                case 0:
                    entry["idgraph"] = column.second;
                    break;
                case 1:
                    entry["name"] = column.second;
                    break;
                case 2:
                    entry["upload_path"] = column.second;
                    break;
                case 3:
                    if (std::stoi(column.second) == Conts::GRAPH_STATUS::LOADING) {
                        entry["status"] = "loading";
                    } else if (std::stoi(column.second) == Conts::GRAPH_STATUS::DELETING) {
                        entry["status"] = "deleting";
                    } else if (std::stoi(column.second) == Conts::GRAPH_STATUS::NONOPERATIONAL) {
                        entry["status"] = "nop";
                    } else if (std::stoi(column.second) == Conts::GRAPH_STATUS::OPERATIONAL) {
                        entry["status"] = "op";
                    }
                    break;
                case 4:
                    entry["vertexcount"] = column.second;
                    break;
                case 5:
                    entry["edgecount"] = column.second;
                    break;
                case 6:
                    entry["centralpartitioncount"] = column.second;
                    break;
                default:
                    break;
            }
            counter++;
        }

        // Add the entry to the JSON array
        result_json.push_back(entry);
    }

    // Convert JSON object to string
    string result = result_json.dump();

    // Write the result to the socket
    if (result.size() == 0) {
        int result_wr = write(connFd, EMPTY.c_str(), EMPTY.length());
        if (result_wr < 0) {
            ui_frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }

        result_wr = write(connFd, "\r\n", 2);
        if (result_wr < 0) {
            ui_frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
    } else {
        int result_wr = write(connFd, result.c_str(), result.length());
        if (result_wr < 0) {
            ui_frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
    }
}
