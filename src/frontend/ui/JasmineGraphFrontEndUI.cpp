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
#include <iostream>
#include <fstream>
#include <string>
#include <curl/curl.h>
#include <regex>

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
static std::atomic<int> currentFESession;
static bool canCalibrate = true;
Logger ui_frontend_logger;
std::set<ProcessInfo> processdata;
bool JasmineGraphFrontEndUI::strian_exit;
string JasmineGraphFrontEndUI::stream_topic_name;

static void list_command(int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p);
static void add_graph_command(std::string masterIP,
    int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p, std::string command);
static void remove_graph_command(std::string masterIP,
    int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p, std::string command);
static void triangles_command(std::string masterIP,
    int connFd, SQLiteDBInterface *sqlite, PerformanceSQLiteDBInterface *perfSqlite,
    JobScheduler *jobScheduler, bool *loop_exit_p, std::string command);

void *uifrontendservicesesion(void *dummyPt) {
    frontendservicesessionargs *sessionargs = (frontendservicesessionargs *)dummyPt;
    std::string masterIP = sessionargs->masterIP;
    int connFd = sessionargs->connFd;
    SQLiteDBInterface *sqlite = sessionargs->sqlite;
    PerformanceSQLiteDBInterface *perfSqlite = sessionargs->perfSqlite;
    JobScheduler *jobScheduler = sessionargs->jobScheduler;
    delete sessionargs;

    if (JasmineGraphFrontEndCommon::checkServerBusy(&currentFESession, connFd)) {
        ui_frontend_logger.error("Server is busy");
        return NULL;
    }

    char data[FRONTEND_DATA_LENGTH + 1];
    //  Initiate Thread
    thread input_stream_handler;

    bool loop_exit = false;
    int failCnt = 0;
    while (!loop_exit) {
        std::string line = JasmineGraphFrontEndCommon::readAndProcessInput(connFd, data, failCnt);
        if (line.empty()) {
            continue;
        }
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

        // split the string in '|' and take first
        char delimiter = '|';
        std::stringstream ss(line);
        std::string token;
        std::getline(ss, token, delimiter);

        if (token.compare(EXIT) == 0) {
            break;
        } else if (token.compare(LIST) == 0) {
            list_command(connFd, sqlite, &loop_exit);
        } else if (token.compare(ADGR) == 0) {
            add_graph_command(masterIP, connFd, sqlite, &loop_exit, line);
        } else if (token.compare(TRIANGLES) == 0) {
            triangles_command(masterIP, connFd, sqlite, perfSqlite, jobScheduler, &loop_exit, line);
        } else if (token.compare(RMGR) == 0) {
            remove_graph_command(masterIP, connFd, sqlite, &loop_exit, line);
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
    std::vector<vector<pair<string, string>>> data = JasmineGraphFrontEndCommon::getGraphData(sqlite);

    // Iterate through the result set and construct the JSON array
    for (auto &row : data) {
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

// Function to extract the file name from the URL
std::string extractFileNameFromURL(const std::string& url) {
    std::regex urlRegex("([^/]+)$");
    std::smatch matches;
    if (std::regex_search(url, matches, urlRegex) && matches.size() > 1) {
        return matches.str(1);
    }
    return "downloaded_file";
}

std::string sanitizeFileName(const std::string& fileName) {
    // Remove unsafe characters using regex (allow alphanumeric and some safe symbols)
    std::regex unsafePattern("[^a-zA-Z0-9_.-]");
    return std::regex_replace(fileName, unsafePattern, "");
}

static void add_graph_command(std::string masterIP,
    int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p, std::string command) {
    char delimiter = '|';
    std::stringstream ss(command);
    std::string token;
    std::string graph;
    std::string fileURL;

    std::getline(ss, token, delimiter);
    std::getline(ss, graph, delimiter);
    std::getline(ss, fileURL, delimiter);

    std::string safeFileName = sanitizeFileName(extractFileNameFromURL(fileURL));
    std::string localFilePath = Conts::TEMP_GRAPH_FILE_PATH + safeFileName;
    std::string savedFilePath = Utils::downloadFile(fileURL, localFilePath);

    if (!savedFilePath.empty()) {
        ui_frontend_logger.info("File downloaded and saved as "+ savedFilePath);
    } else {
        ui_frontend_logger.info("Failed to download the file.");
    }

    int result_wr = write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
    if (result_wr < 0) {
        ui_frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, "\r\n", 2);
    if (result_wr < 0) {
        ui_frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    string name = "";
    string path = "";
    string partitionCount = "";

    name = graph;
    path = savedFilePath;

    std::time_t time = chrono::system_clock::to_time_t(chrono::system_clock::now());
    string uploadStartTime = ctime(&time);

    partitionCount = JasmineGraphFrontEndCommon::getPartitionCount(path);

    if (JasmineGraphFrontEndCommon::graphExists(path, sqlite)) {
        ui_frontend_logger.error("Graph exists");
        // TODO: inform client?
        return;
    }

    if (Utils::fileExists(path)) {
        ui_frontend_logger.info("Path exists");

        string sqlStatement =
            "INSERT INTO graph (name,upload_path,upload_start_time,upload_end_time,graph_status_idgraph_status,"
            "vertexcount,centralpartitioncount,edgecount) VALUES(\"" +
            name + "\", \"" + path + "\", \"" + uploadStartTime + "\", \"\",\"" +
            to_string(Conts::GRAPH_STATUS::LOADING) + "\", \"\", \"\", \"\")";
        int newGraphID = sqlite->runInsert(sqlStatement);
        MetisPartitioner partitioner(sqlite);
        vector<std::map<int, string>> fullFileList;

        partitioner.loadDataSet(path, newGraphID);
        int result = partitioner.constructMetisFormat(Conts::GRAPH_TYPE_NORMAL);
        if (result == 0) {
            string reformattedFilePath = partitioner.reformatDataSet(path, newGraphID);
            partitioner.loadDataSet(reformattedFilePath, newGraphID);
            partitioner.constructMetisFormat(Conts::GRAPH_TYPE_NORMAL_REFORMATTED);
            fullFileList = partitioner.partitioneWithGPMetis(partitionCount);
        } else {
            fullFileList = partitioner.partitioneWithGPMetis(partitionCount);
        }
        ui_frontend_logger.info("Upload done");
        JasmineGraphServer *server = JasmineGraphServer::getInstance();
        server->uploadGraphLocally(newGraphID, Conts::GRAPH_TYPE_NORMAL, fullFileList, masterIP);
        Utils::deleteDirectory(Utils::getHomeDir() + "/.jasminegraph/tmp/" + to_string(newGraphID));
        JasmineGraphFrontEndCommon::getAndUpdateUploadTime(to_string(newGraphID), sqlite);
        int result_wr = write(connFd, DONE.c_str(), DONE.size());
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
        ui_frontend_logger.error("Graph data file does not exist on the specified path");
    }
}

static void remove_graph_command(std::string masterIP,
    int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p, std::string command) {
    char delimiter = '|';
    std::stringstream ss(command);
    std::string token;
    std::string graphID;

    std::getline(ss, token, delimiter);
    std::getline(ss, graphID, delimiter);

    ui_frontend_logger.info("recieved graph id: " + graphID);

    if (JasmineGraphFrontEndCommon::graphExistsByID(graphID, sqlite)) {
        ui_frontend_logger.info("Graph with ID " + graphID + " is being deleted now");
        JasmineGraphFrontEndCommon::removeGraph(graphID, sqlite, masterIP);
        int result_wr = write(connFd, DONE.c_str(), DONE.size());
        if (result_wr < 0) {
            ui_frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        result_wr = write(connFd, "\r\n", 2);
        if (result_wr < 0) {
            ui_frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
    } else {
        ui_frontend_logger.error("Graph does not exist or cannot be deleted with the current hosts setting");
        int result_wr = write(connFd, ERROR.c_str(), ERROR.size());
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
    }
}

static void triangles_command(std::string masterIP, int connFd,
    SQLiteDBInterface *sqlite, PerformanceSQLiteDBInterface *perfSqlite,
    JobScheduler *jobScheduler, bool *loop_exit_p, std::string command) {
    char delimiter = '|';
    std::stringstream ss(command);
    std::string token;
    std::string graph_id;
    std::string priority;

    std::getline(ss, token, delimiter);
    std::getline(ss, graph_id, delimiter);
    std::getline(ss, priority, delimiter);

    ui_frontend_logger.info("recieved graph id: " + graph_id);
    ui_frontend_logger.info("Priority: " + priority);

    // add RDF graph
    int uniqueId = JasmineGraphFrontEndCommon::getUid();

    string name = "";

    if (!JasmineGraphFrontEndCommon::graphExistsByID(graph_id, sqlite)) {
        string error_message = "The specified graph id does not exist";
        int result_wr = write(connFd, error_message.c_str(), FRONTEND_COMMAND_LENGTH);
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
        if (!(std::find_if(priority.begin(), priority.end(), [](unsigned char c) { return !std::isdigit(c); }) ==
              priority.end())) {
            *loop_exit_p = true;
            string error_message = "Priority should be numeric and > 1 or empty";
            int result_wr = write(connFd, error_message.c_str(), error_message.length());
            if (result_wr < 0) {
                ui_frontend_logger.error("Error writing to socket");
                return;
            }

            result_wr = write(connFd, "\r\n", 2);
            if (result_wr < 0) {
                ui_frontend_logger.error("Error writing to socket");
            }
            return;
        }

        int threadPriority = std::atoi(priority.c_str());

        static volatile int reqCounter = 0;
        string reqId = to_string(reqCounter++);
        ui_frontend_logger.info("Started processing request " + reqId);
        auto begin = chrono::high_resolution_clock::now();
        JobRequest jobDetails;
        jobDetails.setJobId(std::to_string(uniqueId));
        jobDetails.setJobType(TRIANGLES);

        long graphSLA = -1;  // This prevents auto calibration for priority=1 (=default priority)
        if (threadPriority > Conts::DEFAULT_THREAD_PRIORITY) {
            // All high priority threads will be set the same high priority level
            threadPriority = Conts::HIGH_PRIORITY_DEFAULT_VALUE;
            graphSLA = JasmineGraphFrontEndCommon::getSLAForGraphId(sqlite, perfSqlite, graph_id, TRIANGLES,
                                                              Conts::SLA_CATEGORY::LATENCY);
            jobDetails.addParameter(Conts::PARAM_KEYS::GRAPH_SLA, std::to_string(graphSLA));
        }

        if (graphSLA == 0) {
            if (JasmineGraphFrontEnd::areRunningJobsForSameGraph()) {
                if (canCalibrate) {
                    // initial calibration
                    jobDetails.addParameter(Conts::PARAM_KEYS::AUTO_CALIBRATION, "false");
                } else {
                    // auto calibration
                    jobDetails.addParameter(Conts::PARAM_KEYS::AUTO_CALIBRATION, "true");
                }
            } else {
                // TODO(ASHOK12011234): Need to investigate for multiple graphs
                ui_frontend_logger.error("Can't calibrate the graph now");
            }
        }

        jobDetails.setPriority(threadPriority);
        jobDetails.setMasterIP(masterIP);
        jobDetails.addParameter(Conts::PARAM_KEYS::GRAPH_ID, graph_id);
        jobDetails.addParameter(Conts::PARAM_KEYS::CATEGORY, Conts::SLA_CATEGORY::LATENCY);
        if (canCalibrate) {
            jobDetails.addParameter(Conts::PARAM_KEYS::CAN_CALIBRATE, "true");
        } else {
            jobDetails.addParameter(Conts::PARAM_KEYS::CAN_CALIBRATE, "false");
        }

        jobScheduler->pushJob(jobDetails);
        JobResponse jobResponse = jobScheduler->getResult(jobDetails);
        std::string errorMessage = jobResponse.getParameter(Conts::PARAM_KEYS::ERROR_MESSAGE);

        if (!errorMessage.empty()) {
            *loop_exit_p = true;
            int result_wr = write(connFd, errorMessage.c_str(), errorMessage.length());

            if (result_wr < 0) {
                ui_frontend_logger.error("Error writing to socket");
                return;
            }
            result_wr = write(connFd, "\r\n", 2);
            if (result_wr < 0) {
                ui_frontend_logger.error("Error writing to socket");
            }
            return;
        }

        std::string triangleCount = jobResponse.getParameter(Conts::PARAM_KEYS::TRIANGLE_COUNT);

        if (threadPriority == Conts::HIGH_PRIORITY_DEFAULT_VALUE) {
            highPriorityTaskCount--;
        }

        auto end = chrono::high_resolution_clock::now();
        auto dur = end - begin;
        auto msDuration = std::chrono::duration_cast<std::chrono::milliseconds>(dur).count();
        ui_frontend_logger.info("Req: " + reqId + " Triangle Count: " + triangleCount +
                             " Time Taken: " + to_string(msDuration) + " milliseconds");
        int result_wr = write(connFd, triangleCount.c_str(), triangleCount.length());
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
    }
}
