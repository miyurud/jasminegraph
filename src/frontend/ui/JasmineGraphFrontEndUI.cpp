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
#include "../../partitioner/local/RDFParser.h"
#include "../../util/kafka/StreamHandler.h"
#include "../JasmineGraphFrontEndProtocol.h"
#include "antlr4-runtime.h"
#include "/home/ubuntu/software/antlr/CypherLexer.h"
#include "/home/ubuntu/software/antlr/CypherParser.h"
#include "../../query/processor/cypher/astbuilder/ASTBuilder.h"
#include "../../query/processor/cypher/astbuilder/ASTNode.h"
#include "../../query/processor/cypher/semanticanalyzer/SemanticAnalyzer.h"
#include "../../query/processor/cypher/queryplanner/Operators.h"
#include "../../query/processor/cypher/queryplanner/QueryPlanner.h"
#include "../../localstore/incremental/JasmineGraphIncrementalLocalStore.h"
#include "../../server/JasmineGraphInstanceService.h"
#include "../../query/processor/cypher/util/SharedBuffer.h"
#include "../../query/processor/cypher/runtime/AggregationFactory.h"
#include "../../query/processor/cypher/runtime/Aggregation.h"

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
static void get_degree_command(int connFd, std::string command, int numberOfPartition,
                               std::string type, bool *loop_exit_p);
static void cypher_ast_command(int connFd, vector<DataPublisher *> &workerClients,
                               int numberOfPartitions, bool *loop_exit, std::string command);

static vector<DataPublisher *> getWorkerClients(SQLiteDBInterface *sqlite) {
    const vector<Utils::worker> &workerList = Utils::getWorkerList(sqlite);
    vector<DataPublisher *> workerClients;
    for (int i = 0; i < workerList.size(); i++) {
        Utils::worker currentWorker = workerList.at(i);
        string workerHost = currentWorker.hostname;
        int workerPort = atoi(string(currentWorker.port).c_str());
        int dataPort = atoi(string(currentWorker.dataPort).c_str());
        DataPublisher *workerClient = new DataPublisher(workerPort, workerHost, dataPort);
        workerClients.push_back(workerClient);
    }
    return workerClients;
}
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
    std::string partitionCount = Utils::getJasmineGraphProperty("org.jasminegraph.server.npartitions");
    int numberOfPartitions = std::stoi(partitionCount);
    std::string kafka_server_IP;
    cppkafka::Configuration configs;
    KafkaConnector *kstream;

    vector<DataPublisher *> workerClients;
    bool workerClientsInitialized = false;

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
        } else if (token.compare(IN_DEGREE) == 0) {
            get_degree_command(connFd, line, numberOfPartitions, "_idd_",  &loop_exit);
        } else if (token.compare(OUT_DEGREE) == 0) {
            get_degree_command(connFd, line, numberOfPartitions, "_odd_",  &loop_exit);
        } else if (token.compare(CYPHER) == 0) {
            workerClients = getWorkerClients(sqlite);
            workerClientsInitialized = true;
            cypher_ast_command(connFd, workerClients, numberOfPartitions, &loop_exit, line);
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
    std::vector<vector<pair<string, string>>> graphData = JasmineGraphFrontEndCommon::getGraphData(sqlite);

    // Fetch partition data
    std::vector<std::vector<std::pair<std::string, std::string>>> partitionData =
        JasmineGraphFrontEndCommon::getPartitionData(sqlite);

    // Create a map to group partitions by graph_idgraph for efficient lookup
    std::unordered_map<int, std::vector<json>> partition_map;
    for (const auto& row : partitionData) {
        if (row.size() != 7) {
            // Log error or skip malformed partition row
            continue;
        }

        json partition_entry;
        int graph_idgraph = -1;

        for (const auto& column : row) {
            const std::string& col_name = column.first;
            const std::string& col_value = column.second;

            try {
                if (col_name == "idpartition") {
                    partition_entry["idpartition"] = std::stoi(col_value);
                } else if (col_name == "graph_idgraph") {
                    graph_idgraph = std::stoi(col_value);  // Store for grouping
                } else if (col_name == "vertexcount") {
                    partition_entry["vertexcount"] = std::stoi(col_value);
                } else if (col_name == "central_vertexcount") {
                    partition_entry["central_vertexcount"] = std::stoi(col_value);
                } else if (col_name == "edgecount") {
                    partition_entry["edgecount"] = std::stoi(col_value);
                } else if (col_name == "central_edgecount") {
                    partition_entry["central_edgecount"] = std::stoi(col_value);
                } else if (col_name == "central_edgecount_with_dups") {
                    partition_entry["central_edgecount_with_dups"] = std::stoi(col_value);
                }
            } catch (const std::exception& e) {
                // Log error and skip this partition
                partition_entry.clear();
                break;
            }
        }

        if (!partition_entry.empty() && graph_idgraph != -1) {
            partition_map[graph_idgraph].push_back(partition_entry);
        }
    }

    // Process graph data
    for (const auto& row : graphData) {
        if (row.size() != 7) {
            // Log error or skip malformed graph row
            continue;
        }

        json entry;  // JSON object for a single graph
        int idgraph = -1;

        // Map graph columns to JSON
        for (const auto& column : row) {
            const std::string& col_name = column.first;
            const std::string& col_value = column.second;

            try {
                if (col_name == "idgraph") {
                    idgraph = std::stoi(col_value);
                    entry["idgraph"] = idgraph;
                } else if (col_name == "name") {
                    entry["name"] = col_value;
                } else if (col_name == "upload_path") {
                    entry["upload_path"] = col_value;
                } else if (col_name == "graph_status_idgraph_status") {
                    try {
                        if (std::stoi(column.second) == Conts::GRAPH_STATUS::LOADING) {
                            entry["status"] = "loading";
                        } else if (std::stoi(column.second) == Conts::GRAPH_STATUS::DELETING) {
                            entry["status"] = "deleting";
                        } else if (std::stoi(column.second) == Conts::GRAPH_STATUS::NONOPERATIONAL) {
                            entry["status"] = "nop";
                        } else if (std::stoi(column.second) == Conts::GRAPH_STATUS::OPERATIONAL) {
                            entry["status"] = "op";
                        }
                    } catch (const std::exception& e) {
                        entry["status"] = "unknown";  // Handle invalid status
                    }
                } else if (col_name == "vertexcount") {
                    entry["vertexcount"] = std::stoi(col_value);
                } else if (col_name == "edgecount") {
                    entry["edgecount"] = std::stoi(col_value);
                } else if (col_name == "centralpartitioncount") {
                    entry["centralpartitioncount"] = std::stoi(col_value);
                }
            } catch (const std::exception& e) {
                // Log error and skip this graph
                entry.clear();
                break;
            }
        }

        // Add partitions array for this graph
        if (!entry.empty() && idgraph != -1) {
            entry["partitions"] = json::array();
            auto it = partition_map.find(idgraph);
            if (it != partition_map.end()) {
                entry["partitions"] = it->second;  // Add all partitions for this graph
            }
            result_json.push_back(entry);
        }
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

static void get_degree_command(int connFd, std::string command, int numberOfPartition,
                               std::string type, bool *loop_exit_p) {
    char delimiter = '|';
    std::stringstream ss(command);
    std::string token;
    std::string graph_id;

    std::getline(ss, token, delimiter);
    std::getline(ss, graph_id, delimiter);

    string graphID(graph_id);

    graphID = Utils::trim_copy(graphID);
    ui_frontend_logger.info("Graph ID received: " + graphID);

    JasmineGraphServer::inDegreeDistribution(graphID);
    string instanceDataFolderLocation = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");

    ui_frontend_logger.info("instance data folder location" + instanceDataFolderLocation);
    for (int partitionId = 0; partitionId < numberOfPartition; partitionId++) {
        string attributeFilePath = instanceDataFolderLocation + "/" + graphID + type + std::to_string(partitionId);

        // Create an input file stream object
        std::ifstream inputFile(attributeFilePath);

        // Check if the file was opened successfully
        if (!inputFile.is_open()) {
            ui_frontend_logger.error("Error: Could not open the file '" + attributeFilePath + "'");
            continue;
        }

        // Read the file line by line and print to the console
        std::string line;
        while (std::getline(inputFile, line)) {
            std::istringstream iss(line);
            std::string num1, num2;

            // Split the line by tab
            if (std::getline(iss, num1, '\t') && std::getline(iss, num2, '\t')) {
                json point;
                point["node"] = num1;
                point["value"] = num2;

                // Convert JSON object to string and log it
                string result = point.dump();
                // Write the result to the socket
                if (result.size() > 0) {
                    int result_wr = write(connFd, result.c_str(), result.length());
                    if (result_wr < 0) {
                        ui_frontend_logger.error("Error writing to socket");
                        *loop_exit_p = true;
                    }
                    result_wr = write(connFd, "\r\n", 2);
                    if (result_wr < 0) {
                        ui_frontend_logger.error("Error writing to socket");
                        *loop_exit_p = true;
                    }
                }
            } else {
                ui_frontend_logger.error("Error: Malformed line: " + line);
            }
        }

        // Close the file
        inputFile.close();
    }

    int result_wr = write(connFd, "-1", 2);
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

static void cypher_ast_command(int connFd, vector<DataPublisher *> &workerClients,
                               int numberOfPartitions, bool *loop_exit, std::string command) {
    char delimiter = '|';
    std::stringstream ss(command);
    std::string token;
    std::string graph_id;
    std::string query;

    std::getline(ss, token, delimiter);
    std::getline(ss, graph_id, delimiter);
    std::getline(ss, query, delimiter);

    ui_frontend_logger.info("recieved graph id: " + graph_id);
    ui_frontend_logger.info("query: " + query);

    string user_res_1(graph_id);
    string user_res_s(query);

    antlr4::ANTLRInputStream input(user_res_s);
    // Create a lexer from the input
    CypherLexer lexer(&input);

    // Create a token stream from the lexer
    antlr4::CommonTokenStream tokens(&lexer);

    // Create a parser from the token stream
    CypherParser parser(&tokens);

    ASTBuilder ast_builder;
    auto* ast = any_cast<ASTNode*>(ast_builder.visitOC_Cypher(parser.oC_Cypher()));

    SemanticAnalyzer semantic_analyzer;
    string obj;
    if (semantic_analyzer.analyze(ast)) {
        ui_frontend_logger.log("AST is successfully analyzed", "log");
        QueryPlanner query_planner;
        Operator *opr = query_planner.createExecutionPlan(ast);
        obj = opr->execute();
    } else {
        ui_frontend_logger.error("query isn't semantically correct: "+user_res_s);
    }

    // print query plan
    ui_frontend_logger.info((obj.c_str()));

    // Create buffer pool
    std::vector<std::unique_ptr<SharedBuffer>> bufferPool;
    bufferPool.reserve(numberOfPartitions);  // Pre-allocate space for pointers
    for (size_t i = 0; i < numberOfPartitions; ++i) {
        bufferPool.emplace_back(std::make_unique<SharedBuffer>(5));
    }

    // send query plan
    JasmineGraphServer *server = JasmineGraphServer::getInstance();
    server->sendQueryPlan(stoi(user_res_1), workerClients.size(), obj, std::ref(bufferPool));

    int closeFlag = 0;
    if (Operator::isAggregate) {
        if (Operator::aggregateType == AggregationFactory::AVERAGE) {
            Aggregation* aggregation = AggregationFactory::getAggregationMethod(AggregationFactory::AVERAGE);
            while (true) {
                if (closeFlag == numberOfPartitions) {
                    write(connFd, "-1", 2);
                    break;
                }
                for (size_t i = 0; i < bufferPool.size(); ++i) {
                    std::string data;
                    if (bufferPool[i]->tryGet(data)) {
                        if (data == "-1") {
                            closeFlag++;
                        } else {
                            aggregation->insert(data);
                        }
                    }
                }
            }
            aggregation->getResult(connFd);
        } else if (Operator::aggregateType == AggregationFactory::ASC ||
            Operator::aggregateType == AggregationFactory::DESC) {
            struct BufferEntry {
                std::string value;
                size_t bufferIndex;
                json data;
                bool isAsc;
                BufferEntry(const std::string& v, size_t idx, const json& parsed, bool asc)
                    : value(v), bufferIndex(idx), data(parsed), isAsc(asc) {}
                bool operator<(const BufferEntry& other) const {
                    const auto& val1 = data[Operator::aggregateKey];
                    const auto& val2 = other.data[Operator::aggregateKey];
                    bool result;
                    if (val1.is_number_integer() && val2.is_number_integer()) {
                        result = val1.get<int>() > val2.get<int>();
                    } else if (val1.is_string() && val2.is_string()) {
                        result = val1.get<std::string>() > val2.get<std::string>();
                    } else {
                        result = val1.dump() > val2.dump();
                    }
                    return isAsc ? result : !result;  // Flip for DESC
                }
            };

            // Initialize with first value from each buffer
            bool isAsc = (Operator::aggregateType == AggregationFactory::ASC);
            std::priority_queue<BufferEntry> mergeQueue;  // Min-heap
            for (size_t i = 0; i < numberOfPartitions; ++i) {
                std::string value = bufferPool[i]->get();
                if (value != "-1") {
                    try {
                        json parsed = json::parse(value);
                        if (!parsed.contains(Operator::aggregateKey)) {
                            ui_frontend_logger.error("Missing key '" + Operator::aggregateKey + "' in JSON: " + value);
                            continue;
                        }
                        BufferEntry entry{value, i, parsed, isAsc};
                        mergeQueue.push(entry);
                    } catch (const json::exception& e) {
                        ui_frontend_logger.error("JSON parse error: " + std::string(e.what()));
                        continue;
                    }
                } else {
                    closeFlag++;
                }
            }

            ui_frontend_logger.info("START MASTER SORTING");
            ui_frontend_logger.info(std::to_string(mergeQueue.size()));

            // Merge loop
            while (!mergeQueue.empty()) {
                ui_frontend_logger.info(":::::::FRONTEND:::::::");

                // Pick smallest value
                BufferEntry smallest = mergeQueue.top();
                ui_frontend_logger.info(smallest.value);
                size_t queueSize = mergeQueue.size();
                ui_frontend_logger.info(std::to_string(queueSize));
                mergeQueue.pop();
                int result_wr = write(connFd, smallest.value.c_str(), smallest.value.length());
                if (result_wr < 0) {
                    ui_frontend_logger.error("Error writing to socket");
                    return;
                }
                result_wr = write(connFd, "\r\n", 2);
                if (result_wr < 0) {
                    ui_frontend_logger.error("Error writing to socket");
                    return;
                }

                // Only fetch next value if the buffer isn't exhausted
                if (closeFlag < numberOfPartitions) {
                    std::string nextValue = bufferPool[smallest.bufferIndex]->get();
                    if (nextValue == "-1") {
                        closeFlag++;
                        ui_frontend_logger.info("closeflag" + std::to_string(closeFlag));
                    } else {
                        try {
                            json parsed = json::parse(nextValue);
                            if (!parsed.contains(Operator::aggregateKey)) {
                                ui_frontend_logger.error("Missing key '" + Operator::aggregateKey +
                                    "' in JSON: " + nextValue);
                                continue;
                            }
                            BufferEntry entry{nextValue, smallest.bufferIndex, parsed, isAsc};
                            mergeQueue.push(entry);
                        } catch (const json::exception& e) {
                            ui_frontend_logger.error("JSON parse error: " + std::string(e.what()));
                        }
                    }
                }
            }
        } else {
            std::string log = "Query is recongnized as Aggreagation, but method doesnot have implemented yet";
            int result_wr = write(connFd, log.c_str(), log.length());
            result_wr = write(connFd, "\r\n", 2);
        }
    } else {
        while (true) {
            if (closeFlag == numberOfPartitions) {
                write(connFd, "-1", 2);
                break;
            }

            for (size_t i = 0; i < bufferPool.size(); ++i) {
                std::string data;
                if (bufferPool[i]->tryGet(data)) {
                    if (data == "-1") {
                        closeFlag++;
                    } else {
                        int result_wr = write(connFd, data.c_str(), data.length());
                        result_wr = write(connFd, "\r\n", 2);
                    }
                }
            }
        }
    }
}
