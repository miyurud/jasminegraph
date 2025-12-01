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

#include "JasmineGraphFrontEnd.h"

#include <curl/curl.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <ctime>
#include <fstream>
#include <iostream>
#include <map>
#include <nlohmann/json.hpp>
#include <set>
#include <thread>

#include "../knowledgegraph/construction/Pipeline.h"
#include "../localstore/incremental/JasmineGraphIncrementalLocalStore.h"
#include "../metadb/SQLiteDBInterface.h"
#include "../nativestore/DataPublisher.h"
#include "../nativestore/RelationBlock.h"
#include "../partitioner/local/JSONParser.h"
#include "../partitioner/local/MetisPartitioner.h"
#include "../partitioner/local/RDFParser.h"
#include "../partitioner/local/RDFPartitioner.h"
#include "../partitioner/stream/Partitioner.h"
#include "../performance/metrics/PerformanceUtil.h"
#include "../query/algorithms/linkprediction/JasminGraphLinkPredictor.h"
#include "../query/processor/cypher/astbuilder/ASTBuilder.h"
#include "../query/processor/cypher/astbuilder/ASTNode.h"
#include "../query/processor/cypher/queryplanner/Operators.h"
#include "../query/processor/cypher/queryplanner/QueryPlanner.h"
#include "../query/processor/cypher/runtime/Aggregation.h"
#include "../query/processor/cypher/runtime/AggregationFactory.h"
#include "../query/processor/cypher/semanticanalyzer/SemanticAnalyzer.h"
#include "../query/processor/cypher/util/SharedBuffer.h"
#include "../server/JasmineGraphInstanceProtocol.h"
#include "../server/JasmineGraphInstanceService.h"
#include "../server/JasmineGraphServer.h"
#include "../util/Conts.h"
#include "../util/hdfs/HDFSConnector.h"
#include "../util/hdfs/HDFSStreamHandler.h"
#include "../util/kafka/KafkaCC.h"
#include "../util/kafka/StreamHandler.h"
#include "../util/logger/Logger.h"
#include "/home/ubuntu/software/antlr/CypherLexer.h"
#include "/home/ubuntu/software/antlr/CypherParser.h"
#include "JasmineGraphFrontEndProtocol.h"
#include "antlr4-runtime.h"
#include "core/CoreConstants.h"
#include "core/common/JasmineGraphFrontendCommon.h"
#include "core/scheduler/JobScheduler.h"

#define MAX_PENDING_CONNECTIONS 10
#define DATA_BUFFER_SIZE (FRONTEND_DATA_LENGTH + 1)

using json = nlohmann::json;
using namespace std;
using namespace std::chrono;

std::atomic<int> highPriorityTaskCount;
static int connFd;
static std::atomic<int> currentFESession;
static bool canCalibrate = true;
Logger frontend_logger;
std::set<ProcessInfo> processData;
std::string stream_topic_name;
bool JasmineGraphFrontEnd::strian_exit;
std::map<int, std::thread::id> activeStreamThreads;           // map graphID → thread ID
std::map<int, std::shared_ptr<std::atomic<bool>>> stopFlags;  // map graphID → stop flag

std::mutex threadMapMutex;
static void list_command(int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p);
static void cypherCommand(std::string masterIP, int connFd, vector<DataPublisher *> &workerClients,
                          int numberOfPartitions, bool *loop_exit, SQLiteDBInterface *sqlite,
                          PerformanceSQLiteDBInterface *perfSqlite, JobScheduler *jobScheduler);
static void semanticBeamSearch(std::string masterIP, int connFd, vector<DataPublisher *> &workerClients,
                               int numberOfPartitions, bool *loop_exit, SQLiteDBInterface *sqlite,
                               PerformanceSQLiteDBInterface *perfSqlite, JobScheduler *jobScheduler);
static void add_rdf_command(std::string masterIP, int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p);
static void add_graph_command(std::string masterIP, int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p);
static void add_graph_cust_command(std::string masterIP, int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p);
static void remove_graph_command(std::string masterIP, int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p);
static void add_model_command(int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p);
static void add_stream_kafka_command(int connFd, std::string &kafka_server_IP, cppkafka::Configuration &configs,
                                     KafkaConnector *&kstream, thread &input_stream_handler_thread,
                                     vector<DataPublisher *> &workerClients, int numberOfPartitions,
                                     SQLiteDBInterface *sqlite, bool *loop_exit_p);
static void addStreamHDFSCommand(std::string masterIP, int connFd, std::string &hdfsServerIp,
                                 std::thread &inputStreamHandlerThread, int numberOfPartitions,
                                 SQLiteDBInterface *sqlite, bool *loop_exit_p);
static void stop_stream_kafka_command(int connFd, KafkaConnector *kstream, bool *loop_exit_p);
static void process_dataset_command(int connFd, bool *loop_exit_p);
static void triangles_command(std::string masterIP, int connFd, SQLiteDBInterface *sqlite,
                              PerformanceSQLiteDBInterface *perfSqlite, JobScheduler *jobScheduler, bool *loop_exit_p);
static void streaming_triangles_command(std::string masterIP, int connFd, JobScheduler *jobScheduler, bool *loop_exit_p,
                                        int numberOfPartitions, bool *strian_exit);
static void stop_strian_command(int connFd, bool *strian_exit);
static void vertex_count_command(int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p);
static void edge_count_command(int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p);
static void merge_command(int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p);
static void train_command(int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p);
static void in_degree_command(int connFd, bool *loop_exit_p);
static void out_degree_command(int connFd, bool *loop_exit_p);
static void page_rank_command(std::string masterIP, int connFd, SQLiteDBInterface *sqlite,
                              PerformanceSQLiteDBInterface *perfSqlite, JobScheduler *jobScheduler, bool *loop_exit_p);
static void egonet_command(int connFd, bool *loop_exit_p);
static void duplicate_centralstore_command(int connFd, bool *loop_exit_p);
static void predict_command(std::string masterIP, int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p);
static void start_remote_worker_command(int connFd, bool *loop_exit_p);
static void sla_command(int connFd, SQLiteDBInterface *sqlite, PerformanceSQLiteDBInterface *perfSqlite,
                        bool *loop_exit_p);
std::map<int, std::shared_ptr<::KGConstructionRate>> JasmineGraphFrontEnd::kgConstructionRates = {};
static vector<DataPublisher *> getWorkerClients(SQLiteDBInterface *sqlite) {
    const vector<Utils::worker> &workerList = Utils::getWorkerList(sqlite);
    vector<DataPublisher *> workerClients;
    for (int i = 0; i < workerList.size(); i++) {
        Utils::worker currentWorker = workerList.at(i);
        string workerHost = currentWorker.hostname;
        int workerDataPort = std::stoi(currentWorker.dataPort);
        int workerPort = atoi(string(currentWorker.port).c_str());
        DataPublisher *workerClient = new DataPublisher(workerPort, workerHost, workerDataPort);
        workerClients.push_back(workerClient);
    }
    return workerClients;
}
static size_t WriteCallback(void *contents, size_t size, size_t nmemb, std::string *output) {
    size_t totalSize = size * nmemb;
    output->append((char *)contents, totalSize);
    return totalSize;
}

void *frontendservicesesion(void *dummyPt) {
    frontendservicesessionargs *sessionargs = (frontendservicesessionargs *)dummyPt;
    std::string masterIP = sessionargs->masterIP;
    int connFd = sessionargs->connFd;
    SQLiteDBInterface *sqlite = sessionargs->sqlite;
    PerformanceSQLiteDBInterface *perfSqlite = sessionargs->perfSqlite;
    JobScheduler *jobScheduler = sessionargs->jobScheduler;
    delete sessionargs;

    if (JasmineGraphFrontEndCommon::checkServerBusy(&currentFESession, connFd)) {
        frontend_logger.error("Server is busy");
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

    // Initiate HDFS parameters
    std::string hdfsServerIp;
    hdfsFS fileSystem;

    vector<DataPublisher *> workerClients;

    bool workerClientsInitialized = false;

    bool loop_exit = false;
    int failCnt = 0;
    while (!loop_exit) {
        std::string line = JasmineGraphFrontEndCommon::readAndProcessInput(connFd, data, failCnt);
        if (line.empty()) {
            continue;
        }
        frontend_logger.info("Command received: " + line);
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
        } else if (line.compare(CYPHER) == 0) {
            workerClients = getWorkerClients(sqlite);
            workerClientsInitialized = true;
            cypherCommand(masterIP, connFd, workerClients, numberOfPartitions, &loop_exit, sqlite, perfSqlite,
                          jobScheduler);
        } else if (line.compare(SEMANTIC_BEAM_SEARCH) == 0) {
            workerClients = getWorkerClients(sqlite);
            workerClientsInitialized = true;
            semanticBeamSearch(masterIP, connFd, workerClients, numberOfPartitions, &loop_exit, sqlite, perfSqlite,
                               jobScheduler);
        } else if (line.compare(SHTDN) == 0) {
            JasmineGraphServer::shutdown_workers();
            close(connFd);
            exit(0);
        } else if (line.compare(ADRDF) == 0) {
            add_rdf_command(masterIP, connFd, sqlite, &loop_exit);
        } else if (line.compare(ADGR) == 0) {
            add_graph_command(masterIP, connFd, sqlite, &loop_exit);
        } else if (line.compare(ADMDL) == 0) {
            add_model_command(connFd, sqlite, &loop_exit);
        } else if (line.compare(ADGR_CUST) == 0) {
            add_graph_cust_command(masterIP, connFd, sqlite, &loop_exit);
        } else if (line.compare(ADD_STREAM_KAFKA) == 0) {
            if (!workerClientsInitialized) {
                workerClients = getWorkerClients(sqlite);
                workerClientsInitialized = true;
            }
            add_stream_kafka_command(connFd, kafka_server_IP, configs, kstream, input_stream_handler, workerClients,
                                     numberOfPartitions, sqlite, &loop_exit);
        } else if (line.compare(ADD_STREAM_HDFS) == 0) {
            addStreamHDFSCommand(masterIP, connFd, hdfsServerIp, input_stream_handler, numberOfPartitions, sqlite,
                                 &loop_exit);
        } else if (line.compare(CONSTRUCT_KG) == 0) {
            JasmineGraphFrontEnd::constructKGStreamHDFSCommand(masterIP, connFd, numberOfPartitions, sqlite,
                                                               &loop_exit);
        } else if (line.compare(STOP_CONSTRUCT_KG) == 0) {
            JasmineGraphFrontEnd::stop_graph_streaming(connFd, &loop_exit);
        } else if (line.compare(STOP_STREAM_KAFKA) == 0) {
            stop_stream_kafka_command(connFd, kstream, &loop_exit);
        } else if (line.compare(RMGR) == 0) {
            remove_graph_command(masterIP, connFd, sqlite, &loop_exit);
        } else if (line.compare(PROCESS_DATASET) == 0) {
            process_dataset_command(connFd, &loop_exit);
        } else if (line.compare(TRIANGLES) == 0) {
            triangles_command(masterIP, connFd, sqlite, perfSqlite, jobScheduler, &loop_exit);
        } else if (line.compare(STREAMING_TRIANGLES) == 0) {
            streaming_triangles_command(masterIP, connFd, jobScheduler, &loop_exit, numberOfPartitions,
                                        &JasmineGraphFrontEnd::strian_exit);
        } else if (line.compare(STOP_STRIAN) == 0) {
            stop_strian_command(connFd, &JasmineGraphFrontEnd::strian_exit);
        } else if (line.compare(VCOUNT) == 0) {
            vertex_count_command(connFd, sqlite, &loop_exit);
        } else if (line.compare(ECOUNT) == 0) {
            edge_count_command(connFd, sqlite, &loop_exit);
        } else if (line.compare(MERGE) == 0) {
            merge_command(connFd, sqlite, &loop_exit);
        } else if (line.compare(TRAIN) == 0) {
            train_command(connFd, sqlite, &loop_exit);
        } else if (line.compare(IN_DEGREE) == 0) {
            in_degree_command(connFd, &loop_exit);
        } else if (line.compare(OUT_DEGREE) == 0) {
            out_degree_command(connFd, &loop_exit);
        } else if (line.compare(PAGE_RANK) == 0) {
            page_rank_command(masterIP, connFd, sqlite, perfSqlite, jobScheduler, &loop_exit);
        } else if (line.compare(EGONET) == 0) {
            egonet_command(connFd, &loop_exit);
        } else if (line.compare(DPCNTRL) == 0) {
            duplicate_centralstore_command(connFd, &loop_exit);
        } else if (line.compare(PREDICT) == 0) {
            predict_command(masterIP, connFd, sqlite, &loop_exit);
        } else if (line.compare(START_REMOTE_WORKER) == 0) {
            start_remote_worker_command(connFd, &loop_exit);
        } else if (line.compare(SLA) == 0) {
            sla_command(connFd, sqlite, perfSqlite, &loop_exit);
        } else {
            frontend_logger.error("Message format not recognized " + line);
            int result_wr = write(connFd, INVALID_FORMAT.c_str(), INVALID_FORMAT.size());
            if (result_wr < 0) {
                frontend_logger.error("Error writing to socket");
                continue;
            }
        }
    }
    if (input_stream_handler.joinable()) {
        input_stream_handler.join();
    }
    frontend_logger.info("Closing thread " + to_string(pthread_self()) + " and connection");
    close(connFd);
    currentFESession--;
    return NULL;
}

JasmineGraphFrontEnd::JasmineGraphFrontEnd(SQLiteDBInterface *db, PerformanceSQLiteDBInterface *perfDb,
                                           std::string masterIP, JobScheduler *jobScheduler) {
    this->sqlite = db;
    this->masterIP = masterIP;
    this->perfSqlite = perfDb;
    this->jobScheduler = jobScheduler;
}

int JasmineGraphFrontEnd::run() {
    int pId;
    int portNo = Conts::JASMINEGRAPH_FRONTEND_PORT;
    int listenFd;
    socklen_t len;
    bool loop = false;
    struct sockaddr_in svrAdd;
    struct sockaddr_in clntAdd;

    // create socket
    listenFd = socket(AF_INET, SOCK_STREAM, 0);

    if (listenFd < 0) {
        frontend_logger.error("Cannot open socket");
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
        frontend_logger.error("Cannot bind on port " + portNo);
        return 0;
    }

    listen(listenFd, MAX_PENDING_CONNECTIONS);

    std::vector<std::thread> threadVector;
    len = sizeof(clntAdd);

    int noThread = 0;

    while (true) {
        frontend_logger.info("Frontend Listening");

        // this is where client connects. svr will hang in this mode until client conn
        connFd = accept(listenFd, (struct sockaddr *)&clntAdd, &len);

        if (connFd < 0) {
            frontend_logger.error("Cannot accept connection");
            continue;
        }
        frontend_logger.info("Connection successful from " + std::string(inet_ntoa(clntAdd.sin_addr)));

        frontendservicesessionargs *sessionargs = new frontendservicesessionargs;
        sessionargs->masterIP = masterIP;
        sessionargs->connFd = connFd;
        sessionargs->sqlite = this->sqlite;
        sessionargs->perfSqlite = this->perfSqlite;
        sessionargs->jobScheduler = this->jobScheduler;
        pthread_t pt;
        pthread_create(&pt, NULL, frontendservicesesion, sessionargs);
        pthread_detach(pt);
    }
}

int JasmineGraphFrontEnd::getRunningHighPriorityTaskCount() {
    int taskCount = 0;

    std::set<ProcessInfo>::iterator processQueryIterator;
    for (processQueryIterator = processData.begin(); processQueryIterator != processData.end();
         ++processQueryIterator) {
        ProcessInfo processInformation = *processQueryIterator;

        if (processInformation.priority == Conts::HIGH_PRIORITY_DEFAULT_VALUE) {
            taskCount++;
        }
    }

    return taskCount;
}

/*
    Method to check if all the running jobs are for the same graph
*/
bool JasmineGraphFrontEnd::areRunningJobsForSameGraph() {
    if (processData.empty()) {
        return true;  // No running jobs
    }

    std::string commonGraphId;  // To store the common graph ID among running jobs
    bool firstJob = true;       // To track if it's the first job being checked

    std::set<ProcessInfo>::iterator processQueryIterator;

    for (processQueryIterator = processData.begin(); processQueryIterator != processData.end();
         ++processQueryIterator) {
        ProcessInfo processInformation = *processQueryIterator;

        if (firstJob) {
            commonGraphId = processInformation.graphId;
            firstJob = false;
        } else {
            if (commonGraphId != processInformation.graphId) {
                // Graph IDs are not the same, so return false
                return false;
            }
        }
    }

    // All jobs have the same graph ID, so return true
    return true;
}

static void list_command(int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p) {
    std::stringstream ss;

    std::vector<vector<pair<string, string>>> graphData = JasmineGraphFrontEndCommon::getGraphData(sqlite);
    for (std::vector<vector<pair<string, string>>>::iterator i = graphData.begin(); i != graphData.end(); ++i) {
        ss << "|";
        int counter = 0;
        for (std::vector<pair<string, string>>::iterator j = (i->begin()); j != i->end(); ++j) {
            if (counter == 3) {
                if (std::stoi(j->second) == Conts::GRAPH_STATUS::LOADING) {
                    ss << "loading|";
                } else if (std::stoi(j->second) == Conts::GRAPH_STATUS::DELETING) {
                    ss << "deleting|";
                } else if (std::stoi(j->second) == Conts::GRAPH_STATUS::NONOPERATIONAL) {
                    ss << "nop|";
                } else if (std::stoi(j->second) == Conts::GRAPH_STATUS::OPERATIONAL) {
                    ss << "op|";
                }
                break;
            } else {
                ss << j->second << "|";
            }
            counter++;
        }
        ss << Conts::CARRIAGE_RETURN_NEW_LINE.c_str();
    }
    string result = ss.str();
    if (result.size() == 0) {
        int result_wr = write(connFd, EMPTY.c_str(), EMPTY.length());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }

        result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
    } else {
        int result_wr = write(connFd, result.c_str(), result.length());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
    }
}

void *frontendservicesesion(std::string masterIP, int connFd, SQLiteDBInterface *sqlite,
                            PerformanceSQLiteDBInterface *perfSqlite, JobScheduler *jobScheduler) {
}

static void cypherCommand(std::string masterIP, int connFd, vector<DataPublisher *> &workerClients,
                          int numberOfPartitions, bool *loop_exit, SQLiteDBInterface *sqlite,
                          PerformanceSQLiteDBInterface *perfSqlite, JobScheduler *jobScheduler) {
    string graphId = "Graph ID:";
    int result_wr = write(connFd, graphId.c_str(), graphId.length());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit = true;
        return;
    }
    char graphIdResponse[FRONTEND_DATA_LENGTH + 1];
    bzero(graphIdResponse, FRONTEND_DATA_LENGTH + 1);
    read(connFd, graphIdResponse, FRONTEND_DATA_LENGTH);
    string user_res_1(graphIdResponse);

    string queryInput = "Input query :";
    result_wr = write(connFd, queryInput.c_str(), queryInput.length());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit = true;
        return;
    }

    // Get user response.
    char query[FRONTEND_DATA_LENGTH + 1];
    bzero(query, FRONTEND_DATA_LENGTH + 1);
    read(connFd, query, FRONTEND_DATA_LENGTH);
    string queryString(query);

    auto begin = chrono::high_resolution_clock::now();

    JobRequest jobDetails;
    int uid = JasmineGraphFrontEndCommon::getUid();
    jobDetails.setJobId(std::to_string(uid));
    jobDetails.setJobType(CYPHER);
    jobDetails.addParameter(Conts::PARAM_KEYS::CYPHER_QUERY::QUERY_STRING, queryString);

    long graphSLA = -1;
    int threadPriority = Conts::HIGH_PRIORITY_DEFAULT_VALUE;
    graphSLA = JasmineGraphFrontEndCommon::getSLAForGraphId(sqlite, perfSqlite, graphIdResponse, CYPHER,
                                                            Conts::SLA_CATEGORY::LATENCY);
    jobDetails.addParameter(Conts::PARAM_KEYS::GRAPH_SLA, std::to_string(graphSLA));

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
            frontend_logger.error("Can't calibrate the graph now");
        }
    }

    jobDetails.setPriority(threadPriority);
    jobDetails.setMasterIP(masterIP);
    jobDetails.addParameter(Conts::PARAM_KEYS::GRAPH_ID, graphIdResponse);
    jobDetails.addParameter(Conts::PARAM_KEYS::CATEGORY, Conts::SLA_CATEGORY::LATENCY);
    jobDetails.addParameter(Conts::PARAM_KEYS::NO_OF_PARTITIONS, std::to_string(numberOfPartitions));
    jobDetails.addParameter(Conts::PARAM_KEYS::CONN_FILE_DESCRIPTOR, std::to_string(connFd));
    jobDetails.addParameter(Conts::PARAM_KEYS::LOOP_EXIT_POINTER,
                            std::to_string(reinterpret_cast<std::uintptr_t>(loop_exit)));

    if (canCalibrate) {
        jobDetails.addParameter(Conts::PARAM_KEYS::CAN_CALIBRATE, "true");
    } else {
        jobDetails.addParameter(Conts::PARAM_KEYS::CAN_CALIBRATE, "false");
    }

    jobScheduler->pushJob(jobDetails);
    frontend_logger.info("Job pushed");

    JobResponse jobResponse = jobScheduler->getResult(jobDetails);
    std::string errorMessage = jobResponse.getParameter(Conts::PARAM_KEYS::ERROR_MESSAGE);

    if (!errorMessage.empty()) {
        *loop_exit = true;
        result_wr = write(connFd, errorMessage.c_str(), errorMessage.length());

        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            return;
        }
        result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
        }
        return;
    }

    if (threadPriority == Conts::HIGH_PRIORITY_DEFAULT_VALUE) {
        highPriorityTaskCount--;
    }

    auto end = chrono::high_resolution_clock::now();
    auto dur = end - begin;
    auto msDuration = std::chrono::duration_cast<std::chrono::milliseconds>(dur).count();
    frontend_logger.info("Time Taken for query execution: " + to_string(msDuration) + " milliseconds");

    result_wr = write(connFd, DONE.c_str(), FRONTEND_COMMAND_LENGTH);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit = true;
    }
}
static void semanticBeamSearch(std::string masterIP, int connFd, vector<DataPublisher *> &workerClients,
                               int numberOfPartitions, bool *loop_exit, SQLiteDBInterface *sqlite,
                               PerformanceSQLiteDBInterface *perfSqlite, JobScheduler *jobScheduler) {
    frontend_logger.info("Requesting Graph ID from client");
    string graphId = "Graph ID:";
    int result_wr = write(connFd, graphId.c_str(), graphId.length());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit = true;
        return;
    }
    char graphIdResponse[FRONTEND_DATA_LENGTH + 1];
    memset(graphIdResponse, 0,  FRONTEND_DATA_LENGTH + 1);
    read(connFd, graphIdResponse, FRONTEND_DATA_LENGTH);
    string user_res_1(graphIdResponse);
    frontend_logger.info("Graph ID received: " + user_res_1);

    frontend_logger.info("Requesting query input from client");
    string queryInput = "Input query :";
    result_wr = write(connFd, queryInput.c_str(), queryInput.length());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit = true;
        return;
    }

    // Get user response.
    char query[FRONTEND_DATA_LENGTH + 1];
    memset(query, 0,  FRONTEND_DATA_LENGTH + 1);
    read(connFd, query, FRONTEND_DATA_LENGTH);
    string queryString(query);
    frontend_logger.info("Query received: " + queryString);

    auto begin = chrono::high_resolution_clock::now();
    JobRequest jobDetails;
    int uid = JasmineGraphFrontEndCommon::getUid();
    jobDetails.setJobId(std::to_string(uid));
    jobDetails.setJobType(SEMANTIC_BEAM_SEARCH);
    jobDetails.addParameter(Conts::PARAM_KEYS::CYPHER_QUERY::QUERY_STRING, queryString);

    long graphSLA = -1;
    int threadPriority = Conts::HIGH_PRIORITY_DEFAULT_VALUE;
    graphSLA = JasmineGraphFrontEndCommon::getSLAForGraphId(sqlite, perfSqlite, graphIdResponse, CYPHER,
                                                            Conts::SLA_CATEGORY::LATENCY);

    jobDetails.addParameter(Conts::PARAM_KEYS::GRAPH_SLA, std::to_string(graphSLA));

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
            frontend_logger.error("Can't calibrate the graph now");
        }
    }

    jobDetails.setPriority(threadPriority);
    jobDetails.setMasterIP(masterIP);
    jobDetails.addParameter(Conts::PARAM_KEYS::GRAPH_ID, graphIdResponse);
    jobDetails.addParameter(Conts::PARAM_KEYS::CATEGORY, Conts::SLA_CATEGORY::LATENCY);
    jobDetails.addParameter(Conts::PARAM_KEYS::NO_OF_PARTITIONS, std::to_string(numberOfPartitions));
    jobDetails.addParameter(Conts::PARAM_KEYS::CONN_FILE_DESCRIPTOR, std::to_string(connFd));
    jobDetails.addParameter(Conts::PARAM_KEYS::LOOP_EXIT_POINTER,
                            std::to_string(reinterpret_cast<std::uintptr_t>(loop_exit)));

    if (canCalibrate) {
        jobDetails.addParameter(Conts::PARAM_KEYS::CAN_CALIBRATE, "true");
    } else {
        jobDetails.addParameter(Conts::PARAM_KEYS::CAN_CALIBRATE, "false");
    }

    jobScheduler->pushJob(jobDetails);
    frontend_logger.info("Job pushed");

    JobResponse jobResponse = jobScheduler->getResult(jobDetails);
    std::string errorMessage = jobResponse.getParameter(Conts::PARAM_KEYS::ERROR_MESSAGE);

    if (!errorMessage.empty()) {
        *loop_exit = true;
        result_wr = write(connFd, errorMessage.c_str(), errorMessage.length());

        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            return;
        }
        result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
        }
        return;
    }

    if (threadPriority == Conts::HIGH_PRIORITY_DEFAULT_VALUE) {
        highPriorityTaskCount--;
    }

    auto end = chrono::high_resolution_clock::now();
    auto dur = end - begin;
    auto msDuration = std::chrono::duration_cast<std::chrono::milliseconds>(dur).count();
    frontend_logger.info("Time Taken for query execution: " + to_string(msDuration) + " milliseconds");

    result_wr = write(connFd, DONE.c_str(), FRONTEND_COMMAND_LENGTH);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit = true;
    }
}
static void add_rdf_command(std::string masterIP, int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p) {
    // add RDF graph
    int result_wr = write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    // We get the name and the path to graph as a pair separated by |.
    char graph_data[FRONTEND_DATA_LENGTH + 1];
    bzero(graph_data, FRONTEND_DATA_LENGTH + 1);
    string name = "";
    string path = "";

    read(connFd, graph_data, FRONTEND_DATA_LENGTH);

    std::time_t time = chrono::system_clock::to_time_t(chrono::system_clock::now());
    string uploadStartTime = ctime(&time);
    string gData(graph_data);

    gData = Utils::trim_copy(gData);
    frontend_logger.info("Data received: " + gData);

    std::vector<std::string> strArr = Utils::split(gData, '|');

    if (strArr.size() != 2) {
        frontend_logger.error("Message format not recognized");
        result_wr = write(connFd, INVALID_FORMAT.c_str(), INVALID_FORMAT.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
        return;
    }

    name = strArr[0];
    path = strArr[1];

    if (JasmineGraphFrontEndCommon::graphExists(path, sqlite)) {
        frontend_logger.error("Graph exists");
        result_wr = write(connFd, INVALID_FORMAT.c_str(), INVALID_FORMAT.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
        return;
    }

    if (Utils::fileExists(path)) {
        frontend_logger.info("Path exists");

        string sqlStatement =
            "INSERT INTO graph (name,upload_path,upload_start_time,upload_end_time,graph_status_idgraph_status,"
            "vertexcount,centralpartitioncount,edgecount) VALUES(\"" +
            name + "\", \"" + path + "\", \"" + uploadStartTime + "\", \"\",\"" +
            to_string(Conts::GRAPH_STATUS::LOADING) + "\", \"\", \"\", \"\")";
        int newGraphID = sqlite->runInsert(sqlStatement);

        GetConfig appConfig;
        appConfig.readConfigFile(path, newGraphID);

        MetisPartitioner metisPartitioner(sqlite);
        vector<std::map<int, string>> fullFileList;
        string input_file_path =
            Utils::getHomeDir() + "/.jasminegraph/tmp/" + to_string(newGraphID) + "/" + to_string(newGraphID);
        metisPartitioner.loadDataSet(input_file_path, newGraphID);

        metisPartitioner.constructMetisFormat(Conts::GRAPH_TYPE_RDF);
        fullFileList = metisPartitioner.partitioneWithGPMetis("");
        JasmineGraphServer *server = JasmineGraphServer::getInstance();
        server->uploadGraphLocally(newGraphID, Conts::GRAPH_WITH_ATTRIBUTES, fullFileList, masterIP);
        Utils::deleteDirectory(Utils::getHomeDir() + "/.jasminegraph/tmp/" + to_string(newGraphID));
        Utils::deleteDirectory("/tmp/" + std::to_string(newGraphID));
        JasmineGraphFrontEndCommon::getAndUpdateUploadTime(to_string(newGraphID), sqlite);
        int result_wr = write(connFd, DONE.c_str(), DONE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
    } else {
        frontend_logger.error("Graph data file does not exist on the specified path");
    }
}

static void add_graph_command(std::string masterIP, int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p) {
    int result_wr = write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    // We get the name and the path to graph as a pair separated by |.
    char graph_data[FRONTEND_DATA_LENGTH + 1];
    char partition_count[FRONTEND_DATA_LENGTH + 1];
    bzero(graph_data, FRONTEND_DATA_LENGTH + 1);
    string name = "";
    string path = "";
    string partitionCount = "";

    read(connFd, graph_data, FRONTEND_DATA_LENGTH);

    std::time_t time = chrono::system_clock::to_time_t(chrono::system_clock::now());
    string uploadStartTime = ctime(&time);
    string gData(graph_data);

    gData = Utils::trim_copy(gData);
    frontend_logger.info("Data received: " + gData);

    std::vector<std::string> strArr = Utils::split(gData, '|');

    if (strArr.size() < 2) {
        frontend_logger.error("Message format not recognized");
        // TODO: inform client?
        return;
    }

    name = strArr[0];
    path = strArr[1];

    partitionCount = JasmineGraphFrontEndCommon::getPartitionCount(path);

    if (JasmineGraphFrontEndCommon::graphExists(path, sqlite)) {
        frontend_logger.error("Graph exists");
        // TODO: inform client?
        return;
    }

    if (Utils::fileExists(path)) {
        frontend_logger.info("Path exists");

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
        frontend_logger.info("Upload done");
        JasmineGraphServer *server = JasmineGraphServer::getInstance();
        server->uploadGraphLocally(newGraphID, Conts::GRAPH_TYPE_NORMAL, fullFileList, masterIP);
        Utils::deleteDirectory(Utils::getHomeDir() + "/.jasminegraph/tmp/" + to_string(newGraphID));
        JasmineGraphFrontEndCommon::getAndUpdateUploadTime(to_string(newGraphID), sqlite);
        int result_wr = write(connFd, DONE.c_str(), DONE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
    } else {
        frontend_logger.error("Graph data file does not exist on the specified path");
    }
}

static void add_graph_cust_command(std::string masterIP, int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p) {
    string message = "Select a custom graph upload option\r\n";
    int result_wr = write(connFd, message.c_str(), message.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::GRAPH_WITH::TEXT_ATTRIBUTES.c_str(), Conts::GRAPH_WITH::TEXT_ATTRIBUTES.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::GRAPH_WITH::JSON_ATTRIBUTES.c_str(), Conts::GRAPH_WITH::JSON_ATTRIBUTES.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::GRAPH_WITH::XML_ATTRIBUTES.c_str(), Conts::GRAPH_WITH::XML_ATTRIBUTES.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());

    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    char type[FRONTEND_GRAPH_TYPE_LENGTH + 1];
    bzero(type, FRONTEND_GRAPH_TYPE_LENGTH + 1);
    read(connFd, type, FRONTEND_GRAPH_TYPE_LENGTH);
    string graphType(type);
    graphType = Utils::trim_copy(graphType);

    std::unordered_set<std::string> s = {"1", "2", "3"};
    if (s.find(graphType) == s.end()) {
        frontend_logger.error("Graph type not recognized");
        // TODO: inform client?
        return;
    }

    string graphAttributeType = "";
    if (graphType == "1") {
        graphAttributeType = Conts::GRAPH_WITH_TEXT_ATTRIBUTES;
    } else if (graphType == "2") {
        graphAttributeType = Conts::GRAPH_WITH_JSON_ATTRIBUTES;
    } else if (graphType == "3") {
        graphAttributeType = Conts::GRAPH_WITH_XML_ATTRIBUTES;
    }

    // We get the name and the path to graph edge list and attribute list as a triplet separated by | .
    // <name>|<path to edge list>|<path to attribute file>|(optional)<attribute data type: int8. int16, int32 or
    // float> Data types based on numpy array data types for numerical values with int8 referring to 8bit
    // integers etc. If data type is not specified, it will be inferred from values present in the first line of
    // the attribute file The provided data type should be the largest in the following order: float > int32 >
    // int16 > int8 Inferred data type will be the largest type based on the values present in the attribute
    // file first line
    message =
        "Send <name>|<path to edge list>|<path to attribute file>|(optional)<attribute data type: int8. int16, "
        "int32 or float>\r\n";
    result_wr = write(connFd, message.c_str(), message.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    char graph_data[FRONTEND_DATA_LENGTH + 1];
    bzero(graph_data, FRONTEND_DATA_LENGTH + 1);
    string name = "";
    string edgeListPath = "";
    string attributeListPath = "";
    string attrDataType = "";

    read(connFd, graph_data, FRONTEND_DATA_LENGTH);

    std::time_t time = chrono::system_clock::to_time_t(chrono::system_clock::now());
    string uploadStartTime = ctime(&time);
    string gData(graph_data);

    gData = Utils::trim_copy(gData);
    frontend_logger.info("Data received: " + gData);

    std::vector<std::string> strArr = Utils::split(gData, '|');

    if (strArr.size() != 3 && strArr.size() != 4) {
        frontend_logger.error("Message format not recognized");
        // TODO: inform client?
        return;
    }

    name = strArr[0];
    edgeListPath = strArr[1];
    attributeListPath = strArr[2];
    // If data type is specified
    if (strArr.size() == 4) {
        attrDataType = strArr[3];
        if (attrDataType != "int8" && attrDataType != "int16" && attrDataType != "int32" && attrDataType != "float") {
            frontend_logger.error("Data type not recognized");
            // TODO: inform client?
            return;
        }
    }

    if (JasmineGraphFrontEndCommon::graphExists(edgeListPath, sqlite)) {
        frontend_logger.error("Graph exists");
        // TODO: inform client?
        return;
    }

    if (Utils::fileExists(edgeListPath) && Utils::fileExists(attributeListPath)) {
        frontend_logger.info("Paths exists");

        string sqlStatement =
            "INSERT INTO graph (name,upload_path,upload_start_time,upload_end_time,graph_status_idgraph_status,"
            "vertexcount,centralpartitioncount,edgecount) VALUES(\"" +
            name + "\", \"" + edgeListPath + "\", \"" + uploadStartTime + "\", \"\",\"" +
            to_string(Conts::GRAPH_STATUS::LOADING) + "\", \"\", \"\", \"\")";
        int newGraphID = sqlite->runInsert(sqlStatement);
        MetisPartitioner partitioner(sqlite);
        vector<std::map<int, string>> fullFileList;
        partitioner.loadContentData(attributeListPath, graphAttributeType, newGraphID, attrDataType);
        partitioner.loadDataSet(edgeListPath, newGraphID);
        int result = partitioner.constructMetisFormat(Conts::GRAPH_TYPE_NORMAL);
        if (result == 0) {
            string reformattedFilePath = partitioner.reformatDataSet(edgeListPath, newGraphID);
            partitioner.loadDataSet(reformattedFilePath, newGraphID);
            partitioner.constructMetisFormat(Conts::GRAPH_TYPE_NORMAL_REFORMATTED);
        }
        fullFileList = partitioner.partitioneWithGPMetis("");

        // Graph type should be changed to identify graphs with attributes
        // because this graph type has additional attribute files to be uploaded
        JasmineGraphServer *server = JasmineGraphServer::getInstance();
        server->uploadGraphLocally(newGraphID, Conts::GRAPH_WITH_ATTRIBUTES, fullFileList, masterIP);
        Utils::deleteDirectory(Utils::getHomeDir() + "/.jasminegraph/tmp/" + to_string(newGraphID));
        Utils::deleteDirectory("/tmp/" + std::to_string(newGraphID));
        JasmineGraphFrontEndCommon::getAndUpdateUploadTime(to_string(newGraphID), sqlite);
        result_wr = write(connFd, DONE.c_str(), DONE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
    } else {
        frontend_logger.error("Graph data file does not exist on the specified path");
    }
}

static void remove_graph_command(std::string masterIP, int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p) {
    int result_wr = write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    // We get the name and the path to graph as a pair separated by |.
    char graph_id[FRONTEND_DATA_LENGTH + 1];
    bzero(graph_id, FRONTEND_DATA_LENGTH + 1);
    string name = "";
    string path = "";

    read(connFd, graph_id, FRONTEND_DATA_LENGTH);

    string graphID(graph_id);

    graphID = Utils::trim_copy(graphID);
    frontend_logger.info("Graph ID received: " + graphID);

    if (JasmineGraphFrontEndCommon::graphExistsByID(graphID, sqlite)) {
        frontend_logger.info("Graph with ID " + graphID + " is being deleted now");
        JasmineGraphFrontEndCommon::removeGraph(graphID, sqlite, masterIP);
        result_wr = write(connFd, DONE.c_str(), DONE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
    } else {
        frontend_logger.error("Graph does not exist or cannot be deleted with the current hosts setting");
        result_wr = write(connFd, ERROR.c_str(), ERROR.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
    }
}

static void add_model_command(int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p) {
    // TODO add error handling
    int result_wr = write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    char graph_data[FRONTEND_DATA_LENGTH + 1];
    bzero(graph_data, FRONTEND_DATA_LENGTH + 1);
    string name = "";
    string path = "";

    read(connFd, graph_data, FRONTEND_DATA_LENGTH);

    std::time_t time = chrono::system_clock::to_time_t(chrono::system_clock::now());
    string uploadStartTime = ctime(&time);
    string gData(graph_data);

    gData = Utils::trim_copy(gData);
    frontend_logger.info("Data received: " + gData);

    std::vector<std::string> strArr = Utils::split(gData, '|');

    if (strArr.size() < 2) {
        frontend_logger.error("Message format not recognized");
        // TODO: inform client?
        return;
    }

    name = strArr[0];
    path = strArr[1];

    if (JasmineGraphFrontEndCommon::modelExists(path, sqlite)) {
        frontend_logger.error("Model exists");
        // TODO: inform client?
        return;
    }

    if (Utils::fileExists(path)) {
        frontend_logger.info("Path exists");
        std::string toDir = Utils::getJasmineGraphProperty("org.jasminegraph.server.modelDir");
        Utils::copyToDirectory(path, toDir);

        string sqlStatement =
            "INSERT INTO model (name,upload_path,upload_time,model_status_idmodel_status"
            ")VALUES(\"" +
            name + "\", \"" + path + "\", \"" + uploadStartTime + "\",\"" + to_string(Conts::GRAPH_STATUS::LOADING) +
            "\")";

        int newModelID = sqlite->runInsert(sqlStatement);

        frontend_logger.info("Upload done");
        result_wr = write(connFd, DONE.c_str(), DONE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
    } else {
        frontend_logger.error("Model file does not exist on the specified path");
    }
}

static void add_stream_kafka_command(int connFd, std::string &kafka_server_IP, cppkafka::Configuration &configs,
                                     KafkaConnector *&kstream, thread &input_stream_handler_thread,
                                     vector<DataPublisher *> &workerClients, int numberOfPartitions,
                                     SQLiteDBInterface *sqlite, bool *loop_exit_p) {
    string exist = "Do you want to stream into existing graph(y/n) ? ";
    int result_wr = write(connFd, exist.c_str(), exist.length());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    // Get user response.
    string existingGraph = Utils::getFrontendInput(connFd);
    string graphId;
    string partitionAlgo;
    string direction;

    if (existingGraph == "y") {
        string existingGraphIdMsg = "Send the existing graph ID ? ";
        result_wr = write(connFd, existingGraphIdMsg.c_str(), existingGraphIdMsg.length());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        // Get user response.
        string existingGraphId = Utils::getFrontendInput(connFd);

        bool isExist = sqlite->isGraphIdExist(existingGraphId);
        if (!isExist) {
            string errorMsg = "Error: Graph ID you entered is not in the system";
            result_wr = write(connFd, errorMsg.c_str(), errorMsg.length());
            if (result_wr < 0) {
                frontend_logger.error("Error writing to socket");
                *loop_exit_p = true;
                return;
            }
            return;
        }
        string existingSuccessMsg = "Set data streaming into graph ID: " + existingGraphId;
        result_wr = write(connFd, existingSuccessMsg.c_str(), existingSuccessMsg.length());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        graphId = existingGraphId;
        partitionAlgo = sqlite->getPartitionAlgoByGraphID(graphId);

    } else {
        int nextID = sqlite->getNextGraphId();
        if (nextID < 0) {
            return;
        }
        graphId = to_string(nextID);
        string defaultIdMsg = "Do you use default graph ID: " + graphId + "(y/n) ? ";
        result_wr = write(connFd, defaultIdMsg.c_str(), defaultIdMsg.length());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        // Get user response.
        string isDefaultGraphId = Utils::getFrontendInput(connFd);

        if (isDefaultGraphId != "y") {
            string inputGraphIdMsg = "Input your graph ID: ";
            result_wr = write(connFd, inputGraphIdMsg.c_str(), inputGraphIdMsg.length());
            if (result_wr < 0) {
                frontend_logger.error("Error writing to socket");
                *loop_exit_p = true;
                return;
            }

            // Get user response.
            string userGraphId = Utils::getFrontendInput(connFd);

            bool isExist = sqlite->isGraphIdExist(userGraphId);
            if (isExist) {
                string errorMsg = "Error: Graph ID you entered already exists";
                result_wr = write(connFd, errorMsg.c_str(), errorMsg.length());
                if (result_wr < 0) {
                    frontend_logger.error("Error writing to socket");
                    *loop_exit_p = true;
                    return;
                }
                return;
            }

            string userGraphIdSuccessMsg = "Set graph ID successfully";
            result_wr = write(connFd, userGraphIdSuccessMsg.c_str(), userGraphIdSuccessMsg.length());
            if (result_wr < 0) {
                frontend_logger.error("Error writing to socket");
                *loop_exit_p = true;
                return;
            }
            result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
            if (result_wr < 0) {
                frontend_logger.error("Error writing to socket");
                *loop_exit_p = true;
                return;
            }
            graphId = userGraphId;
        }

        std::string partitionSelectionMsg =
            "Select the partitioning technique\n"
            "\toption 1: Hash partitioning\n"
            "\toption 2: Fennel partitioning\n"
            "\toption 3: LDG partitioning\n"
            "Choose an option(1,2,3): ";
        result_wr = write(connFd, partitionSelectionMsg.c_str(), partitionSelectionMsg.length());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        // Get user response.
        string partitionAlgoInput = Utils::getFrontendInput(connFd);

        if (partitionAlgoInput == "1" || partitionAlgoInput == "2" || partitionAlgoInput == "3") {
            string partition_success_msg = "Set partition technique: " + partitionAlgoInput;
            result_wr = write(connFd, partition_success_msg.c_str(), partition_success_msg.length());
            if (result_wr < 0) {
                frontend_logger.error("Error writing to socket");
                *loop_exit_p = true;
                return;
            }
            result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
            if (result_wr < 0) {
                frontend_logger.error("Error writing to socket");
                *loop_exit_p = true;
                return;
            }
            partitionAlgo = partitionAlgoInput;
        } else {
            string errorMsg = "Error: invalid partition option: " + partitionAlgoInput;
            result_wr = write(connFd, errorMsg.c_str(), errorMsg.length());
            if (result_wr < 0) {
                frontend_logger.error("Error writing to socket");
                *loop_exit_p = true;
                return;
            }
            return;
        }

        string checkDirection = "Is this graph Directed (y/n)? ";
        result_wr = write(connFd, checkDirection.c_str(), checkDirection.length());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        // Get user response.
        string isDirected = Utils::getFrontendInput(connFd);
        if (isDirected == "y") {
            direction = Conts::DIRECTED;
        } else {
            direction = Conts::UNDIRECTED;
        }

        string checkGraphType = "Graph type received";
        result_wr = write(connFd, checkGraphType.c_str(), checkGraphType.length());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
    }

    string msg_1 = "Do you want to use default KAFKA consumer(y/n) ?";
    result_wr = write(connFd, msg_1.c_str(), msg_1.length());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    // Get user response.
    string default_kafka = Utils::getFrontendInput(connFd);
    //          use default kafka consumer details
    string group_id =  Conts::KAFKA_GROUP_ID;  // TODO(sakeerthan): MOVE TO CONSTANT LATER
    if (default_kafka == "y") {
        kafka_server_IP = Utils::getJasmineGraphProperty("org.jasminegraph.server.streaming.kafka.host");
        configs = {
            {"metadata.broker.list", kafka_server_IP}, {"group.id", group_id}, {"auto.offset.reset", "earliest"}};
    } else {
        // user need to start relevant kafka cluster using relevant IP address
        // read relevant IP address from given file path
        string message = "Send file path to the kafka configuration file.";
        result_wr = write(connFd, message.c_str(), message.length());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }

        // We get the file path here.
        char file_path[FRONTEND_DATA_LENGTH + 1];
        bzero(file_path, FRONTEND_DATA_LENGTH + 1);
        read(connFd, file_path, FRONTEND_DATA_LENGTH);
        string file_path_s(file_path);
        file_path_s = Utils::trim_copy(file_path_s);
        // reading kafka_server IP from the given file.
        std::vector<std::string>::iterator it;
        vector<std::string> vec = Utils::getFileContent(file_path_s);
        for (it = vec.begin(); it < vec.end(); it++) {
            std::string item = *it;
            if (item.length() > 0 && !(item.rfind("#", 0) == 0)) {
                std::vector<std::string> vec2 = Utils::split(item, '=');
                if (vec2.at(0).compare("kafka.host") == 0) {
                    if (item.substr(item.length() - 1, item.length()).compare("=") != 0) {
                        std::string kafka_server_IP = vec2.at(1);
                    } else {
                        std::string kafka_server_IP = " ";
                    }
                }
            }
        }
        //              set the config according to given IP address
        configs = {
            {"metadata.broker.list", kafka_server_IP}, {"group.id", "knnect"}, {"auto.offset.reset", "earliest"}};
    }

    frontend_logger.info("Start serving `" + ADD_STREAM_KAFKA + "` command");
    string message = "send kafka topic name";
    result_wr = write(connFd, message.c_str(), message.length());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    // We get the topic name here.
    char topic_name[FRONTEND_DATA_LENGTH + 1];
    bzero(topic_name, FRONTEND_DATA_LENGTH + 1);
    read(connFd, topic_name, FRONTEND_DATA_LENGTH);
    string topic_name_s(topic_name);
    topic_name_s = Utils::trim_copy(topic_name_s);
    string con_message = "Received the kafka topic";
    int con_result_wr = write(connFd, con_message.c_str(), con_message.length());
    if (con_result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    // create kafka consumer and graph partitioner
    kstream = new KafkaConnector(configs);
    // Create the KafkaConnector object.
    kstream = new KafkaConnector(configs);
    // Subscribe to the Kafka topic.
    kstream->Subscribe(topic_name_s);
    // Create the StreamHandler object.
    StreamHandler *stream_handler = new StreamHandler(kstream, numberOfPartitions, workerClients, sqlite, stoi(graphId),
                                                      direction == Conts::DIRECTED, spt::getPartitioner(partitionAlgo));

    if (existingGraph != "y") {
        string path = "kafka:\\" + topic_name_s + ":" + group_id;
        std::time_t time = chrono::system_clock::to_time_t(chrono::system_clock::now());
        string uploadStartTime = ctime(&time);
        string sqlStatement =
            "INSERT INTO graph (idgraph,id_algorithm,name,upload_path, upload_start_time, upload_end_time,"
            "graph_status_idgraph_status, vertexcount, centralpartitioncount, edgecount, is_directed) VALUES(" +
            graphId + "," + partitionAlgo + ",\"" + topic_name_s + "\", \"" + path + "\", \"" + uploadStartTime +
            "\", \"\",\"" + to_string(Conts::GRAPH_STATUS::STREAMING) + "\", \"\"," + to_string(numberOfPartitions) +
            ", \"\",\"" + direction + "\")";
        int newGraphID = sqlite->runInsert(sqlStatement);
    } else {
        std::string sqlStatement =
            "UPDATE graph SET graph_status_idgraph_status =" + to_string(Conts::GRAPH_STATUS::STREAMING) +
            " WHERE idgraph = " + graphId;
        sqlite->runUpdate(sqlStatement);
    }
    frontend_logger.info("Start listening to " + topic_name_s);
    input_stream_handler_thread = thread(&StreamHandler::listen_to_kafka_topic, stream_handler);
}

void addStreamHDFSCommand(std::string masterIP, int connFd, std::string &hdfsServerIp,
                          std::thread &inputStreamHandlerThread, int numberOfPartitions, SQLiteDBInterface *sqlite,
                          bool *loop_exit_p) {
    std::string hdfsPort;
    std::string message1 = "Do you want to use the default HDFS server(y/n)?";
    int resultWr = write(connFd, message1.c_str(), message1.length());
    if (resultWr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (resultWr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    char userRes[FRONTEND_DATA_LENGTH + 1];
    bzero(userRes, FRONTEND_DATA_LENGTH + 1);
    read(connFd, userRes, FRONTEND_DATA_LENGTH);
    std::string userResS(userRes);
    userResS = Utils::trim_copy(userResS);
    for (char &c : userResS) {
        c = tolower(c);
    }

    if (userResS == "y") {
        hdfsServerIp = Utils::getJasmineGraphProperty("org.jasminegraph.server.streaming.hdfs.host");
        hdfsPort = Utils::getJasmineGraphProperty("org.jasminegraph.server.streaming.hdfs.port");
    } else {
        std::string message =
            "Send the file path to the HDFS configuration file. This file needs to be in some"
            " directory location that is accessible for JasmineGraph master";
        resultWr = write(connFd, message.c_str(), message.length());
        if (resultWr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        if (resultWr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }

        char filePath[FRONTEND_DATA_LENGTH + 1];
        bzero(filePath, FRONTEND_DATA_LENGTH + 1);
        read(connFd, filePath, FRONTEND_DATA_LENGTH);
        std::string filePathS(filePath);
        filePathS = Utils::trim_copy(filePathS);

        frontend_logger.info("Reading HDFS configuration file: " + filePathS);

        std::vector<std::string> vec = Utils::getFileContent(filePathS);
        for (const auto &item : vec) {
            if (item.length() > 0 && !(item.rfind("#", 0) == 0)) {
                std::vector<std::string> vec2 = Utils::split(item, '=');
                if (vec2.size() == 2) {
                    if (vec2.at(0).compare("hdfs.host") == 0) {
                        hdfsServerIp = vec2.at(1);
                    } else if (vec2.at(0).compare("hdfs.port") == 0) {
                        hdfsPort = vec2.at(1);
                    }
                } else {
                    frontend_logger.error("Invalid line in configuration file: " + item);
                }
            }
        }
    }

    if (hdfsServerIp.empty()) {
        frontend_logger.error("HDFS server IP is empty.");
    }
    if (hdfsPort.empty()) {
        frontend_logger.error("HDFS server port is empty.");
    }

    std::string message2 = "HDFS file path: ";
    resultWr = write(connFd, message2.c_str(), message2.length());
    if (resultWr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (resultWr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    char hdfsFilePath[FRONTEND_DATA_LENGTH + 1];
    bzero(hdfsFilePath, FRONTEND_DATA_LENGTH + 1);
    read(connFd, hdfsFilePath, FRONTEND_DATA_LENGTH);
    std::string hdfsFilePathS(hdfsFilePath);
    hdfsFilePathS = Utils::trim_copy(hdfsFilePathS);

    HDFSConnector *hdfsConnector = new HDFSConnector(hdfsServerIp, hdfsPort);

    if (!hdfsConnector->isPathValid(hdfsFilePathS)) {
        frontend_logger.error("Invalid HDFS file path: " + hdfsFilePathS);
        std::string error_message = "The provided HDFS path is invalid.";
        write(connFd, error_message.c_str(), error_message.length());
        write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        delete hdfsConnector;
        *loop_exit_p = true;
        return;
    }

    // get graph type
    bool isEdgeListType = false;
    std::string graphType = "Is this an edge list type graph(y/n)?";
    resultWr = write(connFd, graphType.c_str(), graphType.length());
    if (resultWr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (resultWr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    char isEdgeListTypeRes[FRONTEND_DATA_LENGTH + 1];
    bzero(isEdgeListTypeRes, FRONTEND_DATA_LENGTH + 1);
    read(connFd, isEdgeListTypeRes, FRONTEND_DATA_LENGTH);
    std::string isEdgeListTypeGraph(isEdgeListTypeRes);
    isEdgeListTypeGraph = Utils::trim_copy(isEdgeListTypeGraph);

    if (isEdgeListTypeGraph == "y") {
        isEdgeListType = true;
    }

    /*get directionality*/
    std::string isDirectedGraph = "Is this a directed graph(y/n)?";
    resultWr = write(connFd, isDirectedGraph.c_str(), isDirectedGraph.length());
    if (resultWr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (resultWr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    char isDirectedRes[FRONTEND_DATA_LENGTH + 1];
    bzero(isDirectedRes, FRONTEND_DATA_LENGTH + 1);
    read(connFd, isDirectedRes, FRONTEND_DATA_LENGTH);
    std::string isDirectedS(isDirectedRes);
    isDirectedS = Utils::trim_copy(isDirectedS);

    bool directed = false;
    if (isDirectedS == "y") {
        directed = true;
    }

    std::string path = "hdfs:" + hdfsFilePathS;

    std::time_t time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::string uploadStartTime = ctime(&time);
    std::string sqlStatement =
        "INSERT INTO graph (name, upload_path, upload_start_time, upload_end_time, graph_status_idgraph_status, "
        "vertexcount, centralpartitioncount, edgecount, is_directed) VALUES(\"" +
        hdfsFilePathS + "\", \"" + path + "\", \"" + uploadStartTime + "\", \"\", \"" +
        std::to_string(Conts::GRAPH_STATUS::NONOPERATIONAL) + "\", \"\", \"\", \"\", \"" +
        (directed ? "TRUE" : "FALSE") + "\")";

    int newGraphID = sqlite->runInsert(sqlStatement);
    frontend_logger.info("Created graph ID: " + std::to_string(newGraphID));
    HDFSStreamHandler *streamHandler =
        new HDFSStreamHandler(hdfsConnector->getFileSystem(), hdfsFilePathS, numberOfPartitions, newGraphID, sqlite,
                              masterIP, directed, isEdgeListType);
    frontend_logger.info("Started listening to " + hdfsFilePathS);
    inputStreamHandlerThread = std::thread(&HDFSStreamHandler::startStreamingFromBufferToPartitions, streamHandler);
    inputStreamHandlerThread.join();

    std::string uploadEndTime = ctime(&time);
    std::string sqlStatementUpdateEndTime =
        "UPDATE graph "
        "SET upload_end_time = \"" +
        uploadEndTime +
        "\" "
        "WHERE idgraph = " +
        std::to_string(newGraphID);
    sqlite->runInsert(sqlStatementUpdateEndTime);

    int conResultWr = write(connFd, DONE.c_str(), DONE.length());
    if (conResultWr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (resultWr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
}

bool JasmineGraphFrontEnd::constructKGStreamHDFSCommand(std::string masterIP, int connFd, int numberOfPartitions,
                                                        SQLiteDBInterface *sqlite, bool *loop_exit_p) {
    std::string hdfsPort;
    std::string hdfsServerIp;
    std::string message1 = "Do you want to use the default HDFS server(y/n)?";
    int resultWr = write(connFd, message1.c_str(), message1.length());
    if (resultWr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return false;
    }
    resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (resultWr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return false;
    }

    char userRes[FRONTEND_DATA_LENGTH + 1];
    memset(userRes, 0, FRONTEND_DATA_LENGTH + 1);
    read(connFd, userRes, FRONTEND_DATA_LENGTH);
    std::string userResS(userRes);
    userResS = Utils::trim_copy(userResS);
    for (char &c : userResS) {
        c = tolower(c);
    }

    if (userResS == "y") {
        hdfsServerIp = Utils::getJasmineGraphProperty("org.jasminegraph.server.streaming.hdfs.host");
        hdfsPort = Utils::getJasmineGraphProperty("org.jasminegraph.server.streaming.hdfs.port");
    } else {
        std::string hdfsIPMSG = "HDFS Server IP:";
        resultWr = write(connFd, hdfsIPMSG.c_str(), hdfsIPMSG.length());
        if (resultWr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return false;
        }
        resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        if (resultWr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return false;
        }

        char hdfsIP[FRONTEND_DATA_LENGTH + 1];
        memset(hdfsIP, 0, FRONTEND_DATA_LENGTH + 1);
        read(connFd, hdfsIP, FRONTEND_DATA_LENGTH);
        std::string hdfsIPS(hdfsIP);
        hdfsIPS = Utils::trim_copy(hdfsIPS);
        hdfsServerIp = hdfsIPS;

        std::string hdfsPortMSG = "HDFS Server Port:";
        resultWr = write(connFd, hdfsPortMSG.c_str(), hdfsPortMSG.length());
        if (resultWr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return false;
        }
        resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        if (resultWr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return false;
        }

        char hdfsPortChar[FRONTEND_DATA_LENGTH + 1];
        memset(hdfsPortChar, 0, FRONTEND_DATA_LENGTH + 1);
        read(connFd, hdfsPortChar, FRONTEND_DATA_LENGTH);
        std::string hdfsPortS(hdfsPortChar);
        hdfsPortS = Utils::trim_copy(hdfsPortS);
        hdfsPort = hdfsPortS;
    }
    frontend_logger.info("HDFS Server IP:" + hdfsServerIp);
    frontend_logger.info("HDFS Server Port:" + hdfsPort);
    if (hdfsServerIp.empty()) {
        frontend_logger.error("HDFS server IP is empty.");
    }
    if (hdfsPort.empty()) {
        frontend_logger.error("HDFS server port is empty.");
    }

    std::string message2 = "HDFS file path: ";
    resultWr = write(connFd, message2.c_str(), message2.length());
    if (resultWr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return false;
    }
    resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (resultWr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return false;
    }

    char hdfsFilePath[FRONTEND_DATA_LENGTH + 1];
    memset(hdfsFilePath, 0, FRONTEND_DATA_LENGTH + 1);
    read(connFd, hdfsFilePath, FRONTEND_DATA_LENGTH);
    std::string hdfsFilePathS(hdfsFilePath);
    hdfsFilePathS = Utils::trim_copy(hdfsFilePathS);

    HDFSConnector *hdfsConnector = new HDFSConnector(hdfsServerIp, hdfsPort);

    if (!hdfsConnector->isPathValid(hdfsFilePathS)) {
        frontend_logger.error("Invalid HDFS file path: " + hdfsFilePathS);
        std::string error_message = "The provided HDFS path is invalid.";
        write(connFd, error_message.c_str(), error_message.length());
        write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        delete hdfsConnector;
        *loop_exit_p = true;
        return false;
    }

    std::string path = "hdfs:" + hdfsFilePathS;
    double_t total_file_size = hdfsGetPathInfo(hdfsConnector->getFileSystem(), hdfsFilePathS.c_str())->mSize;
    std::time_t time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::string uploadStartTime = ctime(&time);

    // 2. Prepare new graph insertion
    std::time_t now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    uploadStartTime.erase(uploadStartTime.find_last_not_of(Conts::CARRIAGE_RETURN_NEW_LINE) + 1);  // remove newline

    std::string llmRunnerMSG = "LLM runner hostname:port: ";
    resultWr = write(connFd, llmRunnerMSG.c_str(), llmRunnerMSG.length());
    if (resultWr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return false;
    }
    resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (resultWr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return false;
    }

    char hostnamePort[FRONTEND_DATA_LENGTH + 1];
    memset(hostnamePort, 0, FRONTEND_DATA_LENGTH + 1);
    read(connFd, hostnamePort, FRONTEND_DATA_LENGTH);
    std::string hostnamePortS(hostnamePort);
    hostnamePortS = Utils::trim_copy(hostnamePortS);

    frontend_logger.info("Recieved LLM runnners: " + hostnamePortS);

    std::string llmInferenceMSG = "LLM inference engine? ollama/vllm? ";
    resultWr = write(connFd, llmInferenceMSG.c_str(), llmInferenceMSG.length());
    if (resultWr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return false;
    }
    resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (resultWr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return false;
    }

    char llmInferenceEngine[FRONTEND_DATA_LENGTH + 1];
    memset(llmInferenceEngine, 0, FRONTEND_DATA_LENGTH + 1);
    read(connFd, llmInferenceEngine, FRONTEND_DATA_LENGTH);
    std::string llmInferenceEngineS(llmInferenceEngine);
    llmInferenceEngineS = Utils::trim_copy(llmInferenceEngineS);

    frontend_logger.info("received Inference Engine: " + llmInferenceEngineS);

    std::string LLM_MSG = "What is the LLM you want to use?:";
    resultWr = write(connFd, LLM_MSG.c_str(), LLM_MSG.length());
    if (resultWr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return false;
    }
    resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (resultWr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return false;
    }

    char llm[FRONTEND_DATA_LENGTH + 1];
    memset(llm, 0, FRONTEND_DATA_LENGTH + 1);
    read(connFd, llm, FRONTEND_DATA_LENGTH);
    std::string llmS(llm);
    llmS = Utils::trim_copy(llmS);
    frontend_logger.info("Received LLM " + llmS);

    vector<std::string> llmServers = Utils::getUniqueLLMRunners(hostnamePortS);

    for (auto llmServer : llmServers) {
        std::string url;
        bool modelFound = false;
        std::string endpointPath;
        if (llmInferenceEngineS == "ollama") {
            endpointPath = "api/tags";
        } else if (llmInferenceEngineS == "vllm") {
            endpointPath = "/v1/models";
        } else {
            frontend_logger.error("Unknown inference engine: " + llmInferenceEngineS);
            std::string msg = "Unknown inference engine '" + llmInferenceEngineS + "'";
            write(connFd, msg.c_str(), msg.length());
            write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
            *loop_exit_p = true;
            return false;
        }

        url = Utils::normalizeURL(llmServer, endpointPath);
        frontend_logger.info("Final LLM endpoint: " + url);

        CURL *curl = curl_easy_init();
        if (curl) {
            std::string response;
            curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
            curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
            curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
            curl_easy_setopt(curl, CURLOPT_TIMEOUT, 5L);

            CURLcode res = curl_easy_perform(curl);
            curl_easy_cleanup(curl);

            if (res != CURLE_OK) {
                frontend_logger.error("Failed to reach " + llmInferenceEngineS + " server at " + llmServer);
                std::string msg = "Could not connect to " + llmInferenceEngineS + " server.";
                write(connFd, msg.c_str(), msg.length());
                write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
                *loop_exit_p = true;
                return false;
            }

            // --- Check model existence ---
            if (llmInferenceEngineS == "ollama") {
                // Ollama returns {"models":[{"name":"llama2"}]}
                if (response.find("\"name\":\"" + llmS + "\"") != std::string::npos) {
                    modelFound = true;
                }
            } else if (llmInferenceEngineS == "vllm") {
                // vLLM returns {"data":[{"id":"mistral"}]}
                frontend_logger.info(response);
                if (response.find("\"id\":\"" + llmS + "\"") != std::string::npos) {
                    modelFound = true;
                }
            }

            if (!modelFound) {
                frontend_logger.error("Model '" + llmS + "' not found on " + llmInferenceEngineS + " server.");
                std::string msg = "Model '" + llmS + "' not available on " + llmInferenceEngineS + " server.";
                write(connFd, msg.c_str(), msg.length());
                write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
                *loop_exit_p = true;
                return false;
            } else {
                frontend_logger.info("Verified model '" + llmS + "' exists on " + llmInferenceEngineS + " server.");
            }
        }
    }

    std::string chunk_size_msg = "chunk size (Bytes):";
    resultWr = write(connFd, chunk_size_msg.c_str(), chunk_size_msg.length());
    if (resultWr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return false;
    }
    resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (resultWr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return false;
    }

    char chunkSize[FRONTEND_DATA_LENGTH + 1];
    memset(chunkSize, 0, FRONTEND_DATA_LENGTH + 1);
    read(connFd, chunkSize, FRONTEND_DATA_LENGTH);
    std::string chunkSizeS(chunkSize);
    chunkSizeS = Utils::trim_copy(chunkSizeS);
    frontend_logger.info("Received engine chunk size: " + chunkSizeS);

    int newGraphID;
    bool graphExits = false;
    std::string checkQuery = "SELECT idgraph FROM graph WHERE upload_path = \"" + path + "\";";
    auto result = sqlite->runSelect(checkQuery);

    if (!result.empty() && !result[0].empty()) {
        int existingId = std::stoi(result[0][0].second);
        frontend_logger.info("Graph already exists with ID: " + std::to_string(existingId));

        std::string resume_msg = "There exists a graph with the file path, would you like to resume?";
        resultWr = write(connFd, resume_msg.c_str(), resume_msg.length());
        if (resultWr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return false;
        }
        resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        if (resultWr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return false;
        }

        char resume[FRONTEND_DATA_LENGTH + 1];
        memset(resume, 0, FRONTEND_DATA_LENGTH + 1);
        read(connFd, resume, FRONTEND_DATA_LENGTH);
        std::string resumeS(resume);
        resumeS = Utils::trim_copy(resumeS);

        if (resumeS == "y") {
            resume_msg = "Graph Id to resume?";
            resultWr = write(connFd, resume_msg.c_str(), resume_msg.length());
            if (resultWr < 0) {
                frontend_logger.error("Error writing to socket");
                *loop_exit_p = true;
                return false;
            }
            resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
            if (resultWr < 0) {
                frontend_logger.error("Error writing to socket");
                *loop_exit_p = true;
                return false;
            }

            char resumeGraphId[FRONTEND_DATA_LENGTH + 1];
            memset(resumeGraphId, 0, FRONTEND_DATA_LENGTH + 1);
            read(connFd, resumeGraphId, FRONTEND_DATA_LENGTH);
            std::string resumeGraphIdS(resumeGraphId);
            resumeGraphIdS = Utils::trim_copy(resumeGraphIdS);

            newGraphID = stoi(resumeGraphIdS);
            graphExits = true;
            frontend_logger.info("Resuming Knowledge Graph construction from GraphID: " + to_string(newGraphID));

        } else {
            std::string insertQuery =
                "INSERT INTO graph (name, upload_path, upload_start_time, "
                "upload_end_time, graph_status_idgraph_status, "
                "vertexcount, centralpartitioncount, edgecount, is_directed , "
                "file_size_bytes ) VALUES(\"" +
                hdfsFilePathS + "\", \"" + path + "\", \"" + uploadStartTime + "\", \"\", \"" +
                std::to_string(Conts::GRAPH_STATUS::NONOPERATIONAL) + "\", \"\", \"\", \"\", \"TRUE\", \"" +
                to_string(total_file_size) + "\");";
            frontend_logger.info("Constructing new Knowledge Graph with new GraphID: " + to_string(newGraphID));

            newGraphID = sqlite->runInsert(insertQuery);
        }

    } else {
        std::string insertQuery =
            "INSERT INTO graph (name, upload_path, upload_start_time, "
            "upload_end_time, graph_status_idgraph_status, "
            "vertexcount, centralpartitioncount, edgecount, is_directed , "
            "file_size_bytes ) VALUES(\"" +
            hdfsFilePathS + "\", \"" + path + "\", \"" + uploadStartTime + "\", \"\", \"" +
            std::to_string(Conts::GRAPH_STATUS::NONOPERATIONAL) + "\", \"\", \"\", \"\", \"TRUE\", \"" +
            to_string(total_file_size) + "\");";
            frontend_logger.info("Constructing new Knowledge Graph with new GraphID: " + to_string(newGraphID));

        newGraphID = sqlite->runInsert(insertQuery);
    }
    JasmineGraphServer::worker designatedWorker = JasmineGraphServer::getDesignatedWorker();
    auto stopFlag = std::make_shared<std::atomic<bool>>(false);
    {
        std::lock_guard<std::mutex> lock(threadMapMutex);
        stopFlags[newGraphID] = stopFlag;
    }

    // Launch streaming thread
    std::thread streamingThread([=]() mutable {
        frontend_logger.info("Starting streaming thread for GraphID: " + std::to_string(newGraphID));
        kgConstructionRates[newGraphID] = std::make_shared<KGConstructionRate>();
        kgConstructionRates[newGraphID]->bytesPerSecond = 0.0;
        kgConstructionRates[newGraphID]->triplesPerSecond = 0.0;

        std::make_shared<std::atomic<bool>>(false);
        bool success = Pipeline::streamGraphToDesignatedWorker(
            designatedWorker.hostname, designatedWorker.port, masterIP, std::to_string(newGraphID), numberOfPartitions,
            hdfsServerIp, hdfsPort, hostnamePortS, llmInferenceEngineS, llm, chunkSizeS, hdfsFilePathS, graphExits,
            sqlite, stopFlag, kgConstructionRates[newGraphID]);

        if (!success) {
            frontend_logger.error("Streaming to worker failed for GraphID: " + std::to_string(newGraphID));
        }

        std::time_t time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        std::string uploadEndTime = ctime(&time);

        std::string sqlStatementUpdateEndTime = "UPDATE graph SET upload_end_time = \"" + uploadEndTime +
                                                "\" WHERE idgraph = " + std::to_string(newGraphID);
        sqlite->runInsert(sqlStatementUpdateEndTime);

        frontend_logger.info("Async streaming finished for GraphID: " + std::to_string(newGraphID));
    });

    // store its id
    {
        std::lock_guard<std::mutex> lock(threadMapMutex);
        activeStreamThreads[newGraphID] = streamingThread.get_id();
    }

    streamingThread.detach();
    string finalMessage = "Graph Id: " + std::to_string(newGraphID);
    int conResultWr = write(connFd, finalMessage.c_str(), finalMessage.length());
    if (conResultWr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return false;
    }
    resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (resultWr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return false;
    }
    return true;
}

static void stop_stream_kafka_command(int connFd, KafkaConnector *kstream, bool *loop_exit_p) {
    frontend_logger.info("Started serving `" + STOP_STREAM_KAFKA + "` command");
    //          Unsubscribe the kafka consumer.
    kstream->Unsubscribe();
    string message = "Successfully stop `" + stream_topic_name + "` input kafka stream";
    int result_wr = write(connFd, message.c_str(), message.length());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());

    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
    }
}

static void process_dataset_command(int connFd, bool *loop_exit_p) {
    int result_wr = write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    // We get the name and the path to graph as a pair separated by |.
    char graph_data[FRONTEND_DATA_LENGTH + 1];
    bzero(graph_data, FRONTEND_DATA_LENGTH + 1);

    read(connFd, graph_data, FRONTEND_DATA_LENGTH);

    string gData(graph_data);

    gData = Utils::trim_copy(gData);
    frontend_logger.info("Data received: " + gData);

    if (gData.length() == 0) {
        frontend_logger.error("Message format not recognized");
        result_wr = write(connFd, INVALID_FORMAT.c_str(), INVALID_FORMAT.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
        return;
    }
    string path = gData;

    if (!Utils::fileExists(path)) {
        frontend_logger.error("Graph data file does not exist on the specified path");
    }
    frontend_logger.info("Path exists");

    JSONParser::jsonParse(path);
    frontend_logger.info("Reformatted files created on /home/.jasminegraph/tmp/JSONParser/output");
}

static void triangles_command(std::string masterIP, int connFd, SQLiteDBInterface *sqlite,
                              PerformanceSQLiteDBInterface *perfSqlite, JobScheduler *jobScheduler, bool *loop_exit_p) {
    // add RDF graph
    int uniqueId = JasmineGraphFrontEndCommon::getUid();
    int result_wr = write(connFd, GRAPHID_SEND.c_str(), GRAPHID_SEND.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    // We get the name and the path to graph as a pair separated by |.
    char graph_id_data[301];
    bzero(graph_id_data, 301);
    string name = "";

    read(connFd, graph_id_data, 300);

    string graph_id(graph_id_data);
    graph_id.erase(std::remove(graph_id.begin(), graph_id.end(), '\n'), graph_id.end());
    graph_id.erase(std::remove(graph_id.begin(), graph_id.end(), '\r'), graph_id.end());

    if (!JasmineGraphFrontEndCommon::graphExistsByID(graph_id, sqlite)) {
        string error_message = "The specified graph id does not exist";
        result_wr = write(connFd, error_message.c_str(), FRONTEND_COMMAND_LENGTH);
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }

        result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
    } else {
        int result_wr = write(connFd, PRIORITY.c_str(), PRIORITY.length());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }

        // We get the name and the path to graph as a pair separated by |.
        char priority_data[FRONTEND_DATA_LENGTH + 1];
        bzero(priority_data, FRONTEND_DATA_LENGTH + 1);

        read(connFd, priority_data, FRONTEND_DATA_LENGTH);

        string priority(priority_data);

        priority = Utils::trim_copy(priority);

        if (!(std::find_if(priority.begin(), priority.end(), [](unsigned char c) { return !std::isdigit(c); }) ==
              priority.end())) {
            *loop_exit_p = true;
            string error_message = "Priority should be numeric and > 1 or empty";
            result_wr = write(connFd, error_message.c_str(), error_message.length());
            if (result_wr < 0) {
                frontend_logger.error("Error writing to socket");
                return;
            }

            result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
            if (result_wr < 0) {
                frontend_logger.error("Error writing to socket");
            }
            return;
        }

        int threadPriority = std::atoi(priority.c_str());

        static volatile int reqCounter = 0;
        string reqId = to_string(reqCounter++);
        frontend_logger.info("Started processing request " + reqId);
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
                frontend_logger.error("Can't calibrate the graph now");
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
            result_wr = write(connFd, errorMessage.c_str(), errorMessage.length());

            if (result_wr < 0) {
                frontend_logger.error("Error writing to socket");
                return;
            }
            result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
            if (result_wr < 0) {
                frontend_logger.error("Error writing to socket");
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
        frontend_logger.info("Req: " + reqId + " Triangle Count: " + triangleCount +
                             " Time Taken: " + to_string(msDuration) + " milliseconds");
        result_wr = write(connFd, triangleCount.c_str(), triangleCount.length());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
    }
}

void JasmineGraphFrontEnd::scheduleStrianJobs(JobRequest &jobDetails, std::priority_queue<JobRequest> &jobQueue,
                                              JobScheduler *jobScheduler, bool *strian_exit) {
    while (!(*strian_exit)) {
        auto begin = chrono::high_resolution_clock::now();
        jobDetails.setBeginTime(begin);
        int uniqueId = JasmineGraphFrontEndCommon::getUid();
        jobDetails.setJobId(std::to_string(uniqueId));
        jobQueue.push(jobDetails);
        jobScheduler->pushJob(jobDetails);
        sleep(Conts::STREAMING_STRAIN_GAP);
    }
}

static void streaming_triangles_command(std::string masterIP, int connFd, JobScheduler *jobScheduler, bool *loop_exit_p,
                                        int numberOfPartitions, bool *strian_exit) {
    int result_wr = write(connFd, GRAPHID_SEND.c_str(), FRONTEND_COMMAND_LENGTH);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    // We get the name and the path to graph as a pair separated by |.
    char graph_id_data[FRONTEND_DATA_LENGTH + 1];
    bzero(graph_id_data, FRONTEND_DATA_LENGTH + 1);

    read(connFd, graph_id_data, FRONTEND_DATA_LENGTH);

    string graph_id(graph_id_data);
    graph_id = Utils::trim_copy(graph_id, " \f\n\r\t\v");

    frontend_logger.info("Got graph Id " + graph_id);

    result_wr = write(connFd, SEND_MODE.c_str(), FRONTEND_COMMAND_LENGTH);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    char mode_data[FRONTEND_DATA_LENGTH + 1];
    bzero(mode_data, FRONTEND_DATA_LENGTH + 1);

    read(connFd, mode_data, FRONTEND_DATA_LENGTH);

    string mode(mode_data);
    mode = Utils::trim_copy(mode, " \f\n\r\t\v");
    frontend_logger.info("Got mode " + mode);

    std::priority_queue<JobRequest> jobQueue;
    JobRequest jobDetails;
    jobDetails.setJobType(STREAMING_TRIANGLES);

    jobDetails.setMasterIP(masterIP);
    jobDetails.addParameter(Conts::PARAM_KEYS::GRAPH_ID, graph_id);
    jobDetails.addParameter(Conts::PARAM_KEYS::MODE, mode);
    jobDetails.addParameter(Conts::PARAM_KEYS::PARTITION, std::to_string(numberOfPartitions));

    if (*strian_exit) {
        *strian_exit = false;
    }

    std::thread schedulerThread(JasmineGraphFrontEnd::scheduleStrianJobs, std::ref(jobDetails), std::ref(jobQueue),
                                jobScheduler, std::ref(strian_exit));

    while (!(*strian_exit)) {
        if (!jobQueue.empty()) {
            JobRequest request = jobQueue.top();
            JobResponse jobResponse = jobScheduler->getResult(request);
            std::string errorMessage = jobResponse.getParameter(Conts::PARAM_KEYS::ERROR_MESSAGE);

            if (!errorMessage.empty()) {
                *loop_exit_p = true;
                result_wr = write(connFd, errorMessage.c_str(), errorMessage.length());

                if (result_wr < 0) {
                    frontend_logger.error("Error writing to socket");
                    return;
                }
                result_wr =
                    write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
                if (result_wr < 0) {
                    frontend_logger.error("Error writing to socket");
                }
                return;
            }

            std::string triangleCount = jobResponse.getParameter(Conts::PARAM_KEYS::STREAMING_TRIANGLE_COUNT);
            std::time_t begin_time_t = std::chrono::system_clock::to_time_t(request.getBegin());
            std::time_t end_time_t = std::chrono::system_clock::to_time_t(jobResponse.getEndTime());
            auto dur = jobResponse.getEndTime() - request.getBegin();
            auto msDuration = std::chrono::duration_cast<std::chrono::milliseconds>(dur).count();
            frontend_logger.info("Streaming triangle " + request.getJobId() + " Count : " + triangleCount +
                                 " Time Taken: " + to_string(msDuration) + " milliseconds");
            std::string out = triangleCount + " Time Taken: " + to_string(msDuration) +
                              " ms , Begin Time: " + std::ctime(&begin_time_t) +
                              " End Time: " + std::ctime(&end_time_t);
            result_wr = write(connFd, out.c_str(), out.length());
            if (result_wr < 0) {
                frontend_logger.error("Error writing to socket");
                *loop_exit_p = true;
                return;
            }
            result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
            if (result_wr < 0) {
                frontend_logger.error("Error writing to socket");
                *loop_exit_p = true;
            }
            jobQueue.pop();
        } else {
            sleep(Conts::SCHEDULER_SLEEP_TIME);
        }
    }
    schedulerThread.join();  // Wait for the scheduler thread to finish
}

static void stop_strian_command(int connFd, bool *strian_exit) { *strian_exit = true; }

static void vertex_count_command(int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p) {
    int result_wr = write(connFd, GRAPHID_SEND.c_str(), GRAPHID_SEND.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    char graph_id_data[301];
    bzero(graph_id_data, 301);
    string name = "";

    read(connFd, graph_id_data, 300);

    string graphId(graph_id_data);

    graphId.erase(std::remove(graphId.begin(), graphId.end(), '\n'), graphId.end());
    graphId.erase(std::remove(graphId.begin(), graphId.end(), '\r'), graphId.end());

    if (!JasmineGraphFrontEndCommon::graphExistsByID(graphId, sqlite)) {
        string error_message = "The specified graph id does not exist";
        result_wr = write(connFd, error_message.c_str(), FRONTEND_COMMAND_LENGTH);
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
    } else {
        string sqlStatement = "SELECT vertexcount from graph where idgraph=" + graphId;

        std::vector<vector<pair<string, string>>> output = sqlite->runSelect(sqlStatement);

        int vertexCount = std::stoi(output[0][0].second);
        frontend_logger.info("Vertex Count: " + to_string(vertexCount));
        result_wr = write(connFd, to_string(vertexCount).c_str(), to_string(vertexCount).length());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
    }
}

static void edge_count_command(int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p) {
    int result_wr = write(connFd, GRAPHID_SEND.c_str(), GRAPHID_SEND.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    char graph_id_data[301];
    bzero(graph_id_data, 301);
    string name = "";

    read(connFd, graph_id_data, 300);

    string graph_id(graph_id_data);

    graph_id.erase(std::remove(graph_id.begin(), graph_id.end(), '\n'), graph_id.end());
    graph_id.erase(std::remove(graph_id.begin(), graph_id.end(), '\r'), graph_id.end());

    if (!JasmineGraphFrontEndCommon::graphExistsByID(graph_id, sqlite)) {
        string error_message = "The specified graph id does not exist";
        result_wr = write(connFd, error_message.c_str(), FRONTEND_COMMAND_LENGTH);
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
    } else {
        string sqlStatement = "SELECT edgecount from graph where idgraph=" + graph_id;

        std::vector<vector<pair<string, string>>> output = sqlite->runSelect(sqlStatement);

        int edgeCount = std::stoi(output[0][0].second);
        frontend_logger.info("Edge Count: " + to_string(edgeCount));
        result_wr = write(connFd, to_string(edgeCount).c_str(), to_string(edgeCount).length());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
    }
}

static void merge_command(int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p) {
    string message = "Available main flags:\r\n";
    int result_wr = write(connFd, message.c_str(), message.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    string flags = Conts::FLAGS::GRAPH_ID;
    result_wr = write(connFd, flags.c_str(), flags.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    message = "Send --<flag1> <value1>\r\n";
    result_wr = write(connFd, message.c_str(), message.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    char train_data[301];
    bzero(train_data, 301);
    read(connFd, train_data, 300);

    string trainData(train_data);
    trainData = Utils::trim_copy(trainData);
    frontend_logger.info("Data received: " + trainData);

    std::vector<std::string> trainargs = Utils::split(trainData, ' ');
    std::vector<std::string>::iterator itr = std::find(trainargs.begin(), trainargs.end(), "--graph_id");
    std::string graphID;

    if (itr != trainargs.cend()) {
        int index = std::distance(trainargs.begin(), itr);
        graphID = trainargs[index + 1];

    } else {
        frontend_logger.error("graph_id should be given as an argument");
        result_wr = write(connFd, INVALID_FORMAT.c_str(), INVALID_FORMAT.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
        return;
    }

    if (trainargs.size() == 0) {
        frontend_logger.error("Message format not recognized");
        result_wr = write(connFd, INVALID_FORMAT.c_str(), INVALID_FORMAT.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
        return;
    }

    JasmineGraphServer *jasmineServer = JasmineGraphServer::getInstance();
    jasmineServer->initiateFiles(graphID, trainData);
    jasmineServer->initiateMerge(graphID, trainData, sqlite);
    result_wr = write(connFd, DONE.c_str(), FRONTEND_COMMAND_LENGTH);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
    }
}

static void train_command(int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p) {
    string message = "Available main flags:\r\n";
    int result_wr = write(connFd, message.c_str(), message.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    string flags = Conts::FLAGS::GRAPH_ID + " " + Conts::FLAGS::LEARNING_RATE + " " + Conts::FLAGS::BATCH_SIZE + " " +
                   Conts::FLAGS::VALIDATE_ITER + " " + Conts::FLAGS::EPOCHS;
    result_wr = write(connFd, flags.c_str(), flags.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    message = "Send --<flag1> <value1> --<flag2> <value2> ..\r\n";
    result_wr = write(connFd, message.c_str(), message.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    char train_data[301];
    bzero(train_data, 301);
    read(connFd, train_data, 300);

    string trainData(train_data);
    trainData = Utils::trim_copy(trainData);
    frontend_logger.info("Data received: " + trainData);

    std::vector<std::string> trainargs = Utils::split(trainData, ' ');
    std::vector<std::string>::iterator itr = std::find(trainargs.begin(), trainargs.end(), "--graph_id");
    std::string graphID;
    std::string modelID;
    if (itr != trainargs.cend()) {
        int index = std::distance(trainargs.begin(), itr);
        graphID = trainargs[index + 1];
    } else {
        frontend_logger.error("graph_id should be given as an argument");
        result_wr = write(connFd, INVALID_FORMAT.c_str(), INVALID_FORMAT.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
        return;
    }

    if (trainargs.size() == 0) {
        frontend_logger.error("Message format not recognized");
        result_wr = write(connFd, INVALID_FORMAT.c_str(), INVALID_FORMAT.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
        return;
    }

    if (!JasmineGraphFrontEndCommon::isGraphActive(graphID, sqlite)) {
        string error_message = "Graph is not in the active status";
        frontend_logger.error(error_message);
        result_wr = write(connFd, error_message.c_str(), error_message.length());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
        return;
    }
    auto *server = JasmineGraphServer::getInstance();
    if (Utils::getJasmineGraphProperty("org.jasminegraph.fl.org.training") == "true") {
        frontend_logger.info("Initiate org communication");
        JasmineGraphServer::initiateOrgCommunication(graphID, trainData, sqlite, server->masterHost);
    } else {
        frontend_logger.info("Initiate communication");
        JasmineGraphServer::initiateCommunication(graphID, trainData, sqlite, server->masterHost);
    }

    result_wr = write(connFd, DONE.c_str(), FRONTEND_COMMAND_LENGTH);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
}

static void in_degree_command(int connFd, bool *loop_exit_p) {
    frontend_logger.info("Calculating In Degree Distribution");

    int result_wr = write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    char graph_id[FRONTEND_DATA_LENGTH + 1];
    bzero(graph_id, FRONTEND_DATA_LENGTH + 1);

    read(connFd, graph_id, FRONTEND_DATA_LENGTH);

    string graphID(graph_id);

    graphID = Utils::trim_copy(graphID);
    frontend_logger.info("Graph ID received: " + graphID);

    JasmineGraphServer::inDegreeDistribution(graphID);

    result_wr = write(connFd, DONE.c_str(), FRONTEND_COMMAND_LENGTH);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
    }
}

static void out_degree_command(int connFd, bool *loop_exit_p) {
    frontend_logger.info("Calculating Out Degree Distribution");

    int result_wr = write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    char graph_id[FRONTEND_DATA_LENGTH + 1];
    bzero(graph_id, FRONTEND_DATA_LENGTH + 1);

    read(connFd, graph_id, FRONTEND_DATA_LENGTH);

    string graphID(graph_id);

    graphID = Utils::trim_copy(graphID);
    frontend_logger.info("Graph ID received: " + graphID);

    JasmineGraphServer::outDegreeDistribution(graphID);

    result_wr = write(connFd, DONE.c_str(), FRONTEND_COMMAND_LENGTH);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
    }
}

static void page_rank_command(std::string masterIP, int connFd, SQLiteDBInterface *sqlite,
                              PerformanceSQLiteDBInterface *perfSqlite, JobScheduler *jobScheduler, bool *loop_exit_p) {
    frontend_logger.info("Calculating Page Rank");

    int result_wr = write(connFd, GRAPHID_SEND.c_str(), FRONTEND_COMMAND_LENGTH);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    char page_rank_command[FRONTEND_DATA_LENGTH + 1];
    bzero(page_rank_command, FRONTEND_DATA_LENGTH + 1);
    string name = "";
    string path = "";

    read(connFd, page_rank_command, FRONTEND_DATA_LENGTH);
    std::vector<std::string> strArr = Utils::split(page_rank_command, '|');

    string graphID;
    graphID = strArr[0];
    double alpha = PAGE_RANK_ALPHA;
    if (strArr.size() > 1) {
        alpha = std::stod(strArr[1]);
        if (alpha < 0 || alpha >= 1) {
            frontend_logger.error("Invalid value for alpha");
            result_wr = write(connFd, INVALID_FORMAT.c_str(), INVALID_FORMAT.size());
            if (result_wr < 0) {
                frontend_logger.error("Error writing to socket");
                *loop_exit_p = true;
            }
            return;
        }
    }

    int iterations = PAGE_RANK_ITERATIONS;
    if (strArr.size() > 2) {
        iterations = std::stod(strArr[2]);
        if (iterations <= 0 || iterations >= 100) {
            frontend_logger.error("Invalid value for iterations");
            result_wr = write(connFd, INVALID_FORMAT.c_str(), INVALID_FORMAT.size());
            if (result_wr < 0) {
                frontend_logger.error("Error writing to socket");
                *loop_exit_p = true;
            }
            return;
        }
    }

    graphID = Utils::trim_copy(graphID);
    frontend_logger.info("Graph ID received: " + graphID);
    frontend_logger.info("Alpha value: " + to_string(alpha));
    frontend_logger.info("Iterations value: " + to_string(iterations));

    result_wr = write(connFd, PRIORITY.c_str(), PRIORITY.length());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    // We get the name and the path to graph as a pair separated by |.
    char priority_data[DATA_BUFFER_SIZE];
    bzero(priority_data, DATA_BUFFER_SIZE);
    read(connFd, priority_data, FRONTEND_DATA_LENGTH);
    string priority(priority_data);
    priority = Utils::trim_copy(priority);

    if (!(std::find_if(priority.begin(), priority.end(), [](unsigned char c) { return !std::isdigit(c); }) ==
          priority.end())) {
        *loop_exit_p = true;
        string error_message = "Priority should be numeric and > 1 or empty";
        result_wr = write(connFd, error_message.c_str(), error_message.length());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            return;
        }

        result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
        }
        return;
    }

    int threadPriority = std::atoi(priority.c_str());

    auto begin = chrono::high_resolution_clock::now();
    JobRequest jobDetails;
    int uniqueId = JasmineGraphFrontEndCommon::getUid();
    jobDetails.setJobId(std::to_string(uniqueId));
    jobDetails.setJobType(PAGE_RANK);

    long graphSLA = -1;  // This prevents auto calibration for priority=1 (=default priority)
    if (threadPriority > Conts::DEFAULT_THREAD_PRIORITY) {
        // All high priority threads will be set the same high priority level
        threadPriority = Conts::HIGH_PRIORITY_DEFAULT_VALUE;
        graphSLA = JasmineGraphFrontEndCommon::getSLAForGraphId(sqlite, perfSqlite, graphID, PAGE_RANK,
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
            frontend_logger.error("Can't calibrate the graph now");
        }
    }

    jobDetails.setPriority(threadPriority);
    jobDetails.setMasterIP(masterIP);
    jobDetails.addParameter(Conts::PARAM_KEYS::GRAPH_ID, graphID);
    jobDetails.addParameter(Conts::PARAM_KEYS::CATEGORY, Conts::SLA_CATEGORY::LATENCY);
    jobDetails.addParameter(Conts::PARAM_KEYS::ALPHA, std::to_string(alpha));
    jobDetails.addParameter(Conts::PARAM_KEYS::ITERATION, std::to_string(iterations));

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
        result_wr = write(connFd, errorMessage.c_str(), errorMessage.length());

        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            return;
        }
        result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
        }
        return;
    }

    if (threadPriority == Conts::HIGH_PRIORITY_DEFAULT_VALUE) {
        highPriorityTaskCount--;
    }

    auto end = chrono::high_resolution_clock::now();
    auto dur = end - begin;
    auto msDuration = std::chrono::duration_cast<std::chrono::milliseconds>(dur).count();
    frontend_logger.info("PageRank Time Taken : " + to_string(msDuration) + " milliseconds");

    result_wr = write(connFd, DONE.c_str(), FRONTEND_COMMAND_LENGTH);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
    }
}

static void egonet_command(int connFd, bool *loop_exit_p) {
    frontend_logger.info("Calculating EgoNet");

    int result_wr = write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    char graph_id[FRONTEND_DATA_LENGTH + 1];
    bzero(graph_id, FRONTEND_DATA_LENGTH + 1);

    read(connFd, graph_id, FRONTEND_DATA_LENGTH);

    string graphID(graph_id);

    graphID = Utils::trim_copy(graphID);
    frontend_logger.info("Graph ID received: " + graphID);

    JasmineGraphServer::egoNet(graphID);

    result_wr = write(connFd, DONE.c_str(), FRONTEND_COMMAND_LENGTH);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
    }
}

static void duplicate_centralstore_command(int connFd, bool *loop_exit_p) {
    frontend_logger.info("Duplicate Centralstore");

    int result_wr = write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    char graph_id[FRONTEND_DATA_LENGTH + 1];
    bzero(graph_id, FRONTEND_DATA_LENGTH + 1);

    read(connFd, graph_id, FRONTEND_DATA_LENGTH);

    string graphID(graph_id);

    graphID = Utils::trim_copy(graphID);
    frontend_logger.info("Graph ID received: " + graphID);

    JasmineGraphServer::duplicateCentralStore(graphID);

    result_wr = write(connFd, DONE.c_str(), FRONTEND_COMMAND_LENGTH);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
    }
}

static void predict_command(std::string masterIP, int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p) {
    if (Utils::getJasmineGraphProperty("org.jasminegraph.federated.enabled") == "true") {
        // check if the model is available
        // then pass the information to the jasminegraph worker

        // Need to define the protocol for the predict command in federated learning context
        int result_wr = write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }

        char predict_data[301];
        bzero(predict_data, 301);
        string graphID = "";
        string modelID = "";
        string path = "";

        read(connFd, predict_data, 300);
        string predictData(predict_data);

        predictData = Utils::trim_copy(predictData);
        frontend_logger.info("Data received: " + predictData);

        std::vector<std::string> strArr = Utils::split(predictData, '|');

        if (strArr.size() != 3) {
            frontend_logger.error("Message format not recognized");
            result_wr = write(connFd, INVALID_FORMAT.c_str(), INVALID_FORMAT.size());
            if (result_wr < 0) {
                frontend_logger.error("Error writing to socket");
                *loop_exit_p = true;
            }
            return;
        }

        graphID = strArr[0];
        modelID = strArr[1];
        path = strArr[2];

    } else {
        int result_wr = write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        char predict_data[301];
        bzero(predict_data, 301);
        string graphID = "";
        string path = "";

        read(connFd, predict_data, 300);
        string predictData(predict_data);

        predictData = Utils::trim_copy(predictData);
        frontend_logger.info("Data received: " + predictData);

        std::vector<std::string> strArr = Utils::split(predictData, '|');

        if (strArr.size() != 2) {
            frontend_logger.error("Message format not recognized");
            result_wr = write(connFd, INVALID_FORMAT.c_str(), INVALID_FORMAT.size());
            if (result_wr < 0) {
                frontend_logger.error("Error writing to socket");
                *loop_exit_p = true;
            }
            return;
        }

        graphID = strArr[0];
        path = strArr[1];

        if (JasmineGraphFrontEndCommon::isGraphActiveAndTrained(graphID, sqlite)) {
            if (Utils::fileExists(path)) {
                frontend_logger.error("Path exists");
                JasminGraphLinkPredictor::initiateLinkPrediction(graphID, path, masterIP);
            } else {
                frontend_logger.error("Graph edge file does not exist on the specified path");
            }
        }
    }
}

static void start_remote_worker_command(int connFd, bool *loop_exit_p) {
    int result_wr = write(connFd, REMOTE_WORKER_ARGS.c_str(), REMOTE_WORKER_ARGS.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    char worker_data[301];
    bzero(worker_data, 301);
    read(connFd, worker_data, 300);
    string remote_worker_data(worker_data);

    remote_worker_data = Utils::trim_copy(remote_worker_data);
    frontend_logger.info("Data received: " + remote_worker_data);
    string host = "";
    string port = "";
    string dataPort = "";
    string masterHost = "";
    string enableNmon = "";

    std::vector<std::string> strArr = Utils::split(remote_worker_data, '|');

    if (strArr.size() < 6) {
        frontend_logger.error("Message format not recognized");
        result_wr = write(connFd, INVALID_FORMAT.c_str(), INVALID_FORMAT.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
        return;
    }

    host = strArr[0];
    port = strArr[1];
    dataPort = strArr[2];
    masterHost = strArr[4];
    enableNmon = strArr[5];

    JasmineGraphServer::spawnNewWorker(host, port, dataPort, masterHost, enableNmon);
}

static void sla_command(int connFd, SQLiteDBInterface *sqlite, PerformanceSQLiteDBInterface *perfSqlite,
                        bool *loop_exit_p) {
    int result_wr = write(connFd, COMMAND.c_str(), COMMAND.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    char category[FRONTEND_DATA_LENGTH + 1];
    bzero(category, FRONTEND_DATA_LENGTH + 1);
    read(connFd, category, FRONTEND_DATA_LENGTH);
    string command_info(category);

    command_info = Utils::trim_copy(command_info);
    frontend_logger.info("Data received: " + command_info);

    std::vector<vector<pair<string, string>>> categoryResults =
        perfSqlite->runSelect("SELECT id FROM sla_category where command='" + command_info + "';");

    string slaCategoryIds;

    for (std::vector<vector<pair<string, string>>>::iterator i = categoryResults.begin(); i != categoryResults.end();
         ++i) {
        for (std::vector<pair<string, string>>::iterator j = (i->begin()); j != i->end(); ++j) {
            slaCategoryIds = slaCategoryIds + "'" + j->second + "',";
        }
    }

    string adjustedIdList = slaCategoryIds.substr(0, slaCategoryIds.size() - 1);

    std::stringstream ss;
    std::vector<vector<pair<string, string>>> v =
        perfSqlite->runSelect("SELECT graph_id, partition_count, sla_value FROM graph_sla where id_sla_category in (" +
                              adjustedIdList + ");");
    for (std::vector<vector<pair<string, string>>>::iterator i = v.begin(); i != v.end(); ++i) {
        std::stringstream slass;
        slass << "|";
        int counter = 0;
        for (std::vector<pair<string, string>>::iterator j = (i->begin()); j != i->end(); ++j) {
            if (counter == 0) {
                std::string graphId = j->second;
                std::string graphQuery = "SELECT name FROM graph where idgraph='" + graphId + "';";
                std::vector<vector<pair<string, string>>> graphData = sqlite->runSelect(graphQuery);
                if (graphData.size() == 0) {
                    slass.str(std::string());
                    break;
                }
                std::string graphName = graphData[0][0].second;
                slass << graphName << "|";
            } else {
                slass << j->second << "|";
            }
            counter++;
        }
        std::string entryString = slass.str();
        if (entryString.size() > 0) {
            ss << entryString << "\n";
        }
    }
    string result = ss.str();
    if (result.size() == 0) {
        int result_wr = write(connFd, EMPTY.c_str(), EMPTY.length());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());

        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
    } else {
        int result_wr = write(connFd, result.c_str(), result.length());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
    }
}

void JasmineGraphFrontEnd::stop_graph_streaming(int connFd, bool *loop_exit_p) {
    std::string message1 = "Graph ID?";
    int resultWr = write(connFd, message1.c_str(), message1.length());
    if (resultWr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (resultWr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    char userRes[FRONTEND_DATA_LENGTH + 1];
    memset(userRes, 0, FRONTEND_DATA_LENGTH + 1);
    read(connFd, userRes, FRONTEND_DATA_LENGTH);
    std::string userResS(userRes);
    userResS = Utils::trim_copy(userResS);

    std::lock_guard<std::mutex> lock(threadMapMutex);
    auto it = stopFlags.find(stoi(userResS));
    if (it != stopFlags.end()) {
        *(it->second) = true;

        int maxWaits = 12;  // Wait up to 6 minute (12 * 5 seconds)
        int waits = 0;
        while (*(it->second) && waits < maxWaits) {
            sleep(5);
            waits++;
        }
        if (*(it->second)) {
            frontend_logger.error("Timeout: The stop flag was not reverted in time");
            std::string message3 = "Failed to stop the process";
            int resultWr = write(connFd, message3.c_str(), message3.length());
        }
        int result_wr = write(connFd, DONE.c_str(), FRONTEND_COMMAND_LENGTH);

    } else {
        std::string message2 = "Graph Id not Found";
        int resultWr = write(connFd, message2.c_str(), message2.length());
    }
}
