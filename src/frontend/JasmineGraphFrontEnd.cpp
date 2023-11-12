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

#include <spdlog/spdlog.h>

#include <cctype>
#include <chrono>
#include <ctime>
#include <fstream>  // C++ header file for file access
#include <iostream>
#include <iostream>  // C++ header file for printing
#include <map>
#include <nlohmann/json.hpp>
#include <set>
#include <thread>

#include "../centralstore/incremental/DataPublisher.h"
#include "../centralstore/incremental/RelationBlock.h"
#include "../metadb/SQLiteDBInterface.h"
#include "../ml/trainer/JasminGraphTrainingInitiator.h"
#include "../ml/trainer/JasmineGraphTrainingSchedular.h"
#include "../ml/trainer/python-c-api/Python_C_API.h"
#include "../partitioner/local/JSONParser.h"
#include "../partitioner/local/MetisPartitioner.h"
#include "../partitioner/local/RDFParser.h"
#include "../partitioner/local/RDFPartitioner.h"
#include "../partitioner/stream/Partitioner.h"
#include "../performance/metrics/PerformanceUtil.h"
#include "../query/algorithms/linkprediction/JasminGraphLinkPredictor.h"
#include "../server/JasmineGraphInstanceProtocol.h"
#include "../server/JasmineGraphServer.h"
#include "../util/Conts.h"
#include "../util/kafka/KafkaCC.h"
#include "../util/logger/Logger.h"
#include "JasmineGraphFrontEndProtocol.h"
#include "core/CoreConstants.h"
#include "core/scheduler/JobScheduler.h"

using json = nlohmann::json;
using namespace std;
using namespace std::chrono;

std::atomic<int> highPriorityTaskCount;
static int connFd;
static int currentFESession;
static bool canCalibrate = true;
Logger frontend_logger;
std::set<ProcessInfo> processData;
std::mutex aggregateWeightMutex;
std::mutex triangleTreeMutex;
std::string stream_topic_name;

static void list_command(int connFd, SQLiteDBInterface sqlite, bool *loop_exit_p);
static void add_rdf_command(std::string masterIP, int connFd, SQLiteDBInterface sqlite, bool *loop_exit_p);
static void add_graph_command(std::string masterIP, int connFd, SQLiteDBInterface sqlite, bool *loop_exit_p);
static void add_graph_cust_command(std::string masterIP, int connFd, SQLiteDBInterface sqlite, bool *loop_exit_p);
static void remove_graph_command(std::string masterIP, int connFd, SQLiteDBInterface sqlite, bool *loop_exit_p);
static void add_model_command(int connFd, SQLiteDBInterface sqlite, bool *loop_exit_p);
static void add_stream_kafka_command(int connFd, std::string &kafka_server_IP, cppkafka::Configuration &configs,
                                     KafkaConnector *&kstream, thread &input_stream_handler,
                                     vector<DataPublisher *> &workerClients, int numberOfPartitions, bool *loop_exit_p);
static void stop_stream_kafka_command(int connFd, KafkaConnector *kstream, bool *loop_exit_p);
static void process_dataset_command(int connFd, bool *loop_exit_p);
static void triangles_command(std::string masterIP, int connFd, SQLiteDBInterface sqlite,
                              PerformanceSQLiteDBInterface perfSqlite, JobScheduler jobScheduler, bool *loop_exit_p);
static void vertex_count_command(int connFd, SQLiteDBInterface sqlite, bool *loop_exit_p);
static void edge_count_command(int connFd, SQLiteDBInterface sqlite, bool *loop_exit_p);
static void merge_command(int connFd, SQLiteDBInterface sqlite, bool *loop_exit_p);
static void train_command(int connFd, SQLiteDBInterface sqlite, bool *loop_exit_p);
static void in_degree_command(int connFd, bool *loop_exit_p);
static void out_degree_command(int connFd, bool *loop_exit_p);
static void page_rank_command(int connFd, bool *loop_exit_p);
static void egonet_command(int connFd, bool *loop_exit_p);
static void duplicate_centralstore_command(int connFd, bool *loop_exit_p);
static void predict_command(std::string masterIP, int connFd, SQLiteDBInterface sqlite, bool *loop_exit_p);
static void start_remote_worker_command(int connFd, bool *loop_exit_p);
static void sla_command(int connFd, SQLiteDBInterface sqlite, PerformanceSQLiteDBInterface perfSqlite,
                        bool *loop_exit_p);
// Thread function
void listen_to_kafka_topic(KafkaConnector *kstream, Partitioner &graphPartitioner,
                           vector<DataPublisher *> &workerClients) {
    while (true) {
        cppkafka::Message msg = kstream->consumer.poll();
        if (!msg || msg.get_error()) {
            continue;
        }
        string data(msg.get_payload());
        if (data == "-1") {  // Marks the end of stream
            frontend_logger.info("Received the end of `" + stream_topic_name + "` input kafka stream");
            break;
        }
        auto edgeJson = json::parse(data);
        auto sourceJson = edgeJson["source"];
        auto destinationJson = edgeJson["destination"];
        std::string sId = std::string(sourceJson["id"]);
        std::string dId = std::string(destinationJson["id"]);
        partitionedEdge partitionedEdge = graphPartitioner.addEdge({sId, dId});
        sourceJson["pid"] = partitionedEdge[0].second;
        destinationJson["pid"] = partitionedEdge[1].second;
        string source = sourceJson.dump();
        string destination = destinationJson.dump();
        json obj;
        obj["source"] = sourceJson;
        obj["destination"] = destinationJson;
        long temp_s = partitionedEdge[0].second;
        long temp_d = partitionedEdge[1].second;
        workerClients.at((int)partitionedEdge[0].second)->publish(sourceJson.dump());
        workerClients.at((int)partitionedEdge[1].second)->publish(destinationJson.dump());
        //      storing Node block
        if (temp_s == temp_d) {
            // +miyurud: Temorarily commeting the following line to make the code build
            // workerClients.at((int) partitionedEdge[0].second)->publish_relation(obj.dump());
        }
    }
    graphPartitioner.printStats();
}

void *frontendservicesesion(std::string masterIP, int connFd, SQLiteDBInterface sqlite,
                            PerformanceSQLiteDBInterface perfSqlite, JobScheduler jobScheduler) {
    frontend_logger.info("Thread No: " + to_string(pthread_self()));
    frontend_logger.info("Master IP: " + masterIP);
    char data[FRONTEND_DATA_LENGTH + 1];
    bzero(data, FRONTEND_DATA_LENGTH + 1);
    vector<Utils::worker> workerList = Utils::getWorkerList(sqlite);
    vector<DataPublisher *> workerClients;

    //  Initiate Thread
    thread input_stream_handler;
    //  Initiate kafka consumer parameters
    std::string partitionCount = Utils::getJasmineGraphProperty("org.jasminegraph.server.npartitions");
    int numberOfPartitions = std::stoi(partitionCount);
    std::string kafka_server_IP;
    cppkafka::Configuration configs;
    KafkaConnector *kstream;
    Partitioner graphPartitioner(numberOfPartitions, 1, spt::Algorithms::HASH);

    for (int i = 0; i < workerList.size(); i++) {
        Utils::worker currentWorker = workerList.at(i);
        string workerHost = currentWorker.hostname;
        string workerID = currentWorker.workerID;
        int workerPort = atoi(string(currentWorker.port).c_str());
        DataPublisher *workerClient = new DataPublisher(workerPort, workerHost);
        workerClients.push_back(workerClient);
    }
    bool loop_exit = false;
    while (!loop_exit) {
        if (currentFESession == Conts::MAX_FE_SESSIONS + 1) {
            currentFESession--;
            std::string errorResponse = "Jasminegraph Server is Busy. Please try again later.";
            int result_wr = write(connFd, errorResponse.c_str(), errorResponse.length());
            if (result_wr < 0) {
                frontend_logger.error("Error writing to socket");
            }
            break;
        }

        bzero(data, FRONTEND_DATA_LENGTH + 1);
        read(connFd, data, FRONTEND_DATA_LENGTH);

        string line(data);
        if (line.compare("\r\n") == 0) {
            continue;
        }
        frontend_logger.info("Command received: " + line);

        if (line.empty()) {
            currentFESession--;
            break;
        }

        line = Utils::trim_copy(line, " \f\n\r\t\v");

        if (currentFESession > 1) {
            canCalibrate = false;
        } else {
            canCalibrate = true;
            workerResponded = false;
        }

        if (line.compare(EXIT) == 0) {
            currentFESession--;
            break;
        } else if (line.compare(LIST) == 0) {
            list_command(connFd, sqlite, &loop_exit);
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
            add_stream_kafka_command(connFd, kafka_server_IP, configs, kstream, input_stream_handler, workerClients,
                                     numberOfPartitions, &loop_exit);
        } else if (line.compare(STOP_STREAM_KAFKA) == 0) {
            stop_stream_kafka_command(connFd, kstream, &loop_exit);
        } else if (line.compare(RMGR) == 0) {
            remove_graph_command(masterIP, connFd, sqlite, &loop_exit);
        } else if (line.compare(PROCESS_DATASET) == 0) {
            process_dataset_command(connFd, &loop_exit);
        } else if (line.compare(TRIANGLES) == 0) {
            triangles_command(masterIP, connFd, sqlite, perfSqlite, jobScheduler, &loop_exit);
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
            page_rank_command(connFd, &loop_exit);
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
            // TODO: Inform client?
        }
    }
    if (input_stream_handler.joinable()) {
        input_stream_handler.join();
    }
    frontend_logger.info("Closing thread " + to_string(pthread_self()) + " and connection");
    close(connFd);
    return NULL;
}

JasmineGraphFrontEnd::JasmineGraphFrontEnd(SQLiteDBInterface db, PerformanceSQLiteDBInterface perfDb,
                                           std::string masterIP, JobScheduler jobScheduler) {
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
        exit(1);
    }

    // bind socket
    if (bind(listenFd, (struct sockaddr *)&svrAdd, sizeof(svrAdd)) < 0) {
        frontend_logger.error("Cannot bind on port " + portNo);
        return 0;
    }

    listen(listenFd, 10);

    std::thread *myThreads = new std::thread[20];
    std::vector<std::thread> threadVector;
    len = sizeof(clntAdd);

    int noThread = 0;

    while (true) {
        frontend_logger.info("Frontend Listening");

        // this is where client connects. svr will hang in this mode until client conn
        connFd = accept(listenFd, (struct sockaddr *)&clntAdd, &len);

        if (connFd < 0) {
            frontend_logger.error("Cannot accept connection");
            return 0;
        } else {
            frontend_logger.info("Connection successful");
        }

        frontend_logger.info("Master IP" + masterIP);

        struct frontendservicesessionargs *frontendservicesessionargs1 =
            (struct frontendservicesessionargs *)malloc(sizeof(struct frontendservicesessionargs) * 1);
        frontendservicesessionargs1->sqlite = this->sqlite;
        frontendservicesessionargs1->connFd = connFd;

        // TODO(miyurud):Temporarily commenting this line to enable building the project. Asked tmkasun to provide a
        //  permanent fix later when he is available.
        threadVector.push_back(
            std::thread(frontendservicesesion, masterIP, connFd, this->sqlite, this->perfSqlite, this->jobScheduler));

        currentFESession++;
    }
}

/**
 * This method checks if a graph exists in JasmineGraph.
 * This method uses the unique path of the graph.
 * @param basic_string
 * @param dummyPt
 * @return
 */
bool JasmineGraphFrontEnd::graphExists(string path, SQLiteDBInterface sqlite) {
    bool result = true;
    string stmt = "SELECT COUNT( * ) FROM graph WHERE upload_path LIKE '" + path +
                  "' AND graph_status_idgraph_status = '" + to_string(Conts::GRAPH_STATUS::OPERATIONAL) + "';";
    std::vector<vector<pair<string, string>>> v = sqlite.runSelect(stmt);
    int count = std::stoi(v[0][0].second);
    if (count == 0) {
        result = false;
    }
    return result;
}

/**
 * This method checks if an accessible graph exists in JasmineGraph with the same unique ID.
 * @param id
 * @param dummyPt
 * @return
 */
bool JasmineGraphFrontEnd::graphExistsByID(string id, SQLiteDBInterface sqlite) {
    bool result = true;
    string stmt = "SELECT COUNT( * ) FROM graph WHERE idgraph = " + id;
    std::vector<vector<pair<string, string>>> v = sqlite.runSelect(stmt);
    int count = std::stoi(v[0][0].second);

    if (count == 0) {
        result = false;
    }

    return result;
}

/**
 * This method removes a graph from JasmineGraph
 */
void JasmineGraphFrontEnd::removeGraph(std::string graphID, SQLiteDBInterface sqlite, std::string masterIP) {
    vector<pair<string, string>> hostHasPartition;
    vector<vector<pair<string, string>>> hostPartitionResults = sqlite.runSelect(
        "SELECT name, partition_idpartition FROM worker_has_partition INNER JOIN worker ON "
        "worker_has_partition.worker_idworker = worker.idworker WHERE partition_graph_idgraph = " +
        graphID + ";");
    for (vector<vector<pair<string, string>>>::iterator i = hostPartitionResults.begin();
         i != hostPartitionResults.end(); ++i) {
        int count = 0;
        string hostname;
        string partitionID;
        for (std::vector<pair<string, string>>::iterator j = (i->begin()); j != i->end(); ++j) {
            if (count == 0) {
                hostname = j->second;
            } else {
                partitionID = j->second;
                hostHasPartition.push_back(pair<string, string>(hostname, partitionID));
            }
            count++;
        }
    }
    for (std::vector<pair<string, string>>::iterator j = (hostHasPartition.begin()); j != hostHasPartition.end(); ++j) {
        cout << "HOST ID : " << j->first << " Partition ID : " << j->second << endl;
    }
    sqlite.runUpdate("UPDATE graph SET graph_status_idgraph_status = " + to_string(Conts::GRAPH_STATUS::DELETING) +
                     " WHERE idgraph = " + graphID);

    JasmineGraphServer *jasmineServer = new JasmineGraphServer();
    jasmineServer->removeGraph(hostHasPartition, graphID, masterIP);

    sqlite.runUpdate("DELETE FROM worker_has_partition WHERE partition_graph_idgraph = " + graphID);
    sqlite.runUpdate("DELETE FROM partition WHERE graph_idgraph = " + graphID);
    sqlite.runUpdate("DELETE FROM graph WHERE idgraph = " + graphID);
}

/**
 * This method checks whether the graph is active and trained
 * @param graphID
 * @param dummyPt
 * @return
 */
bool JasmineGraphFrontEnd::isGraphActiveAndTrained(std::string graphID, SQLiteDBInterface sqlite) {
    bool result = true;
    string stmt = "SELECT COUNT( * ) FROM graph WHERE idgraph LIKE '" + graphID +
                  "' AND graph_status_idgraph_status = '" + to_string(Conts::GRAPH_STATUS::OPERATIONAL) +
                  "' AND train_status = '" + (Conts::TRAIN_STATUS::TRAINED) + "';";
    std::vector<vector<pair<string, string>>> v = sqlite.runSelect(stmt);
    int count = std::stoi(v[0][0].second);
    if (count == 0) {
        result = false;
    }
    return result;
}

/**
 * This method checks whether the graph is active
 * @param graphID
 * @param dummyPt
 * @return
 */
bool JasmineGraphFrontEnd::isGraphActive(std::string graphID, SQLiteDBInterface sqlite) {
    bool result = false;
    string stmt = "SELECT COUNT( * ) FROM graph WHERE idgraph LIKE '" + graphID +
                  "' AND graph_status_idgraph_status = '" + to_string(Conts::GRAPH_STATUS::OPERATIONAL) + "';";
    std::vector<vector<pair<string, string>>> v = sqlite.runSelect(stmt);
    int count = std::stoi(v[0][0].second);
    if (count != 0) {
        result = true;
    }
    return result;
}

void JasmineGraphFrontEnd::getAndUpdateUploadTime(std::string graphID, SQLiteDBInterface sqlite) {
    struct tm tm;
    vector<vector<pair<string, string>>> uploadStartFinishTimes =
        sqlite.runSelect("SELECT upload_start_time,upload_end_time FROM graph WHERE idgraph = '" + graphID + "'");
    string startTime = uploadStartFinishTimes[0][0].second;
    string endTime = uploadStartFinishTimes[0][1].second;
    string sTime = startTime.substr(startTime.size() - 14, startTime.size() - 5);
    string eTime = endTime.substr(startTime.size() - 14, startTime.size() - 5);
    strptime(sTime.c_str(), "%H:%M:%S", &tm);
    time_t start = mktime(&tm);
    strptime(eTime.c_str(), "%H:%M:%S", &tm);
    time_t end = mktime(&tm);
    double difTime = difftime(end, start);
    sqlite.runUpdate("UPDATE graph SET upload_time = " + to_string(difTime) + " WHERE idgraph = " + graphID);
    frontend_logger.info("Upload time updated in the database");
}

JasmineGraphHashMapCentralStore JasmineGraphFrontEnd::loadCentralStore(std::string centralStoreFileName) {
    frontend_logger.info("Loading Central Store File : Started " + centralStoreFileName);
    JasmineGraphHashMapCentralStore *jasmineGraphHashMapCentralStore = new JasmineGraphHashMapCentralStore();
    jasmineGraphHashMapCentralStore->loadGraph(centralStoreFileName);
    frontend_logger.info("Loading Central Store File : Completed");
    return *jasmineGraphHashMapCentralStore;
}

map<long, long> JasmineGraphFrontEnd::getOutDegreeDistributionHashMap(map<long, unordered_set<long>> graphMap) {
    map<long, long> distributionHashMap;

    for (map<long, unordered_set<long>>::iterator it = graphMap.begin(); it != graphMap.end(); ++it) {
        long distribution = (it->second).size();
        distributionHashMap.insert(std::make_pair(it->first, distribution));
    }
    return distributionHashMap;
}

int JasmineGraphFrontEnd::getUid() {
    static std::atomic<std::uint32_t> uid{0};
    return ++uid;
}

long JasmineGraphFrontEnd::getSLAForGraphId(SQLiteDBInterface sqlite, PerformanceSQLiteDBInterface perfSqlite,
                                            std::string graphId, std::string command, std::string category) {
    long graphSLAValue = 0;

    string sqlStatement =
        "SELECT worker_idworker, name,ip,user,server_port,server_data_port,partition_idpartition "
        "FROM worker_has_partition INNER JOIN worker ON worker_has_partition.worker_idworker=worker.idworker "
        "WHERE partition_graph_idgraph=" +
        graphId + ";";

    std::vector<vector<pair<string, string>>> results = sqlite.runSelect(sqlStatement);

    int partitionCount = results.size();

    string graphSlaQuery =
        "select graph_sla.sla_value from graph_sla,sla_category where graph_sla.id_sla_category=sla_category.id "
        "and sla_category.command='" +
        command + "' and sla_category.category='" + category +
        "' and "
        "graph_sla.graph_id='" +
        graphId + "' and graph_sla.partition_count='" + std::to_string(partitionCount) + "';";

    std::vector<vector<pair<string, string>>> slaResults = perfSqlite.runSelect(graphSlaQuery);

    if (slaResults.size() > 0) {
        string currentSlaString = slaResults[0][0].second;
        long graphSLAValue = atol(currentSlaString.c_str());
    }

    return graphSLAValue;
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

void JasmineGraphServer::pageRank(std::string graphID, double alpha, int iterations) {
    std::map<std::string, JasmineGraphServer::workerPartition> graphPartitionedHosts =
        JasmineGraphServer::getWorkerPartitions(graphID);
    int partition_count = 0;
    string partition;
    string host;
    int port;
    int dataPort;
    std::string workerList;

    std::map<std::string, JasmineGraphServer::workerPartition>::iterator workerIter;
    for (workerIter = graphPartitionedHosts.begin(); workerIter != graphPartitionedHosts.end(); workerIter++) {
        JasmineGraphServer::workerPartition workerPartition = workerIter->second;
        partition = workerPartition.partitionID;
        host = workerPartition.hostname;
        port = workerPartition.port;
        dataPort = workerPartition.dataPort;

        if (host.find('@') != std::string::npos) {
            host = Utils::split(host, '@')[1];
        }

        workerList.append(host + ":" + std::to_string(port) + ":" + partition + ",");
    }

    workerList.pop_back();
    frontend_logger.error("Worker list " + workerList);

    for (workerIter = graphPartitionedHosts.begin(); workerIter != graphPartitionedHosts.end(); workerIter++) {
        JasmineGraphServer::workerPartition workerPartition = workerIter->second;
        partition = workerPartition.partitionID;
        host = workerPartition.hostname;
        port = workerPartition.port;
        dataPort = workerPartition.dataPort;

        if (host.find('@') != std::string::npos) {
            host = Utils::split(host, '@')[1];
        }

        int sockfd;
        char data[301];
        bool loop = false;
        socklen_t len;
        struct sockaddr_in serv_addr;
        struct hostent *server;

        sockfd = socket(AF_INET, SOCK_STREAM, 0);

        if (sockfd < 0) {
            std::cout << "Cannot accept connection" << std::endl;
        }
        server = gethostbyname(host.c_str());
        if (server == NULL) {
            std::cout << "ERROR, no host named " << server << std::endl;
        }

        bzero((char *)&serv_addr, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
        serv_addr.sin_port = htons(port);
        if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
            std::cout << "ERROR connecting" << std::endl;
            // TODO::exit
        }

        bzero(data, 301);
        int result_wr = write(sockfd, JasmineGraphInstanceProtocol::PAGE_RANK.c_str(),
                              JasmineGraphInstanceProtocol::PAGE_RANK.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
        }

        frontend_logger.info("Sent : " + JasmineGraphInstanceProtocol::PAGE_RANK);
        bzero(data, 301);
        read(sockfd, data, 300);
        string response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            frontend_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
        } else {
            frontend_logger.error("Error reading from socket");
        }

        result_wr = write(sockfd, graphID.c_str(), graphID.size());

        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
        }
        frontend_logger.info("Sent : Graph ID " + graphID);

        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            frontend_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
        } else {
            frontend_logger.error("Error reading from socket");
        }

        int partitionID = stoi(partition);

        result_wr = write(sockfd, std::to_string(partitionID).c_str(), std::to_string(partitionID).size());

        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
        }

        frontend_logger.info("Sent : Partition ID " + std::to_string(partitionID));

        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            frontend_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
        } else {
            frontend_logger.error("Error reading from socket");
        }

        result_wr = write(sockfd, workerList.c_str(), workerList.size());

        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
        }

        frontend_logger.info("Sent : Host List ");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            frontend_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
        } else {
            frontend_logger.error("Error reading from socket");
        }

        long graphVertexCount = JasmineGraphServer::getGraphVertexCount(graphID);
        result_wr = write(sockfd, std::to_string(graphVertexCount).c_str(), std::to_string(graphVertexCount).size());

        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
        }

        frontend_logger.info("graph vertex count: " + std::to_string(graphVertexCount));
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            frontend_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
        } else {
            frontend_logger.error("Error reading from socket");
        }

        result_wr = write(sockfd, std::to_string(alpha).c_str(), std::to_string(alpha).size());

        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
        }

        frontend_logger.info("page rank alpha value sent : " + std::to_string(alpha));
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            frontend_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
        } else {
            frontend_logger.error("Error reading from socket");
        }

        result_wr = write(sockfd, std::to_string(iterations).c_str(), std::to_string(iterations).size());

        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
        }

        frontend_logger.info("page rank iterations value sent : " + std::to_string(iterations));
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            frontend_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
        } else {
            frontend_logger.error("Error reading from socket");
        }
    }
}

void JasmineGraphServer::egoNet(std::string graphID) {
    std::map<std::string, JasmineGraphServer::workerPartition> graphPartitionedHosts =
        JasmineGraphServer::getWorkerPartitions(graphID);
    int partition_count = 0;
    string partition;
    string host;
    int port;
    int dataPort;
    std::string workerList;

    std::map<std::string, JasmineGraphServer::workerPartition>::iterator workerit;
    for (workerit = graphPartitionedHosts.begin(); workerit != graphPartitionedHosts.end(); workerit++) {
        JasmineGraphServer::workerPartition workerPartition = workerit->second;
        partition = workerPartition.partitionID;
        host = workerPartition.hostname;
        port = workerPartition.port;
        dataPort = workerPartition.dataPort;

        if (host.find('@') != std::string::npos) {
            host = Utils::split(host, '@')[1];
        }

        workerList.append(host + ":" + std::to_string(port) + ":" + partition + ",");
    }

    workerList.pop_back();

    int sockfd;
    char data[301];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cout << "Cannot accept connection" << std::endl;
    }
    server = gethostbyname(host.c_str());
    if (server == NULL) {
        std::cout << "ERROR, no host named " << server << std::endl;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cout << "ERROR connecting" << std::endl;
        // TODO::exit
    }

    bzero(data, 301);
    int result_wr =
        write(sockfd, JasmineGraphInstanceProtocol::EGONET.c_str(), JasmineGraphInstanceProtocol::EGONET.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
    }

    frontend_logger.info("Sent : " + JasmineGraphInstanceProtocol::EGONET);
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);
    response = Utils::trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
        frontend_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
    } else {
        frontend_logger.error("Error reading from socket");
    }

    result_wr = write(sockfd, graphID.c_str(), graphID.size());

    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
    }
    frontend_logger.info("Sent : Graph ID " + graphID);

    bzero(data, 301);
    read(sockfd, data, 300);
    response = (data);
    response = Utils::trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
        frontend_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
    } else {
        frontend_logger.error("Error reading from socket");
    }

    int partitionID = stoi(partition);

    result_wr = write(sockfd, std::to_string(partitionID).c_str(), std::to_string(partitionID).size());

    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
    }

    frontend_logger.info("Sent : Partition ID " + std::to_string(partitionID));

    bzero(data, 301);
    read(sockfd, data, 300);
    response = (data);
    response = Utils::trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
        frontend_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
    } else {
        frontend_logger.error("Error reading from socket");
    }

    result_wr = write(sockfd, workerList.c_str(), workerList.size());

    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
    }

    frontend_logger.info("Sent : Host List ");

    if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
        frontend_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
    } else {
        frontend_logger.error("Error reading from socket");
    }
}

bool JasmineGraphFrontEnd::modelExists(string path, SQLiteDBInterface sqlite) {
    bool result = true;
    string stmt = "SELECT COUNT( * ) FROM model WHERE upload_path LIKE '" + path +
                  "' AND model_status_idmodel_status = '" + to_string(Conts::GRAPH_STATUS::OPERATIONAL) + "';";
    std::vector<vector<pair<string, string>>> v = sqlite.runSelect(stmt);
    int count = std::stoi(v[0][0].second);
    if (count == 0) {
        result = false;
    }
    return result;
}

bool JasmineGraphFrontEnd::modelExistsByID(string id, SQLiteDBInterface sqlite) {
    bool result = true;
    string stmt = "SELECT COUNT( * ) FROM model WHERE idmodel = " + id +
                  " and model_status_idmodel_status = " + to_string(Conts::GRAPH_STATUS::OPERATIONAL);
    std::vector<vector<pair<string, string>>> v = sqlite.runSelect(stmt);
    int count = std::stoi(v[0][0].second);

    if (count == 0) {
        result = false;
    }

    return result;
}

static void list_command(int connFd, SQLiteDBInterface sqlite, bool *loop_exit_p) {
    std::stringstream ss;
    std::vector<vector<pair<string, string>>> v =
        sqlite.runSelect("SELECT idgraph, name, upload_path, graph_status_idgraph_status FROM graph;");
    for (std::vector<vector<pair<string, string>>>::iterator i = v.begin(); i != v.end(); ++i) {
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
            } else {
                ss << j->second << "|";
            }
            counter++;
        }
        ss << "\r\n";
    }
    string result = ss.str();
    if (result.size() == 0) {
        int result_wr = write(connFd, EMPTY.c_str(), EMPTY.length());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }

        result_wr = write(connFd, "\r\n", 2);
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

static void add_rdf_command(std::string masterIP, int connFd, SQLiteDBInterface sqlite, bool *loop_exit_p) {
    // add RDF graph
    int result_wr = write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, "\r\n", 2);
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

    gData = Utils::trim_copy(gData, " \f\n\r\t\v");
    frontend_logger.info("Data received: " + gData);

    std::vector<std::string> strArr = Utils::split(gData, '|');

    if (strArr.size() != 2) {
        frontend_logger.error("Message format not recognized");
        // TODO: Inform client?
        return;
    }

    name = strArr[0];
    path = strArr[1];

    if (JasmineGraphFrontEnd::graphExists(path, sqlite)) {
        frontend_logger.error("Graph exists");
        // TODO: Inform client?
        return;
    }

    if (Utils::fileExists(path)) {
        frontend_logger.info("Path exists");

        string sqlStatement =
            "INSERT INTO graph (name,upload_path,upload_start_time,upload_end_time,graph_status_idgraph_status,"
            "vertexcount,centralpartitioncount,edgecount) VALUES(\"" +
            name + "\", \"" + path + "\", \"" + uploadStartTime + "\", \"\",\"" +
            to_string(Conts::GRAPH_STATUS::LOADING) + "\", \"\", \"\", \"\")";
        int newGraphID = sqlite.runInsert(sqlStatement);

        GetConfig appConfig;
        appConfig.readConfigFile(path, newGraphID);

        MetisPartitioner *metisPartitioner = new MetisPartitioner(&sqlite);
        vector<std::map<int, string>> fullFileList;
        string input_file_path =
            Utils::getHomeDir() + "/.jasminegraph/tmp/" + to_string(newGraphID) + "/" + to_string(newGraphID);
        metisPartitioner->loadDataSet(input_file_path, newGraphID);

        metisPartitioner->constructMetisFormat(Conts::GRAPH_TYPE_RDF);
        fullFileList = metisPartitioner->partitioneWithGPMetis("");
        JasmineGraphServer *jasmineServer = new JasmineGraphServer();
        jasmineServer->uploadGraphLocally(newGraphID, Conts::GRAPH_WITH_ATTRIBUTES, fullFileList, masterIP);
        Utils::deleteDirectory(Utils::getHomeDir() + "/.jasminegraph/tmp/" + to_string(newGraphID));
        Utils::deleteDirectory("/tmp/" + std::to_string(newGraphID));
        JasmineGraphFrontEnd::getAndUpdateUploadTime(to_string(newGraphID), sqlite);
        int result_wr = write(connFd, DONE.c_str(), DONE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        result_wr = write(connFd, "\r\n", 2);
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
    } else {
        frontend_logger.error("Graph data file does not exist on the specified path");
    }
}

static void add_graph_command(std::string masterIP, int connFd, SQLiteDBInterface sqlite, bool *loop_exit_p) {
    int result_wr = write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, "\r\n", 2);
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

    gData = Utils::trim_copy(gData, " \f\n\r\t\v");
    frontend_logger.info("Data received: " + gData);

    std::vector<std::string> strArr = Utils::split(gData, '|');

    if (strArr.size() < 2) {
        frontend_logger.error("Message format not recognized");
        // TODO: inform client?
        return;
    }

    name = strArr[0];
    path = strArr[1];

    if (strArr.size() == 3) {
        partitionCount = strArr[2];
    }

    if (JasmineGraphFrontEnd::graphExists(path, sqlite)) {
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
        int newGraphID = sqlite.runInsert(sqlStatement);
        JasmineGraphServer *jasmineServer = new JasmineGraphServer();
        MetisPartitioner *partitioner = new MetisPartitioner(&sqlite);
        vector<std::map<int, string>> fullFileList;

        partitioner->loadDataSet(path, newGraphID);
        int result = partitioner->constructMetisFormat(Conts::GRAPH_TYPE_NORMAL);
        if (result == 0) {
            string reformattedFilePath = partitioner->reformatDataSet(path, newGraphID);
            partitioner->loadDataSet(reformattedFilePath, newGraphID);
            partitioner->constructMetisFormat(Conts::GRAPH_TYPE_NORMAL_REFORMATTED);
            fullFileList = partitioner->partitioneWithGPMetis(partitionCount);
        } else {
            fullFileList = partitioner->partitioneWithGPMetis(partitionCount);
        }
        frontend_logger.info("Upload done");
        jasmineServer->uploadGraphLocally(newGraphID, Conts::GRAPH_TYPE_NORMAL, fullFileList, masterIP);
        Utils::deleteDirectory(Utils::getHomeDir() + "/.jasminegraph/tmp/" + to_string(newGraphID));
        string workerCountQuery = "select count(*) from worker";
        std::vector<vector<pair<string, string>>> results = sqlite.runSelect(workerCountQuery);
        string workerCount = results[0][0].second;
        int nWorkers = atoi(workerCount.c_str());
        JasmineGraphFrontEnd::getAndUpdateUploadTime(to_string(newGraphID), sqlite);
        int result_wr = write(connFd, DONE.c_str(), DONE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        result_wr = write(connFd, "\r\n", 2);
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
    } else {
        frontend_logger.error("Graph data file does not exist on the specified path");
    }
}

static void add_graph_cust_command(std::string masterIP, int connFd, SQLiteDBInterface sqlite, bool *loop_exit_p) {
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
    result_wr = write(connFd, "\r\n", 2);
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
    result_wr = write(connFd, "\r\n", 2);
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
    result_wr = write(connFd, "\r\n", 2);

    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    char type[FRONTEND_GRAPH_TYPE_LENGTH + 1];
    bzero(type, FRONTEND_GRAPH_TYPE_LENGTH + 1);
    read(connFd, type, FRONTEND_GRAPH_TYPE_LENGTH);
    string graphType(type);
    graphType = Utils::trim_copy(graphType, " \f\n\r\t\v");

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

    gData = Utils::trim_copy(gData, " \f\n\r\t\v");
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

    if (JasmineGraphFrontEnd::graphExists(edgeListPath, sqlite)) {
        frontend_logger.error("Graph exists");
        // TODO: inform client?
        return;
    }

    if (Utils::fileExists(edgeListPath) && Utils::fileExists(attributeListPath)) {
        std::cout << "Paths exists" << endl;

        string sqlStatement =
            "INSERT INTO graph (name,upload_path,upload_start_time,upload_end_time,graph_status_idgraph_status,"
            "vertexcount,centralpartitioncount,edgecount) VALUES(\"" +
            name + "\", \"" + edgeListPath + "\", \"" + uploadStartTime + "\", \"\",\"" +
            to_string(Conts::GRAPH_STATUS::LOADING) + "\", \"\", \"\", \"\")";
        int newGraphID = sqlite.runInsert(sqlStatement);
        JasmineGraphServer *jasmineServer = new JasmineGraphServer();
        MetisPartitioner *partitioner = new MetisPartitioner(&sqlite);
        vector<std::map<int, string>> fullFileList;
        partitioner->loadContentData(attributeListPath, graphAttributeType, newGraphID, attrDataType);
        partitioner->loadDataSet(edgeListPath, newGraphID);
        int result = partitioner->constructMetisFormat(Conts::GRAPH_TYPE_NORMAL);
        if (result == 0) {
            string reformattedFilePath = partitioner->reformatDataSet(edgeListPath, newGraphID);
            partitioner->loadDataSet(reformattedFilePath, newGraphID);
            partitioner->constructMetisFormat(Conts::GRAPH_TYPE_NORMAL_REFORMATTED);
            fullFileList = partitioner->partitioneWithGPMetis("");
        } else {
            fullFileList = partitioner->partitioneWithGPMetis("");
        }
        // Graph type should be changed to identify graphs with attributes
        // because this graph type has additional attribute files to be uploaded
        jasmineServer->uploadGraphLocally(newGraphID, Conts::GRAPH_WITH_ATTRIBUTES, fullFileList, masterIP);
        Utils::deleteDirectory(Utils::getHomeDir() + "/.jasminegraph/tmp/" + to_string(newGraphID));
        Utils::deleteDirectory("/tmp/" + std::to_string(newGraphID));
        JasmineGraphFrontEnd::getAndUpdateUploadTime(to_string(newGraphID), sqlite);
        result_wr = write(connFd, DONE.c_str(), DONE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        result_wr = write(connFd, "\r\n", 2);
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
    } else {
        frontend_logger.error("Graph data file does not exist on the specified path");
    }
}

static void remove_graph_command(std::string masterIP, int connFd, SQLiteDBInterface sqlite, bool *loop_exit_p) {
    int result_wr = write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, "\r\n", 2);
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

    graphID = Utils::trim_copy(graphID, " \f\n\r\t\v");
    frontend_logger.info("Graph ID received: " + graphID);

    if (JasmineGraphFrontEnd::graphExistsByID(graphID, sqlite)) {
        frontend_logger.info("Graph with ID " + graphID + " is being deleted now");
        JasmineGraphFrontEnd::removeGraph(graphID, sqlite, masterIP);
        result_wr = write(connFd, DONE.c_str(), DONE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        result_wr = write(connFd, "\r\n", 2);
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
        result_wr = write(connFd, "\r\n", 2);
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
    }
}

static void add_model_command(int connFd, SQLiteDBInterface sqlite, bool *loop_exit_p) {
    // TODO add error handling
    int result_wr = write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, "\r\n", 2);
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

    gData = Utils::trim_copy(gData, " \f\n\r\t\v");
    frontend_logger.info("Data received: " + gData);

    std::vector<std::string> strArr = Utils::split(gData, '|');

    if (strArr.size() < 2) {
        frontend_logger.error("Message format not recognized");
        // TODO: inform client?
        return;
    }

    name = strArr[0];
    path = strArr[1];

    if (JasmineGraphFrontEnd::modelExists(path, sqlite)) {
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

        int newModelID = sqlite.runInsert(sqlStatement);

        frontend_logger.info("Upload done");
        result_wr = write(connFd, DONE.c_str(), DONE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        result_wr = write(connFd, "\r\n", 2);
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
    } else {
        frontend_logger.error("Model file does not exist on the specified path");
    }
}

static void add_stream_kafka_command(int connFd, std::string &kafka_server_IP, cppkafka::Configuration &configs,
                                     KafkaConnector *&kstream, thread &input_stream_handler,
                                     vector<DataPublisher *> &workerClients, int numberOfPartitions,
                                     bool *loop_exit_p) {
    string msg_1 = "DO you want to use default KAFKA consumer(y/n) ? ";
    int result_wr = write(connFd, msg_1.c_str(), msg_1.length());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, "\r\n", 2);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    // Get user response.
    char user_res[FRONTEND_DATA_LENGTH + 1];
    bzero(user_res, FRONTEND_DATA_LENGTH + 1);
    read(connFd, user_res, FRONTEND_DATA_LENGTH);
    string user_res_s(user_res);
    user_res_s = Utils::trim_copy(user_res_s, " \f\n\r\t\v");
    for (char &c : user_res_s) {
        c = tolower(c);
    }
    //          use default kafka consumer details
    if (user_res_s == "y") {
        kafka_server_IP = Utils::getJasmineGraphProperty("org.jasminegraph.server.streaming.kafka.host");
        configs = {{"metadata.broker.list", kafka_server_IP}, {"group.id", "knnect"}};
    } else {
        // user need to start relevant kafka cluster using relevant IP address
        // read relevant IP address from given file path
        string message = "Send file path to the kafka configuration file.";
        int result_wr = write(connFd, message.c_str(), message.length());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        result_wr = write(connFd, "\r\n", 2);
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
        file_path_s = Utils::trim_copy(file_path_s, " \f\n\r\t\v");
        // reading kafka_server IP from the given file.
        std::vector<std::string>::iterator it;
        vector<std::string> vec = Utils::getFileContent(file_path_s);
        it = vec.begin();
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
        configs = {{"metadata.broker.list", kafka_server_IP}, {"group.id", "knnect"}};
    }

    frontend_logger.info("Start serving `" + ADD_STREAM_KAFKA + "` command");
    string message = "send kafka topic name";
    result_wr = write(connFd, message.c_str(), message.length());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, "\r\n", 2);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    // We get the topic name here.
    char topic_name[FRONTEND_DATA_LENGTH + 1];
    bzero(topic_name, FRONTEND_DATA_LENGTH + 1);
    read(connFd, topic_name, FRONTEND_DATA_LENGTH);

    string con_message = "Received the kafka topic";
    int con_result_wr = write(connFd, con_message.c_str(), con_message.length());
    if (con_result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    //          create kafka consumer and graph partitioner
    kstream = new KafkaConnector(configs);
    Partitioner graphPartitioner(numberOfPartitions, 1, spt::Algorithms::HASH);

    string topic_name_s(topic_name);
    topic_name_s = Utils::trim_copy(topic_name_s, " \f\n\r\t\v");
    stream_topic_name = topic_name_s;
    kstream->Subscribe(topic_name_s);
    frontend_logger.info("Start listening to " + topic_name_s);
    input_stream_handler = thread(listen_to_kafka_topic, kstream, std::ref(graphPartitioner), std::ref(workerClients));
}

static void stop_stream_kafka_command(int connFd, KafkaConnector *kstream, bool *loop_exit_p) {
    frontend_logger.info("Start serving `" + STOP_STREAM_KAFKA + "` command");
    //          Unsubscribe the kafka consumer.
    kstream->Unsubscribe();
    string message = "Successfully stop `" + stream_topic_name + "` input kafka stream";
    int result_wr = write(connFd, message.c_str(), message.length());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, "\r\n", 2);

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
    result_wr = write(connFd, "\r\n", 2);
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

    gData = Utils::trim_copy(gData, " \f\n\r\t\v");
    frontend_logger.info("Data received: " + gData);

    if (gData.length() == 0) {
        frontend_logger.error("Message format not recognized");
        // TODO: Inform client?
        return;
    }
    string path = gData;

    if (Utils::fileExists(path)) {
        frontend_logger.info("Path exists");

        JSONParser *jsonParser = new JSONParser();
        jsonParser->jsonParse(path);
        frontend_logger.info("Reformatted files created on /home/.jasminegraph/tmp/JSONParser/output");
    } else {
        frontend_logger.error("Graph data file does not exist on the specified path");
    }
}

static void triangles_command(std::string masterIP, int connFd, SQLiteDBInterface sqlite,
                              PerformanceSQLiteDBInterface perfSqlite, JobScheduler jobScheduler, bool *loop_exit_p) {
    // add RDF graph
    int uniqueId = JasmineGraphFrontEnd::getUid();
    int result_wr = write(connFd, GRAPHID_SEND.c_str(), FRONTEND_COMMAND_LENGTH);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, "\r\n", 2);
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

    if (!JasmineGraphFrontEnd::graphExistsByID(graph_id, sqlite)) {
        string error_message = "The specified graph id does not exist";
        result_wr = write(connFd, error_message.c_str(), FRONTEND_COMMAND_LENGTH);
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }

        result_wr = write(connFd, "\r\n", 2);
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
        result_wr = write(connFd, "\r\n", 2);
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }

        // We get the name and the path to graph as a pair separated by |.
        char priority_data[301];
        bzero(priority_data, 301);

        read(connFd, priority_data, FRONTEND_DATA_LENGTH);

        string priority(priority_data);

        priority = Utils::trim_copy(priority, " \f\n\r\t\v");

        if (!(std::find_if(priority.begin(), priority.end(), [](unsigned char c) { return !std::isdigit(c); }) ==
              priority.end())) {
            *loop_exit_p = true;
            string error_message = "Priority should be numeric and > 1 or empty";
            result_wr = write(connFd, error_message.c_str(), error_message.length());
            if (result_wr < 0) {
                frontend_logger.error("Error writing to socket");
                return;
            }

            result_wr = write(connFd, "\r\n", 2);
            if (result_wr < 0) {
                frontend_logger.error("Error writing to socket");
            }
            return;
        }

        int threadPriority = std::atoi(priority.c_str());

        auto begin = chrono::high_resolution_clock::now();
        JobRequest jobDetails;
        jobDetails.setJobId(std::to_string(uniqueId));
        jobDetails.setJobType(TRIANGLES);

        long graphSLA;
        // All high priority threads will be set the same high priority level
        if (threadPriority > Conts::DEFAULT_THREAD_PRIORITY) {
            threadPriority = Conts::HIGH_PRIORITY_DEFAULT_VALUE;
            graphSLA = JasmineGraphFrontEnd::getSLAForGraphId(sqlite, perfSqlite, graph_id, TRIANGLES,
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

        jobScheduler.pushJob(jobDetails);
        JobResponse jobResponse = jobScheduler.getResult(jobDetails);
        std::string errorMessage = jobResponse.getParameter(Conts::PARAM_KEYS::ERROR_MESSAGE);

        if (!errorMessage.empty()) {
            *loop_exit_p = true;
            result_wr = write(connFd, errorMessage.c_str(), errorMessage.length());

            if (result_wr < 0) {
                frontend_logger.error("Error writing to socket");
                return;
            }
            result_wr = write(connFd, "\r\n", 2);
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
        frontend_logger.info("Triangle Count: " + triangleCount + " Time Taken: " + to_string(msDuration) +
                             " milliseconds");
        result_wr = write(connFd, triangleCount.c_str(), triangleCount.length());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        result_wr = write(connFd, "\r\n", 2);
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
    }
}

static void vertex_count_command(int connFd, SQLiteDBInterface sqlite, bool *loop_exit_p) {
    int result_wr = write(connFd, GRAPHID_SEND.c_str(), GRAPHID_SEND.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, "\r\n", 2);
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

    if (!JasmineGraphFrontEnd::graphExistsByID(graph_id, sqlite)) {
        string error_message = "The specified graph id does not exist";
        result_wr = write(connFd, error_message.c_str(), FRONTEND_COMMAND_LENGTH);
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        result_wr = write(connFd, "\r\n", 2);
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
    } else {
        string sqlStatement = "SELECT vertexcount from graph where idgraph=" + graph_id;

        std::vector<vector<pair<string, string>>> output = sqlite.runSelect(sqlStatement);

        int vertexCount = std::stoi(output[0][0].second);
        frontend_logger.info("Vertex Count: " + to_string(vertexCount));
        result_wr = write(connFd, to_string(vertexCount).c_str(), to_string(vertexCount).length());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        result_wr = write(connFd, "\r\n", 2);
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
    }
}

static void edge_count_command(int connFd, SQLiteDBInterface sqlite, bool *loop_exit_p) {
    int result_wr = write(connFd, GRAPHID_SEND.c_str(), GRAPHID_SEND.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, "\r\n", 2);
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

    if (!JasmineGraphFrontEnd::graphExistsByID(graph_id, sqlite)) {
        string error_message = "The specified graph id does not exist";
        result_wr = write(connFd, error_message.c_str(), FRONTEND_COMMAND_LENGTH);
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        result_wr = write(connFd, "\r\n", 2);
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
    } else {
        string sqlStatement = "SELECT edgecount from graph where idgraph=" + graph_id;

        std::vector<vector<pair<string, string>>> output = sqlite.runSelect(sqlStatement);

        int edgeCount = std::stoi(output[0][0].second);
        frontend_logger.info("Edge Count: " + to_string(edgeCount));
        result_wr = write(connFd, to_string(edgeCount).c_str(), to_string(edgeCount).length());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        result_wr = write(connFd, "\r\n", 2);
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
    }
}

static void merge_command(int connFd, SQLiteDBInterface sqlite, bool *loop_exit_p) {
    std::string federatedEnabled = Utils::getJasmineGraphProperty("org.jasminegraph.federated.enabled");
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
    result_wr = write(connFd, "\r\n", 2);
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
    trainData = Utils::trim_copy(trainData, " \f\n\r\t\v");
    frontend_logger.info("Data received: " + trainData);

    std::vector<std::string> trainargs = Utils::split(trainData, ' ');
    std::vector<std::string>::iterator itr = std::find(trainargs.begin(), trainargs.end(), "--graph_id");
    std::string graphID;

    if (itr != trainargs.cend()) {
        int index = std::distance(trainargs.begin(), itr);
        graphID = trainargs[index + 1];

    } else {
        frontend_logger.error("graph_id should be given as an argument");
        // TODO: Inform client?
        return;
    }

    if (trainargs.size() == 0) {
        frontend_logger.error("Message format not recognized");
        // TODO: Inform client?
        return;
    }

    JasmineGraphServer *jasmineServer = new JasmineGraphServer();
    jasmineServer->initiateFiles(graphID, trainData);
    jasmineServer->initiateMerge(graphID, trainData, sqlite);
    result_wr = write(connFd, DONE.c_str(), FRONTEND_COMMAND_LENGTH);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, "\r\n", 2);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
    }
}

static void train_command(int connFd, SQLiteDBInterface sqlite, bool *loop_exit_p) {
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
    result_wr = write(connFd, "\r\n", 2);
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
    trainData = Utils::trim_copy(trainData, " \f\n\r\t\v");
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
        // TODO: Inform client?
        return;
    }

    if (trainargs.size() == 0) {
        frontend_logger.error("Message format not recognized");
        // TODO: Inform client?
        return;
    }

    if (!JasmineGraphFrontEnd::isGraphActive(graphID, sqlite)) {
        string error_message = "Graph is not in the active status";
        frontend_logger.error(error_message);
        result_wr = write(connFd, error_message.c_str(), error_message.length());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        result_wr = write(connFd, "\r\n", 2);
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
        return;
    }

    std::string federatedEnabled = Utils::getJasmineGraphProperty("org.jasminegraph.federated.enabled");

    if (federatedEnabled == "true") {
        JasmineGraphServer *jasmineServer = new JasmineGraphServer();
        if (Utils::getJasmineGraphProperty("org.jasminegraph.fl.org.training") == "true") {
            frontend_logger.info("Initiate org communication");
            jasmineServer->initiateOrgCommunication(graphID, trainData, sqlite);
        } else {
            frontend_logger.info("Initiate communication");
            jasmineServer->initiateCommunication(graphID, trainData, sqlite);
        }
    } else {
        JasminGraphTrainingInitiator *jasminGraphTrainingInitiator = new JasminGraphTrainingInitiator();
        jasminGraphTrainingInitiator->initiateTrainingLocally(graphID, trainData);
    }

    result_wr = write(connFd, DONE.c_str(), FRONTEND_COMMAND_LENGTH);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, "\r\n", 2);
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
    result_wr = write(connFd, "\r\n", 2);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    char graph_id[FRONTEND_DATA_LENGTH + 1];
    bzero(graph_id, FRONTEND_DATA_LENGTH + 1);

    read(connFd, graph_id, FRONTEND_DATA_LENGTH);

    string graphID(graph_id);

    graphID = Utils::trim_copy(graphID, " \f\n\r\t\v");
    frontend_logger.info("Graph ID received: " + graphID);

    JasmineGraphServer *jasmineServer = new JasmineGraphServer();
    jasmineServer->inDegreeDistribution(graphID);

    result_wr = write(connFd, DONE.c_str(), FRONTEND_COMMAND_LENGTH);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, "\r\n", 2);
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
    result_wr = write(connFd, "\r\n", 2);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    char graph_id[FRONTEND_DATA_LENGTH + 1];
    bzero(graph_id, FRONTEND_DATA_LENGTH + 1);

    read(connFd, graph_id, FRONTEND_DATA_LENGTH);

    string graphID(graph_id);

    graphID = Utils::trim_copy(graphID, " \f\n\r\t\v");
    frontend_logger.info("Graph ID received: " + graphID);

    JasmineGraphServer *jasmineServer = new JasmineGraphServer();
    jasmineServer->outDegreeDistribution(graphID);

    result_wr = write(connFd, DONE.c_str(), FRONTEND_COMMAND_LENGTH);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, "\r\n", 2);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
    }
}

static void page_rank_command(int connFd, bool *loop_exit_p) {
    frontend_logger.info("Calculating Page Rank");

    int result_wr = write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, "\r\n", 2);
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
            // TODO: Inform client?
            return;
        }
    }

    int iterations = PAGE_RANK_ITERATIONS;
    if (strArr.size() > 2) {
        iterations = std::stod(strArr[2]);
        if (iterations <= 0 || iterations >= 100) {
            frontend_logger.error("Invalid value for iterations");
            // TODO: Inform client?
            return;
        }
    }

    graphID = Utils::trim_copy(graphID, " \f\n\r\t\v");
    frontend_logger.info("Graph ID received: " + graphID);
    frontend_logger.info("Alpha value: " + to_string(alpha));
    frontend_logger.info("Iterations value: " + to_string(iterations));

    JasmineGraphServer *jasmineServer = new JasmineGraphServer();
    jasmineServer->pageRank(graphID, alpha, iterations);

    result_wr = write(connFd, DONE.c_str(), FRONTEND_COMMAND_LENGTH);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, "\r\n", 2);
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
    result_wr = write(connFd, "\r\n", 2);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    char graph_id[FRONTEND_DATA_LENGTH + 1];
    bzero(graph_id, FRONTEND_DATA_LENGTH + 1);

    read(connFd, graph_id, FRONTEND_DATA_LENGTH);

    string graphID(graph_id);

    graphID = Utils::trim_copy(graphID, " \f\n\r\t\v");
    frontend_logger.info("Graph ID received: " + graphID);

    JasmineGraphServer *jasmineServer = new JasmineGraphServer();
    jasmineServer->egoNet(graphID);

    result_wr = write(connFd, DONE.c_str(), FRONTEND_COMMAND_LENGTH);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, "\r\n", 2);
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
    result_wr = write(connFd, "\r\n", 2);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    char graph_id[FRONTEND_DATA_LENGTH + 1];
    bzero(graph_id, FRONTEND_DATA_LENGTH + 1);

    read(connFd, graph_id, FRONTEND_DATA_LENGTH);

    string graphID(graph_id);

    graphID = Utils::trim_copy(graphID, " \f\n\r\t\v");
    frontend_logger.info("Graph ID received: " + graphID);

    JasmineGraphServer *jasmineServer = new JasmineGraphServer();
    jasmineServer->duplicateCentralStore(graphID);

    result_wr = write(connFd, DONE.c_str(), FRONTEND_COMMAND_LENGTH);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, "\r\n", 2);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
    }
}

static void predict_command(std::string masterIP, int connFd, SQLiteDBInterface sqlite, bool *loop_exit_p) {
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
        result_wr = write(connFd, "\r\n", 3);
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

        predictData = Utils::trim_copy(predictData, " \f\n\r\t\v");
        frontend_logger.info("Data received: " + predictData);

        std::vector<std::string> strArr = Utils::split(predictData, '|');

        if (strArr.size() != 3) {
            frontend_logger.error("Message format not recognized");
            // TODO: Inform client?
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
        result_wr = write(connFd, "\r\n", 2);
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

        predictData = Utils::trim_copy(predictData, " \f\n\r\t\v");
        frontend_logger.info("Data received: " + predictData);

        std::vector<std::string> strArr = Utils::split(predictData, '|');

        if (strArr.size() != 2) {
            frontend_logger.error("Message format not recognized");
            // TODO: Inform client?
            return;
        }

        graphID = strArr[0];
        path = strArr[1];

        if (JasmineGraphFrontEnd::isGraphActiveAndTrained(graphID, sqlite)) {
            if (Utils::fileExists(path)) {
                std::cout << "Path exists" << endl;
                JasminGraphLinkPredictor *jasminGraphLinkPredictor = new JasminGraphLinkPredictor();
                jasminGraphLinkPredictor->initiateLinkPrediction(graphID, path, masterIP);
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
    result_wr = write(connFd, "\r\n", 2);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    char worker_data[301];
    bzero(worker_data, 301);
    read(connFd, worker_data, 300);
    string remote_worker_data(worker_data);

    remote_worker_data = Utils::trim_copy(remote_worker_data, " \f\n\r\t\v");
    frontend_logger.info("Data received: " + remote_worker_data);
    string host = "";
    string port = "";
    string dataPort = "";
    string profile = "";
    string masterHost = "";
    string enableNmon = "";

    std::vector<std::string> strArr = Utils::split(remote_worker_data, '|');

    if (strArr.size() < 6) {
        frontend_logger.error("Message format not recognized");
        // TODO: Inform client?
        return;
    }

    host = strArr[0];
    port = strArr[1];
    dataPort = strArr[2];
    profile = strArr[3];
    masterHost = strArr[4];
    enableNmon = strArr[5];

    JasmineGraphServer *jasmineServer = new JasmineGraphServer();
    bool isSpawned = jasmineServer->spawnNewWorker(host, port, dataPort, profile, masterHost, enableNmon);
}

static void sla_command(int connFd, SQLiteDBInterface sqlite, PerformanceSQLiteDBInterface perfSqlite,
                        bool *loop_exit_p) {
    int result_wr = write(connFd, COMMAND.c_str(), COMMAND.size());
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }
    result_wr = write(connFd, "\r\n", 2);
    if (result_wr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return;
    }

    char category[FRONTEND_DATA_LENGTH + 1];
    bzero(category, FRONTEND_DATA_LENGTH + 1);
    read(connFd, category, FRONTEND_DATA_LENGTH);
    string command_info(category);

    command_info = Utils::trim_copy(command_info, " \f\n\r\t\v");
    frontend_logger.info("Data received: " + command_info);

    std::vector<vector<pair<string, string>>> categoryResults =
        perfSqlite.runSelect("SELECT id FROM sla_category where command='" + command_info + "';");

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
        perfSqlite.runSelect("SELECT graph_id, partition_count, sla_value FROM graph_sla where id_sla_category in (" +
                             adjustedIdList + ");");
    for (std::vector<vector<pair<string, string>>>::iterator i = v.begin(); i != v.end(); ++i) {
        std::stringstream slass;
        slass << "|";
        int counter = 0;
        for (std::vector<pair<string, string>>::iterator j = (i->begin()); j != i->end(); ++j) {
            if (counter == 0) {
                std::string graphId = j->second;
                std::string graphQuery = "SELECT name FROM graph where idgraph='" + graphId + "';";
                std::vector<vector<pair<string, string>>> graphData = sqlite.runSelect(graphQuery);
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
        result_wr = write(connFd, "\r\n", 2);

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
