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

#include "JasmineGraphInstanceService.h"

#include <signal.h>
#include <stdio.h>
#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <cmath>
#include <string>

#include "../query/algorithms/triangles/StreamingTriangles.h"
#include "../query/processor/cypher/runtime/InstanceHandler.h"
#include "../server/JasmineGraphServer.h"
#include "../util/kafka/InstanceStreamHandler.h"
#include "../util/logger/Logger.h"
#include "JasmineGraphInstance.h"
#include <thread>

#include "../knowledgegraph/construction/OllamaTupleStreamer.h"
#include "../knowledgegraph/construction/VLLMTupleStreamer.h"

#include "../knowledgegraph/construction/Pipeline.h"
#include "../query/processor/semanticBeamSearch/SemanticBeamSearch.h"
#include "../util/hdfs/HDFSConnector.h"

using namespace std;

#define PENDING_CONNECTION_QUEUE_SIZE 10
#define DATA_BUFFER_SIZE (INSTANCE_DATA_LENGTH + 1)
#define CHUNK_OFFSET (INSTANCE_DATA_LENGTH - 10)

Logger instance_logger;
pthread_mutex_t file_lock;
pthread_mutex_t map_lock;
int JasmineGraphInstanceService::partitionCounter = 0;
std::map<int, std::vector<std::string>> JasmineGraphInstanceService::iterationData;
const string JasmineGraphInstanceService::END_OF_MESSAGE = "eom";
int highestPriority = Conts::DEFAULT_THREAD_PRIORITY;
std::atomic<int> workerHighPriorityTaskCount;
std::mutex threadPriorityMutex;
std::vector<std::string> loadAverageVector;
bool collectValid = false;
std::thread JasmineGraphInstanceService::workerThread;

std::string masterIP;

static void handshake_command(int connFd, bool *loop_exit_p);
static inline void close_command(int connFd, bool *loop_exit_p);
__attribute__((noreturn)) static inline void shutdown_command(int connFd);
static void ready_command(int connFd, bool *loop_exit_p);
static void batch_upload_command(int connFd, bool *loop_exit_p);
static void batch_upload_central_command(int connFd, bool *loop_exit_p);
static void batch_upload_composite_central_command(int connFd, bool *loop_exit_p);
static void upload_rdf_attributes_command(int connFd, bool *loop_exit_p);
static void upload_rdf_attributes_central_command(int connFd, bool *loop_exit_p);
static void delete_graph_command(int connFd, bool *loop_exit_p);
static void delete_graph_fragment_command(int connFd, bool *loop_exit_p);
static void duplicate_centralstore_command(int connFd, int serverPort, bool *loop_exit_p);
static void worker_in_degree_distribution_command(
    int connFd, std::map<std::string, JasmineGraphHashMapLocalStore> &graphDBMapLocalStores,
    std::map<std::string, JasmineGraphHashMapCentralStore> &graphDBMapCentralStores, bool *loop_exit_p);
static void in_degree_distribution_command(
    int connFd, int serverPort, std::map<std::string, JasmineGraphHashMapLocalStore> &graphDBMapLocalStores,
    std::map<std::string, JasmineGraphHashMapCentralStore> &graphDBMapCentralStores, bool *loop_exit_p);
static void worker_out_degree_distribution_command(
    int connFd, std::map<std::string, JasmineGraphHashMapLocalStore> &graphDBMapLocalStores,
    std::map<std::string, JasmineGraphHashMapCentralStore> &graphDBMapCentralStores, bool *loop_exit_p);
static void out_degree_distribution_command(
    int connFd, int serverPort, std::map<std::string, JasmineGraphHashMapLocalStore> &graphDBMapLocalStores,
    std::map<std::string, JasmineGraphHashMapCentralStore> &graphDBMapCentralStores, bool *loop_exit_p);
static void page_rank_command(int connFd, int serverPort,
                              std::map<std::string, JasmineGraphHashMapCentralStore> &graphDBMapCentralStores,
                              bool *loop_exit_p);
static void worker_page_rank_distribution_command(
    int connFd, int serverPort, std::map<std::string, JasmineGraphHashMapCentralStore> &graphDBMapCentralStores,
    bool *loop_exit_p);
static void egonet_command(int connFd, int serverPort,
                           std::map<std::string, JasmineGraphHashMapCentralStore> &graphDBMapCentralStores,
                           bool *loop_exit_p);
static void worker_egonet_command(int connFd, int serverPort,
                                  std::map<std::string, JasmineGraphHashMapCentralStore> &graphDBMapCentralStores,
                                  bool *loop_exit_p);
static void triangles_command(
    int connFd, int serverPort, std::map<std::string, JasmineGraphHashMapLocalStore> &graphDBMapLocalStores,
    std::map<std::string, JasmineGraphHashMapCentralStore> &graphDBMapCentralStores,
    std::map<std::string, JasmineGraphHashMapDuplicateCentralStore> &graphDBMapDuplicateCentralStores,
    bool *loop_exit_p);
static void streaming_triangles_command(
    int connFd, int serverPort, std::map<std::string, JasmineGraphIncrementalLocalStore *> &incrementalLocalStoreMap,
    bool *loop_exit_p);
static void streaming_kg_construction(
    int connFd, int serverPort, std::map<std::string, JasmineGraphIncrementalLocalStore *> &incrementalLocalStoreMap,
    bool *loop_exit_p);
static void streaming_tuple_extraction (
    int connFd, int serverPort, std::map<std::string, JasmineGraphIncrementalLocalStore *> &incrementalLocalStoreMap,
    bool *loop_exit_p);
static void send_centralstore_to_aggregator_command(int connFd, bool *loop_exit_p);
static void send_composite_centralstore_to_aggregator_command(int connFd, bool *loop_exit_p);
static void aggregate_centralstore_triangles_command(int connFd, bool *loop_exit_p);
static void aggregate_streaming_centralstore_triangles_command(
    int connFd, std::map<std::string, JasmineGraphIncrementalLocalStore *> &incrementalLocalStoreMap,
    bool *loop_exit_p);
static void aggregate_composite_centralstore_triangles_command(int connFd, bool *loop_exit_p);
static void initiate_files_command(int connFd, bool *loop_exit_p);
static void initiate_fed_predict_command(int connFd, bool *loop_exit_p);
static void initiate_server_command(int connFd, bool *loop_exit_p);
static void initiate_org_server_command(int connFd, bool *loop_exit_p);
static void initiate_aggregator_command(int connFd, bool *loop_exit_p);
static void initiate_client_command(int connFd, bool *loop_exit_p);
static void initiate_merge_files_command(int connFd, bool *loop_exit_p);
static inline void start_stat_collection_command(int connFd, bool *collectValid_p, bool *loop_exit_p);
static void request_collected_stats_command(int connFd, bool *collectValid_p, bool *loop_exit_p);
static void initiate_train_command(int connFd, bool *loop_exit_p);
static void initiate_predict_command(int connFd, instanceservicesessionargs *sessionargs, bool *loop_exit_p);
static void initiate_model_collection_command(int connFd, bool *loop_exit_p);
static void initiate_fragment_resolution_command(int connFd, bool *loop_exit_p);
static void check_file_accessible_command(int connFd, bool *loop_exit_p);
static void graph_stream_start_command(int connFd, InstanceStreamHandler &instanceStreamHandler, bool *loop_exit_p);
static void send_priority_command(int connFd, bool *loop_exit_p);
static std::string initiate_command_common(int connFd, bool *loop_exit_p);
static void batch_upload_common(int connFd, bool *loop_exit_p, bool batch_upload);
static void degree_distribution_common(int connFd, int serverPort,
                                       std::map<std::string, JasmineGraphHashMapLocalStore> &graphDBMapLocalStores,
                                       std::map<std::string, JasmineGraphHashMapCentralStore> &graphDBMapCentralStores,
                                       bool *loop_exit_p, bool in);
static void push_partition_command(int connFd, bool *loop_exit_p);
static void push_file_command(int connFd, bool *loop_exit_p);
static void query_start_command(int connFd, InstanceHandler &instanceHandler, std::map<std::string,
                                JasmineGraphIncrementalLocalStore *> &incrementalLocalStoreMap, bool *loop_exit_p);
static void semantic_beam_search(int connFd, InstanceHandler &instanceHandler, std::map<std::string,
                                JasmineGraphIncrementalLocalStore *> &incrementalLocalStoreMap, bool *loop_exit_p);

static void sub_query_start_command(int connFd, InstanceHandler &instanceHandler, std::map<std::string,
        JasmineGraphIncrementalLocalStore *> &incrementalLocalStoreMap, bool *loop_exit_p);


static void hdfs_start_stream_command(int connFd, bool *loop_exit_p, bool isLocalStream,
                                      InstanceStreamHandler &instanceStreamHandler);
long countLocalTriangles(
    std::string graphId, std::string partitionId,
    std::map<std::string, JasmineGraphHashMapLocalStore> &graphDBMapLocalStores,
    std::map<std::string, JasmineGraphHashMapCentralStore> &graphDBMapCentralStores,
    std::map<std::string, JasmineGraphHashMapDuplicateCentralStore> &graphDBMapDuplicateCentralStores,
    int threadPriority);

static void processFile(string basicString, bool isLocal, InstanceStreamHandler &handler, bool
    isEmbedGraph) ;

char *converter(const std::string &s) {
    char *pc = new char[s.size() + 1];
    std::strcpy(pc, s.c_str());
    return pc;
}


static void semantic_search_expand_node_remote_batch(int conn_fd,  map<std::string, JasmineGraphIncrementalLocalStore*>& map, bool* loop_exit);

void *instanceservicesession(void *dummyPt) {
    instanceservicesessionargs *sessionargs_p = (instanceservicesessionargs *)dummyPt;
    instanceservicesessionargs sessionargs = *sessionargs_p;
    delete sessionargs_p;
    int connFd = sessionargs.connFd;
    std::map<std::string, JasmineGraphHashMapLocalStore> *graphDBMapLocalStores = sessionargs.graphDBMapLocalStores;
    std::map<std::string, JasmineGraphHashMapCentralStore> *graphDBMapCentralStores =
        sessionargs.graphDBMapCentralStores;
    std::map<std::string, JasmineGraphHashMapDuplicateCentralStore> *graphDBMapDuplicateCentralStores =
        sessionargs.graphDBMapDuplicateCentralStores;
    std::map<std::string, JasmineGraphIncrementalLocalStore *> &incrementalLocalStoreMap =
        *(sessionargs.incrementalLocalStore);
    InstanceStreamHandler streamHandler(incrementalLocalStoreMap);
    InstanceHandler instanceHandler(incrementalLocalStoreMap);

    string serverName = sessionargs.host;
    string masterHost = sessionargs.masterHost;
    int serverPort = sessionargs.port;
    int serverDataPort = sessionargs.dataPort;

    instance_logger.info("New service session started on thread:" + to_string(pthread_self()) +
                         " connFd:" + to_string(connFd));

    char data[DATA_BUFFER_SIZE];
    bool loop_exit = false;
    while (!loop_exit) {
        string line = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, true);
        if (line.empty()) {
            sleep(1);
            continue;
        }
        line = Utils::trim_copy(line);
        instance_logger.info("Received : " + line);

        if (line.compare(JasmineGraphInstanceProtocol::HANDSHAKE) == 0) {
            handshake_command(connFd, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::CLOSE) == 0) {
            close_command(connFd, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::SHUTDOWN) == 0) {
            shutdown_command(connFd);
        } else if (line.compare(JasmineGraphInstanceProtocol::READY) == 0) {
            ready_command(connFd, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD) == 0) {
            batch_upload_command(connFd, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_CENTRAL) == 0) {
            batch_upload_central_command(connFd, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_COMPOSITE_CENTRAL) == 0) {
            batch_upload_composite_central_command(connFd, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::UPLOAD_RDF_ATTRIBUTES) == 0) {
            upload_rdf_attributes_command(connFd, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::UPLOAD_RDF_ATTRIBUTES_CENTRAL) == 0) {
            upload_rdf_attributes_central_command(connFd, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::DELETE_GRAPH) == 0) {
            delete_graph_command(connFd, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::DELETE_GRAPH_FRAGMENT) == 0) {
            delete_graph_fragment_command(connFd, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::DP_CENTRALSTORE) == 0) {
            duplicate_centralstore_command(connFd, serverPort, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::WORKER_IN_DEGREE_DISTRIBUTION) == 0) {
            worker_in_degree_distribution_command(connFd, *graphDBMapLocalStores, *graphDBMapCentralStores, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::IN_DEGREE_DISTRIBUTION) == 0) {
            in_degree_distribution_command(connFd, serverPort, *graphDBMapLocalStores, *graphDBMapCentralStores,
                                           &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::WORKER_OUT_DEGREE_DISTRIBUTION) == 0) {
            worker_out_degree_distribution_command(connFd, *graphDBMapLocalStores, *graphDBMapCentralStores,
                                                   &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::OUT_DEGREE_DISTRIBUTION) == 0) {
            out_degree_distribution_command(connFd, serverPort, *graphDBMapLocalStores, *graphDBMapCentralStores,
                                            &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::PAGE_RANK) == 0) {
            page_rank_command(connFd, serverPort, *graphDBMapCentralStores, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::WORKER_PAGE_RANK_DISTRIBUTION) == 0) {
            worker_page_rank_distribution_command(connFd, serverPort, *graphDBMapCentralStores, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::EGONET) == 0) {
            egonet_command(connFd, serverPort, *graphDBMapCentralStores, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::WORKER_EGO_NET) == 0) {
            worker_egonet_command(connFd, serverPort, *graphDBMapCentralStores, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::TRIANGLES) == 0) {
            triangles_command(connFd, serverPort, *graphDBMapLocalStores, *graphDBMapCentralStores,
                              *graphDBMapDuplicateCentralStores, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::INITIATE_STREAMING_TRIAN) == 0) {
            streaming_triangles_command(connFd, serverPort, incrementalLocalStoreMap, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::INITIATE_STREAMING_KG_CONSTRUCTION) == 0)
        {
            streaming_kg_construction(connFd, serverPort, incrementalLocalStoreMap, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::INITIATE_STREAMING_TUPLE_CONSTRUCTION) == 0) {
            streaming_tuple_extraction(connFd, serverPort, incrementalLocalStoreMap, &loop_exit);


        } else if (line.compare(JasmineGraphInstanceProtocol::SEMANTIC_BEAM_SEARCH) == 0)
        {
            semantic_beam_search(connFd, instanceHandler, incrementalLocalStoreMap, &loop_exit);

        }
        else if (line.compare(JasmineGraphInstanceProtocol::EXPAND_NODE_BATCH) == 0)
        {
            semantic_search_expand_node_remote_batch(connFd, incrementalLocalStoreMap, &loop_exit);

        }


        else if (line.compare(JasmineGraphInstanceProtocol::SEND_CENTRALSTORE_TO_AGGREGATOR) == 0) {
            send_centralstore_to_aggregator_command(connFd, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::SEND_COMPOSITE_CENTRALSTORE_TO_AGGREGATOR) == 0) {
            send_composite_centralstore_to_aggregator_command(connFd, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::AGGREGATE_CENTRALSTORE_TRIANGLES) == 0) {
            aggregate_centralstore_triangles_command(connFd, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::AGGREGATE_STREAMING_CENTRALSTORE_TRIANGLES) == 0) {
            aggregate_streaming_centralstore_triangles_command(connFd, incrementalLocalStoreMap, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::AGGREGATE_COMPOSITE_CENTRALSTORE_TRIANGLES) == 0) {
            aggregate_composite_centralstore_triangles_command(connFd, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::INITIATE_FILES) == 0) {
            initiate_files_command(connFd, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::INITIATE_FED_PREDICT) == 0) {
            initiate_fed_predict_command(connFd, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::INITIATE_SERVER) == 0) {
            initiate_server_command(connFd, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::INITIATE_ORG_SERVER) == 0) {
            initiate_org_server_command(connFd, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::INITIATE_AGG) == 0) {
            initiate_aggregator_command(connFd, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::INITIATE_CLIENT) == 0) {
            initiate_client_command(connFd, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::MERGE_FILES) == 0) {
            initiate_merge_files_command(connFd, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::START_STAT_COLLECTION) == 0) {
            start_stat_collection_command(connFd, &collectValid, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::REQUEST_COLLECTED_STATS) == 0) {
            request_collected_stats_command(connFd, &collectValid, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::INITIATE_TRAIN) == 0) {
            initiate_train_command(connFd, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::INITIATE_PREDICT) == 0) {
            initiate_predict_command(connFd, &sessionargs, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::INITIATE_MODEL_COLLECTION) == 0) {
            initiate_model_collection_command(connFd, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::INITIATE_FRAGMENT_RESOLUTION) == 0) {
            initiate_fragment_resolution_command(connFd, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::CHECK_FILE_ACCESSIBLE) == 0) {
            check_file_accessible_command(connFd, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::GRAPH_STREAM_START) == 0) {
            graph_stream_start_command(connFd, streamHandler, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::SEND_PRIORITY) == 0) {
            send_priority_command(connFd, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::PUSH_PARTITION) == 0) {
            push_partition_command(connFd, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::HDFS_LOCAL_STREAM_START) == 0) {
            hdfs_start_stream_command(connFd, &loop_exit, true, streamHandler);
        } else if (line.compare(JasmineGraphInstanceProtocol::HDFS_CENTRAL_STREAM_START) == 0) {
            hdfs_start_stream_command(connFd, &loop_exit, false, streamHandler);
        } else if (line.compare(JasmineGraphInstanceProtocol::QUERY_START) == 0) {
            query_start_command(connFd, instanceHandler, incrementalLocalStoreMap, &loop_exit);
        } else if (line.compare(JasmineGraphInstanceProtocol::SUB_QUERY_START) == 0) {
            sub_query_start_command(connFd, instanceHandler, incrementalLocalStoreMap, &loop_exit);
        } else {
            instance_logger.error("Invalid command");
            loop_exit = true;
        }
    }
    instance_logger.info("Closing thread " + to_string(pthread_self()));
    close(connFd);
    return NULL;
}

JasmineGraphInstanceService::JasmineGraphInstanceService() {}

void JasmineGraphInstanceService::run(string masterHost, string host, int serverPort, int serverDataPort) {
    int listenFd;
    socklen_t len;
    struct sockaddr_in svrAdd;
    struct sockaddr_in clntAdd;

    // create socket
    listenFd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenFd < 0) {
        instance_logger.error("Cannot create socket");
        return;
    }

    bzero((char *)&svrAdd, sizeof(svrAdd));

    svrAdd.sin_family = AF_INET;
    svrAdd.sin_addr.s_addr = INADDR_ANY;
    svrAdd.sin_port = htons(serverPort);

    int yes = 1;
    if (setsockopt(listenFd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes) == -1) {
        perror("setsockopt");
    }

    // bind socket
    if (bind(listenFd, (struct sockaddr *)&svrAdd, sizeof(svrAdd)) < 0) {
        instance_logger.error("Cannot bind on port " + std::to_string(serverPort));
        return;
    }

    listen(listenFd, PENDING_CONNECTION_QUEUE_SIZE);

    len = sizeof(clntAdd);

    pthread_mutex_init(&file_lock, NULL);
    std::map<std::string, JasmineGraphHashMapLocalStore> graphDBMapLocalStores;
    std::map<std::string, JasmineGraphHashMapCentralStore> graphDBMapCentralStores;
    std::map<std::string, JasmineGraphHashMapDuplicateCentralStore> graphDBMapDuplicateCentralStores;
    std::map<std::string, JasmineGraphIncrementalLocalStore *> incrementalLocalStore;
    // FaissIndex *faissStore =    FaissIndex::getInstance(std::stoi(Utils::getJasmineGraphProperty("org.jasminegraph.vectorstore.dimension")),"faiss.index");
    // std::thread faissThread([faissStore]() {
    //     std::vector<std::vector<float>> sampleVectors;
    //     for (int i = 0; i < 10; i++) {
    //         sampleVectors.push_back(std::vector<float>(768, i * 0.01f));
    //     }
    //     faissStore->addBatch(sampleVectors); // trains automatically and adds vectors
    // });
    // faissThread.detach();

    std::thread perfThread = std::thread(&PerformanceUtil::collectPerformanceStatistics);
    perfThread.detach();

    instance_logger.info("Worker listening on port " + to_string(serverPort));
    while (true) {
        int connFd = accept(listenFd, (struct sockaddr *)&clntAdd, &len);

        if (connFd < 0) {
            instance_logger.error("Cannot accept connection to port " + to_string(serverPort));
            continue;
        }
        instance_logger.info("Connection successful to port " + to_string(serverPort));

        pid_t pid = fork();
        if (pid == 0) {
            close(listenFd);
            instanceservicesessionargs *serviceArguments_p = new instanceservicesessionargs;
            serviceArguments_p->graphDBMapLocalStores = &graphDBMapLocalStores;
            serviceArguments_p->graphDBMapCentralStores = &graphDBMapCentralStores;
            serviceArguments_p->graphDBMapDuplicateCentralStores = &graphDBMapDuplicateCentralStores;
            serviceArguments_p->incrementalLocalStore = &incrementalLocalStore;
            serviceArguments_p->masterHost = masterHost;
            serviceArguments_p->port = serverPort;
            serviceArguments_p->dataPort = serverDataPort;
            serviceArguments_p->host = host;
            serviceArguments_p->connFd = connFd;
            instanceservicesession(serviceArguments_p);
            break;
        } else {
            close(connFd);
        }
    }

    pthread_mutex_destroy(&file_lock);
    exit(0);  // FIXME: Cleanup before exit.
}

int deleteGraphPartition(std::string graphID, std::string partitionID) {
    int status = 0;
    string partitionFilePath = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" +
                               graphID + "_" + partitionID;
    status |= Utils::deleteDirectory(partitionFilePath);
    string centalStoreFilePath = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" +
                                 graphID + "_centralstore_" + partitionID;
    status |= Utils::deleteDirectory(centalStoreFilePath);
    string centalStoreDuplicateFilePath =
        Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + graphID +
        "_centralstore_dp_" + partitionID;
    status |= Utils::deleteDirectory(centalStoreDuplicateFilePath);
    string attributeFilePath = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" +
                               graphID + "_attributes_" + partitionID;
    status |= Utils::deleteDirectory(attributeFilePath);
    string attributeCentalStoreFilePath =
        Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + graphID +
        "_centralstore_attributes_" + partitionID;
    status |= Utils::deleteDirectory(attributeCentalStoreFilePath);
    if (status == 0) {
        instance_logger.info("Graph partition and centralstore files are now deleted");
    } else {
        instance_logger.warn("Graph partition and centralstore files deleting failed");
    }
    return status;
}

int deleteStreamingGraphPartition(std::string graphID, std::string partitionID) {
    int status = 0;
    string partitionFilePathPattern = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder")
                                      + "/g" + graphID + "_p" + partitionID;
    status |= Utils::deleteAllMatchingFiles(partitionFilePathPattern);
    return status;
}

/** Method for deleting all graph fragments given a graph ID
 *
 * @param graphID ID of graph fragments to be deleted in the instance
 */
void removeGraphFragments(std::string graphID) {
    // Delete all files in the datafolder starting with the graphID
    string partitionFilePath =
        Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + graphID + "_*";
    Utils::deleteDirectory(partitionFilePath);
}

void writeCatalogRecord(string record) {
    string catalogFilePath =
        Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/catalog.txt";
    ofstream outfile;
    outfile.open(catalogFilePath.c_str(), std::ios_base::app);
    outfile << record << endl;
    outfile.close();
}

long countLocalTriangles(
    std::string graphId, std::string partitionId,
    std::map<std::string, JasmineGraphHashMapLocalStore> &graphDBMapLocalStores,
    std::map<std::string, JasmineGraphHashMapCentralStore> &graphDBMapCentralStores,
    std::map<std::string, JasmineGraphHashMapDuplicateCentralStore> &graphDBMapDuplicateCentralStores,
    int threadPriority) {
    long result;

    instance_logger.info("###INSTANCE### Local Triangle Count : Started");
    std::string graphIdentifier = graphId + "_" + partitionId;
    std::string centralGraphIdentifier = graphId + "_centralstore_" + partitionId;
    std::string duplicateCentralGraphIdentifier = graphId + "_centralstore_dp_" + partitionId;

    auto localMapIterator = graphDBMapLocalStores.find(graphIdentifier);
    auto centralStoreIterator = graphDBMapCentralStores.find(graphIdentifier);
    auto duplicateCentralStoreIterator = graphDBMapDuplicateCentralStores.find(graphIdentifier);

    if (localMapIterator == graphDBMapLocalStores.end() &&
        JasmineGraphInstanceService::isGraphDBExists(graphId, partitionId)) {
        JasmineGraphInstanceService::loadLocalStore(graphId, partitionId, graphDBMapLocalStores);
    }
    JasmineGraphHashMapLocalStore graphDB = graphDBMapLocalStores[graphIdentifier];

    if (centralStoreIterator == graphDBMapCentralStores.end() &&
        JasmineGraphInstanceService::isInstanceCentralStoreExists(graphId, partitionId)) {
        JasmineGraphInstanceService::loadInstanceCentralStore(graphId, partitionId, graphDBMapCentralStores);
    }
    JasmineGraphHashMapCentralStore centralGraphDB = graphDBMapCentralStores[centralGraphIdentifier];

    if (duplicateCentralStoreIterator == graphDBMapDuplicateCentralStores.end() &&
        JasmineGraphInstanceService::isInstanceDuplicateCentralStoreExists(graphId, partitionId)) {
        JasmineGraphInstanceService::loadInstanceDuplicateCentralStore(graphId, partitionId,
                                                                       graphDBMapDuplicateCentralStores);
    }
    JasmineGraphHashMapDuplicateCentralStore duplicateCentralGraphDB =
        graphDBMapDuplicateCentralStores[duplicateCentralGraphIdentifier];

    result = Triangles::run(graphDB, centralGraphDB, duplicateCentralGraphDB, graphId, partitionId, threadPriority);

    instance_logger.info("###INSTANCE### Local Triangle Count : Completed: Triangles: " + to_string(result));

    return result;
}

bool JasmineGraphInstanceService::isGraphDBExists(std::string graphId, std::string partitionId) {
    std::string dataFolder = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    std::string fileName = dataFolder + "/" + graphId + "_" + partitionId;
    std::ifstream dbFile(fileName, std::ios::binary);
    if (!dbFile) {
        return false;
    }
    return true;
}

bool JasmineGraphInstanceService::isInstanceCentralStoreExists(std::string graphId, std::string partitionId) {
    std::string dataFolder = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    std::string filename = dataFolder + "/" + graphId + "_centralstore_" + partitionId;
    std::ifstream dbFile(filename, std::ios::binary);
    if (!dbFile) {
        return false;
    }
    return true;
}

bool JasmineGraphInstanceService::isInstanceDuplicateCentralStoreExists(std::string graphId, std::string partitionId) {
    std::string dataFolder = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    std::string filename = dataFolder + "/" + graphId + "_centralstore_dp_" + partitionId;
    std::ifstream dbFile(filename, std::ios::binary);
    if (!dbFile) {
        return false;
    }
    return true;
}

JasmineGraphIncrementalLocalStore *JasmineGraphInstanceService::loadStreamingStore(
    std::string graphId, std::string partitionId,
    std::map<std::string, JasmineGraphIncrementalLocalStore *> &graphDBMapStreamingStores, std::string openMode, bool isEmbed) {
    std::string graphIdentifier = graphId + "_" + partitionId;
    instance_logger.info("###INSTANCE### Loading streaming Store for" + graphIdentifier + " : Started");
    std::string folderLocation = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    JasmineGraphIncrementalLocalStore *jasmineGraphStreamingLocalStore =
        new JasmineGraphIncrementalLocalStore(stoi(graphId), stoi(partitionId), openMode , true);
    graphDBMapStreamingStores[graphIdentifier] = jasmineGraphStreamingLocalStore;
    instance_logger.info("###INSTANCE### Loading Streaming Store : Completed");
    return jasmineGraphStreamingLocalStore;
}

void JasmineGraphInstanceService::loadLocalStore(
    std::string graphId, std::string partitionId,
    std::map<std::string, JasmineGraphHashMapLocalStore> &graphDBMapLocalStores) {
    instance_logger.info("###INSTANCE### Loading Local Store : Started");
    std::string graphIdentifier = graphId + "_" + partitionId;
    std::string folderLocation = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    JasmineGraphHashMapLocalStore jasmineGraphHashMapLocalStore(stoi(graphId), stoi(partitionId), folderLocation);
    jasmineGraphHashMapLocalStore.loadGraph();
    graphDBMapLocalStores[graphIdentifier] = jasmineGraphHashMapLocalStore;
    instance_logger.info("###INSTANCE### Loading Local Store : Completed");
}

void JasmineGraphInstanceService::loadInstanceCentralStore(
    std::string graphId, std::string partitionId,
    std::map<std::string, JasmineGraphHashMapCentralStore> &graphDBMapCentralStores) {
    instance_logger.info("###INSTANCE### Loading central Store : Started");
    std::string graphIdentifier = graphId + "_centralstore_" + partitionId;
    JasmineGraphHashMapCentralStore jasmineGraphHashMapCentralStore(stoi(graphId), stoi(partitionId));
    jasmineGraphHashMapCentralStore.loadGraph();
    graphDBMapCentralStores[graphIdentifier] = jasmineGraphHashMapCentralStore;
    instance_logger.info("###INSTANCE### Loading central Store : Completed");
}

void JasmineGraphInstanceService::loadInstanceDuplicateCentralStore(
    std::string graphId, std::string partitionId,
    std::map<std::string, JasmineGraphHashMapDuplicateCentralStore> &graphDBMapDuplicateCentralStores) {
    std::string graphIdentifier = graphId + "_centralstore_dp_" + partitionId;
    JasmineGraphHashMapDuplicateCentralStore jasmineGraphHashMapCentralStore(stoi(graphId), stoi(partitionId));
    jasmineGraphHashMapCentralStore.loadGraph();
    graphDBMapDuplicateCentralStores[graphIdentifier] = jasmineGraphHashMapCentralStore;
}

JasmineGraphHashMapCentralStore *JasmineGraphInstanceService::loadCentralStore(std::string centralStoreFileName) {
    instance_logger.info("###INSTANCE### Loading Central Store File : Started " + centralStoreFileName);
    JasmineGraphHashMapCentralStore *jasmineGraphHashMapCentralStore = new JasmineGraphHashMapCentralStore();
    jasmineGraphHashMapCentralStore->loadGraph(centralStoreFileName);
    instance_logger.info("###INSTANCE### Loading Central Store File : Completed");
    return jasmineGraphHashMapCentralStore;
}

static string aggregateCentralStoreTriangles(std::string graphId, std::string partitionId, std::string partitionIdList,
                                             int threadPriority) {
    instance_logger.info("###INSTANCE### Started Aggregating Central Store Triangles");
    std::string aggregatorDirPath = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.aggregatefolder");
    std::vector<std::string> fileNames;
    map<long, unordered_set<long>> aggregatedCentralStore;
    std::string centralGraphIdentifier = graphId + "_centralstore_" + partitionId;
    std::string dataFolder = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    std::string workerCentralStoreFile = dataFolder + "/" + centralGraphIdentifier;
    instance_logger.info("###INSTANCE### Loading Central Store : Started " + workerCentralStoreFile);
    JasmineGraphHashMapCentralStore *workerCentralStore =
        JasmineGraphInstanceService::loadCentralStore(workerCentralStoreFile);
    instance_logger.info("###INSTANCE### Loading Central Store : Completed");
    const auto &workerCentralGraphMap = workerCentralStore->getUnderlyingHashMap();

    for (auto workerCentalGraphIterator = workerCentralGraphMap.begin();
         workerCentalGraphIterator != workerCentralGraphMap.end(); ++workerCentalGraphIterator) {
        long startVid = workerCentalGraphIterator->first;
        const unordered_set<long> &endVidSet = workerCentalGraphIterator->second;

        unordered_set<long> &aggregatedEndVidSet = aggregatedCentralStore[startVid];
        aggregatedEndVidSet.insert(endVidSet.begin(), endVidSet.end());
    }
    delete workerCentralStore;

    std::vector<std::string> paritionIdList = Utils::split(partitionIdList, ',');
    std::vector<std::string>::iterator partitionIdListIterator;

    for (partitionIdListIterator = paritionIdList.begin(); partitionIdListIterator != paritionIdList.end();
         ++partitionIdListIterator) {
        std::string aggregatePartitionId = *partitionIdListIterator;

        std::string centralGraphIdentifier = graphId + "_centralstore_" + aggregatePartitionId;
        std::string centralStoreFile = aggregatorDirPath + "/" + centralGraphIdentifier;
        if (access(centralStoreFile.c_str(), R_OK) == 0) {
            JasmineGraphHashMapCentralStore *centralStore =
                JasmineGraphInstanceService::loadCentralStore(centralStoreFile);
            const auto &centralGraphMap = centralStore->getUnderlyingHashMap();

            for (auto centralGraphMapIterator = centralGraphMap.begin();
                 centralGraphMapIterator != centralGraphMap.end(); ++centralGraphMapIterator) {
                long startVid = centralGraphMapIterator->first;
                const unordered_set<long> &endVidSet = centralGraphMapIterator->second;

                unordered_set<long> &aggregatedEndVidSet = aggregatedCentralStore[startVid];
                aggregatedEndVidSet.insert(endVidSet.begin(), endVidSet.end());
            }
            delete centralStore;
        }
    }

    instance_logger.info("###INSTANCE### Central Store Aggregation : Completed");

    map<long, long> distributionHashMap =
        JasmineGraphInstanceService::getOutDegreeDistributionHashMap(aggregatedCentralStore);

    const TriangleResult &triangleResult = Triangles::countTriangles(aggregatedCentralStore, distributionHashMap, true);
    return triangleResult.triangles;
}

string JasmineGraphInstanceService::aggregateCompositeCentralStoreTriangles(std::string compositeFileList,
                                                                            std::string availableFileList,
                                                                            int threadPriority) {
    instance_logger.info("###INSTANCE### Started Aggregating Composite Central Store Triangles");
    std::string aggregatorDirPath = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.aggregatefolder");
    std::string dataFolder = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    map<long, unordered_set<long>> aggregatedCompositeCentralStore;

    std::vector<std::string> compositeCentralStoreFileList = Utils::split(compositeFileList, ':');
    std::vector<std::string>::iterator compositeCentralStoreFileIterator;
    std::vector<std::string> availableCompositeFileList = Utils::split(availableFileList, ':');
    std::vector<std::string>::iterator availableCompositeFileIterator;

    for (availableCompositeFileIterator = availableCompositeFileList.begin();
         availableCompositeFileIterator != availableCompositeFileList.end(); ++availableCompositeFileIterator) {
        std::string availableCompositeFileName = *availableCompositeFileIterator;
        size_t lastindex = availableCompositeFileName.find_last_of(".");
        string rawFileName = availableCompositeFileName.substr(0, lastindex);

        std::string availableCompositeFile = dataFolder + "/" + rawFileName;
        if (access(availableCompositeFile.c_str(), R_OK) == 0) {
            JasmineGraphHashMapCentralStore *centralStore =
                JasmineGraphInstanceService::loadCentralStore(availableCompositeFile);
            const auto &compositeCentralGraphMap = centralStore->getUnderlyingHashMap();

            for (auto compositeCentralGraphMapIterator = compositeCentralGraphMap.begin();
                 compositeCentralGraphMapIterator != compositeCentralGraphMap.end();
                 ++compositeCentralGraphMapIterator) {
                long startVid = compositeCentralGraphMapIterator->first;
                const unordered_set<long> &endVidSet = compositeCentralGraphMapIterator->second;

                unordered_set<long> &aggregatedEndVidSet = aggregatedCompositeCentralStore[startVid];
                aggregatedEndVidSet.insert(endVidSet.begin(), endVidSet.end());
            }
            delete centralStore;
        }
    }

    for (compositeCentralStoreFileIterator = compositeCentralStoreFileList.begin();
         compositeCentralStoreFileIterator != compositeCentralStoreFileList.end();
         ++compositeCentralStoreFileIterator) {
        std::string compositeCentralStoreFileName = *compositeCentralStoreFileIterator;
        size_t lastindex = compositeCentralStoreFileName.find_last_of(".");
        string rawFileName = compositeCentralStoreFileName.substr(0, lastindex);

        std::string compositeCentralStoreFile = aggregatorDirPath + "/" + rawFileName;
        if (access(compositeCentralStoreFile.c_str(), R_OK) == 0) {
            JasmineGraphHashMapCentralStore *centralStore =
                JasmineGraphInstanceService::loadCentralStore(compositeCentralStoreFile);
            const auto &centralGraphMap = centralStore->getUnderlyingHashMap();
            for (auto centralGraphMapIterator = centralGraphMap.begin();
                 centralGraphMapIterator != centralGraphMap.end(); ++centralGraphMapIterator) {
                long startVid = centralGraphMapIterator->first;
                const unordered_set<long> &endVidSet = centralGraphMapIterator->second;

                unordered_set<long> &aggregatedEndVidSet = aggregatedCompositeCentralStore[startVid];
                aggregatedEndVidSet.insert(endVidSet.begin(), endVidSet.end());
            }
            delete centralStore;
        }
    }

    instance_logger.info("###INSTANCE### Central Store Aggregation : Completed");

    map<long, long> distributionHashMap =
        JasmineGraphInstanceService::getOutDegreeDistributionHashMap(aggregatedCompositeCentralStore);

    const TriangleResult &triangleResult =
        Triangles::countTriangles(aggregatedCompositeCentralStore, distributionHashMap, true);
    return triangleResult.triangles;
}

map<long, long> JasmineGraphInstanceService::getOutDegreeDistributionHashMap(map<long, unordered_set<long>> &graphMap) {
    map<long, long> distributionHashMap;

    for (map<long, unordered_set<long>>::iterator it = graphMap.begin(); it != graphMap.end(); ++it) {
        long distribution = (it->second).size();
        distributionHashMap[it->first] = distribution;
    }
    return distributionHashMap;
}

void JasmineGraphInstanceService::collectTrainedModels(
    instanceservicesessionargs *sessionargs, std::string graphID,
    std::map<std::string, JasmineGraphInstanceService::workerPartitions> &graphPartitionedHosts, int totalPartitions) {
    int total_threads = totalPartitions;
    std::thread *workerThreads = new std::thread[total_threads];
    int count = 0;
    std::map<std::string, JasmineGraphInstanceService::workerPartitions>::iterator mapIterator;
    for (mapIterator = graphPartitionedHosts.begin(); mapIterator != graphPartitionedHosts.end(); mapIterator++) {
        string hostName = mapIterator->first;
        JasmineGraphInstanceService::workerPartitions workerPartitions = mapIterator->second;
        std::vector<std::string>::iterator it;
        for (it = workerPartitions.partitionID.begin(); it != workerPartitions.partitionID.end(); it++) {
            workerThreads[count] =
                std::thread(&JasmineGraphInstanceService::collectTrainedModelThreadFunction, sessionargs, hostName,
                            workerPartitions.port, workerPartitions.dataPort, graphID, *it);
            count++;
        }
    }

    for (int threadCount = 0; threadCount < count; threadCount++) {
        workerThreads[threadCount].join();
    }
}

int JasmineGraphInstanceService::collectTrainedModelThreadFunction(instanceservicesessionargs *sessionargs,
                                                                   std::string host, int port, int dataPort,
                                                                   std::string graphID, std::string partition) {
    bool result = true;
    int sockfd;
    char data[DATA_BUFFER_SIZE];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        instance_logger.error("Cannot create socket");
        return 0;
    }

    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        instance_logger.error("ERROR, no host named " + host);
        return 0;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        return 0;
    }
    if (Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE)) {
        instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE);
    }

    string response = Utils::read_str_trim_wrapper(sockfd, data, INSTANCE_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        instance_logger.info("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK);

        string server_host = sessionargs->host;
        Utils::send_str_wrapper(sockfd, server_host);
        instance_logger.info("Sent : " + server_host);

        if (Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::INITIATE_MODEL_COLLECTION)) {
            instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::INITIATE_MODEL_COLLECTION);
        }

        response = Utils::read_str_trim_wrapper(sockfd, data, INSTANCE_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            instance_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);

            string server_host = sessionargs->host;
            Utils::send_str_wrapper(sockfd, server_host);
            instance_logger.info("Sent : " + server_host);

            int server_port = sessionargs->port;
            Utils::send_str_wrapper(sockfd, to_string(server_port));
            instance_logger.info("Sent : " + server_port);

            int server_data_port = sessionargs->dataPort;
            Utils::send_str_wrapper(sockfd, to_string(server_data_port));
            instance_logger.info("Sent : " + server_data_port);

            Utils::send_str_wrapper(sockfd, graphID);
            instance_logger.info("Sent : Graph ID " + graphID);

            Utils::send_str_wrapper(sockfd, partition);
            instance_logger.info("Sent : Partition ID " + partition);

            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::SEND_FILE_NAME);
            instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME);

            string fileName = Utils::read_str_wrapper(sockfd, data, INSTANCE_DATA_LENGTH, false);
            instance_logger.info("Received File name: " + fileName);
            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::SEND_FILE_LEN);
            instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN);

            string size = Utils::read_str_trim_wrapper(sockfd, data, INSTANCE_DATA_LENGTH);
            instance_logger.info("Received file size in bytes: " + size);

            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::SEND_FILE_CONT);
            instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_CONT);
            string fullFilePath =
                Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + fileName;
            int fileSize = stoi(size);
            while (Utils::fileExists(fullFilePath) && Utils::getFileSize(fullFilePath) < fileSize) {
                response = Utils::read_str_wrapper(sockfd, data, INSTANCE_DATA_LENGTH, false);

                if (response.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) == 0) {
                    Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::FILE_RECV_WAIT);
                }
            }

            response = Utils::read_str_wrapper(sockfd, data, INSTANCE_DATA_LENGTH, false);
            if (response.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) == 0) {
                instance_logger.info("Received : " + JasmineGraphInstanceProtocol::FILE_RECV_CHK);
                Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::FILE_ACK);
                instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::FILE_ACK);
            }

            Utils::unzipDirectory(fullFilePath);
            size_t lastindex = fileName.find_last_of(".");
            string pre_rawname = fileName.substr(0, lastindex);
            size_t next_lastindex = pre_rawname.find_last_of(".");
            string rawname = fileName.substr(0, next_lastindex);
            fullFilePath =
                Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.trainedmodelfolder") + "/" + rawname;

            while (!Utils::fileExists(fullFilePath)) {
                string response = Utils::read_str_trim_wrapper(sockfd, data, INSTANCE_DATA_LENGTH);
                if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK) == 0) {
                    instance_logger.info("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK);
                    Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT);
                    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT);
                }
            }
            response = Utils::read_str_wrapper(sockfd, data, INSTANCE_DATA_LENGTH, false);
            if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK) == 0) {
                instance_logger.info("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK);
                Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK);
                instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK);
            }
        }
    } else {
        instance_logger.error("There was an error in the model collection process and the response is :: " + response);
    }
    Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
    close(sockfd);
    return 0;
}

void JasmineGraphInstanceService::createPartitionFiles(std::string graphID, std::string partitionID,
                                                       std::string fileType) {
    string inputFilePath = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" +
                           graphID + "_" + partitionID;
    string outputFilePath = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.trainedmodelfolder") +
                            "/" + graphID + "_" + partitionID;
    if (fileType == "centralstore") {
        inputFilePath = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + graphID +
                        "_centralstore_" + partitionID;
        outputFilePath = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.trainedmodelfolder") + "/" +
                         graphID + "_centralstore_" + partitionID;
    }
    JasmineGraphHashMapLocalStore hashMapLocalStore;
    std::map<int, std::vector<int>> partEdgeMap = hashMapLocalStore.getEdgeHashMap(inputFilePath);

    if (partEdgeMap.empty()) {
        return;
    }
    std::ofstream localFile(outputFilePath);

    if (!localFile.is_open()) {
        return;
    }
    for (auto it = partEdgeMap.begin(); it != partEdgeMap.end(); ++it) {
        int vertex = it->first;
        std::vector<int> destinationSet = it->second;

        if (!destinationSet.empty()) {
            for (std::vector<int>::iterator itr = destinationSet.begin(); itr != destinationSet.end(); ++itr) {
                string edge;

                edge = std::to_string(vertex) + " " + std::to_string((*itr));
                localFile << edge;
                localFile << "\n";
            }
        }
    }
    localFile.flush();
    localFile.close();
}

void JasmineGraphInstanceService::collectExecutionData(int iteration, string trainArgs, string partCount) {
    pthread_mutex_lock(&map_lock);
    vector<string> trainData;
    if (iterationData.find(iteration) != iterationData.end()) {
        vector<string> trainData = iterationData[iteration];
    }
    trainData.push_back(trainArgs);
    iterationData[iteration] = trainData;
    partitionCounter++;
    pthread_mutex_unlock(&map_lock);
    if (partitionCounter == stoi(partCount)) {
        int maxPartCountInVector = 0;
        instance_logger.info("Data collection done for all iterations");
        for (auto bin = iterationData.begin(); bin != iterationData.end(); ++bin) {
            if (maxPartCountInVector < bin->second.size()) {
                maxPartCountInVector = bin->second.size();
            }
        }
        JasmineGraphInstanceService::executeTrainingIterations(maxPartCountInVector);
    }
    return;
}

void JasmineGraphInstanceService::executeTrainingIterations(int maxThreads) {
    int iterCounter = 0;
    std::thread *threadList = new std::thread[maxThreads];
    for (auto in = iterationData.begin(); in != iterationData.end(); ++in) {
        vector<string> partVector = in->second;
        int count = 0;

        for (auto trainarg = partVector.begin(); trainarg != partVector.end(); ++trainarg) {
            string trainData = *trainarg;
            threadList[count] = std::thread(trainPartition, trainData);
            count++;
        }
        iterCounter++;
        instance_logger.info("Trainings initiated for iteration " + to_string(iterCounter));
        for (int threadCount = 0; threadCount < count; threadCount++) {
            threadList[threadCount].join();
        }
        instance_logger.info("Trainings completed for iteration " + to_string(iterCounter));
    }
    iterationData.clear();
    partitionCounter = 0;
}

void JasmineGraphInstanceService::trainPartition(string trainData) {
    std::vector<std::string> trainargs = Utils::split(trainData, ' ');
    string graphID;
    string partitionID = trainargs[trainargs.size() - 1];

    for (int i = 0; i < trainargs.size(); i++) {
        if (trainargs[i] == "--graph_id") {
            graphID = trainargs[i + 1];
            break;
        }
    }

    std::vector<char *> vc;
    std::transform(trainargs.begin(), trainargs.end(), std::back_inserter(vc), converter);

    std::string path = "cd " + Utils::getJasmineGraphProperty("org.jasminegraph.graphsage") + " && ";
    std::string command = path + "python3 -m unsupervised_train >  /var/tmp/jasminegraph/logs/unsupervised_train" +
                          partitionID + "-" + Utils::getCurrentTimestamp() + ".txt";

    int argc = trainargs.size();
    for (int i = 0; i < argc - 2; ++i) {
        command += trainargs[i + 2];
        command += " ";
    }
    instance_logger.error("Temporarily disabled the execution of unsupervised train.");
    // TODO(thevindu-w): Temporarily commenting the execution of the following line
    // due to missing unsupervised_train.py file. Removal of graphsage folder resulted in this situation.
    // Need to find a different way of executing Unsupervised train
    // system(command.c_str());
}

bool JasmineGraphInstanceService::duplicateCentralStore(int thisWorkerPort, int graphID, int partitionID,
                                                        std::vector<string> &workerSockets, std::string masterIP) {
    std::string aggregatorDirPath = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.aggregatefolder");
    std::string dataDirPath = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");

    std::string centralGraphIdentifierUnCompressed = to_string(graphID) + "_centralstore_" + to_string(partitionID);
    std::string centralStoreFileUnCompressed = dataDirPath + "/" + centralGraphIdentifierUnCompressed;
    std::string centralStoreFileUnCompressedDestination = aggregatorDirPath + "/" + centralGraphIdentifierUnCompressed;

    // temporary copy the central store into the aggregate folder in order to compress and send
    Utils::copyToDirectory(centralStoreFileUnCompressed, aggregatorDirPath);

    // compress the central store file before sending
    Utils::compressFile(centralStoreFileUnCompressedDestination);

    std::string centralStoreFile = centralStoreFileUnCompressedDestination + ".gz";
    instance_logger.info("###INSTANCE### centralstore " + centralStoreFile);
    char data[DATA_BUFFER_SIZE];

    for (vector<string>::iterator workerIt = workerSockets.begin(); workerIt != workerSockets.end(); ++workerIt) {
        std::vector<string> workerSocketPair;
        stringstream wl(*workerIt);
        string intermediate;
        while (getline(wl, intermediate, ':')) {
            workerSocketPair.push_back(intermediate);
        }

        if (workerSocketPair.size() != 4) {
            instance_logger.error("Received worker socket information is invalid ");
            return false;
        }

        struct stat fileStat;
        if (stat(centralStoreFile.c_str(), &fileStat) != 0) {
            instance_logger.error("stat() failed on " + centralStoreFile);
            return false;
        }
        if (!S_ISREG(fileStat.st_mode)) {
            instance_logger.error(centralStoreFile + " is not a regular file.");
            return 0;
        }
        string host = workerSocketPair[0];
        int port = stoi(workerSocketPair[1]);
        int workerGraphID = stoi(workerSocketPair[2]);
        int dataPort = stoi(workerSocketPair[3]);

        if (port == thisWorkerPort) {
            continue;
        }

        bool result = true;
        int sockfd;
        bool loop = false;
        socklen_t len;
        struct sockaddr_in serv_addr;
        struct hostent *server;

        sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd < 0) {
            instance_logger.error("Cannot create socket");
            return false;
        }

        if (host.find('@') != std::string::npos) {
            host = Utils::split(host, '@')[1];
        }

        server = gethostbyname(host.c_str());
        if (server == NULL) {
            instance_logger.error("ERROR, no host named " + host);
            return false;
        }

        bzero((char *)&serv_addr, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
        serv_addr.sin_port = htons(port);
        if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
            return false;
        }

        if (Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE)) {
            instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE);
        }

        string response = Utils::read_str_trim_wrapper(sockfd, data, INSTANCE_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) != 0) {
            instance_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::HANDSHAKE_OK +
                                  " ; Received: " + response);
            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
            close(sockfd);
            continue;
        }
        instance_logger.info("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK);

        if (Utils::send_str_wrapper(sockfd, masterIP)) {
            instance_logger.info("Sent : " + masterIP);
        }

        response = Utils::read_str_wrapper(sockfd, data, INSTANCE_DATA_LENGTH, false);
        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) != 0) {
            instance_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::HOST_OK +
                                  " ; Received: " + response);
            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
            close(sockfd);
            continue;
        }
        instance_logger.info("Received : " + JasmineGraphInstanceProtocol::HOST_OK);

        if (Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_CENTRAL)) {
            instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CENTRAL);
        }

        response = Utils::read_str_trim_wrapper(sockfd, data, INSTANCE_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
            instance_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::OK +
                                  " ; Received: " + response);
            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
            close(sockfd);
            continue;
        }
        instance_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);

        if (Utils::send_str_wrapper(sockfd, std::to_string(graphID))) {
            instance_logger.info("Sent : Graph ID " + std::to_string(graphID));
        }

        std::string fileName = Utils::getFileName(centralStoreFile);
        int fileSize = Utils::getFileSize(centralStoreFile);
        std::string fileLength = to_string(fileSize);

        response = Utils::read_str_trim_wrapper(sockfd, data, INSTANCE_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_NAME) != 0) {
            instance_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::SEND_FILE_NAME +
                                  " ; Received: " + response);
            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
            close(sockfd);
            continue;
        }
        instance_logger.info("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME);

        if (Utils::send_str_wrapper(sockfd, fileName)) {
            instance_logger.info("Sent : File name " + fileName);
        }

        response = Utils::read_str_wrapper(sockfd, data, INSTANCE_DATA_LENGTH, false);
        if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_LEN) != 0) {
            instance_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::SEND_FILE_LEN +
                                  " ; Received: " + response);
            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
            close(sockfd);
            continue;
        }
        instance_logger.info("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN);
        if (Utils::send_str_wrapper(sockfd, fileLength)) {
            instance_logger.info("Sent : File length in bytes " + fileLength);
        }

        response = Utils::read_str_wrapper(sockfd, data, INSTANCE_DATA_LENGTH, false);
        if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_CONT) != 0) {
            instance_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::SEND_FILE_CONT +
                                  " ; Received: " + response);
            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
            close(sockfd);
            continue;
        }
        instance_logger.info("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_CONT);

        instance_logger.info("Going to send file through service");
        Utils::sendFileThroughService(host, dataPort, fileName, centralStoreFile);

        int count = 0;
        while (true) {
            if (Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::FILE_RECV_CHK)) {
                instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::FILE_RECV_CHK);
            }

            instance_logger.info("Checking if file is received");
            response = Utils::read_str_wrapper(sockfd, data, INSTANCE_DATA_LENGTH, false);
            if (response.compare(JasmineGraphInstanceProtocol::FILE_RECV_WAIT) == 0) {
                instance_logger.info("Received : " + JasmineGraphInstanceProtocol::FILE_RECV_WAIT);
                instance_logger.info("Checking file status : " + to_string(count));
                count++;
                sleep(1);
                continue;
            } else if (response.compare(JasmineGraphInstanceProtocol::FILE_ACK) == 0) {
                instance_logger.info("Received : " + JasmineGraphInstanceProtocol::FILE_ACK);
                instance_logger.info("File transfer completed for file : " + centralStoreFile);
                break;
            } else {
                instance_logger.error("Incorrect response. Received: " + response);
                goto END_OUTER_LOOP;
            }
        };
        // Next we wait till the batch upload completes
        while (true) {
            if (Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK)) {
                instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK);
            }

            response = Utils::read_str_wrapper(sockfd, data, INSTANCE_DATA_LENGTH, false);
            if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT) == 0) {
                instance_logger.info("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT);
                sleep(1);
                continue;
            } else if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK) == 0) {
                instance_logger.info("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK);
                instance_logger.info("Batch upload completed");
                break;
            } else {
                instance_logger.error("Incorrect response. Received: " + response);
                goto END_OUTER_LOOP;
            }
        }
    END_OUTER_LOOP:
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
    }
    return true;
}

map<long, long> calculateOutDegreeDist(string graphID, string partitionID, int serverPort,
                                       std::map<std::string, JasmineGraphHashMapLocalStore> &graphDBMapLocalStores,
                                       std::map<std::string, JasmineGraphHashMapCentralStore> &graphDBMapCentralStores,
                                       std::vector<string> &workerSockets) {
    map<long, long> degreeDistribution =
        calculateLocalOutDegreeDist(graphID, partitionID, graphDBMapLocalStores, graphDBMapCentralStores);

    string instanceDataFolderLocation = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    string attributeFilePart = instanceDataFolderLocation + "/" + graphID + "_odd_" + partitionID;
    ofstream partfile;
    partfile.open(attributeFilePart, std::fstream::trunc);
    for (map<long, long>::iterator it = degreeDistribution.begin(); it != degreeDistribution.end(); ++it) {
        partfile << to_string(it->first) << "\t" << to_string(it->second) << endl;
    }
    partfile.close();

    graphDBMapLocalStores.clear();
    graphDBMapCentralStores.clear();
    degreeDistribution.clear();

    return degreeDistribution;
}

map<long, long> calculateLocalOutDegreeDist(
    string graphID, string partitionID, std::map<std::string, JasmineGraphHashMapLocalStore> &graphDBMapLocalStores,
    std::map<std::string, JasmineGraphHashMapCentralStore> &graphDBMapCentralStores) {
    auto t_start = std::chrono::high_resolution_clock::now();

    JasmineGraphHashMapLocalStore graphDB;
    JasmineGraphHashMapCentralStore centralDB;
    std::map<std::string, JasmineGraphHashMapLocalStore>::iterator it;
    std::map<std::string, JasmineGraphHashMapCentralStore>::iterator itcen;

    if (JasmineGraphInstanceService::isGraphDBExists(graphID, partitionID)) {
        JasmineGraphInstanceService::loadLocalStore(graphID, partitionID, graphDBMapLocalStores);
    }

    if (JasmineGraphInstanceService::isInstanceCentralStoreExists(graphID, partitionID)) {
        JasmineGraphInstanceService::loadInstanceCentralStore(graphID, partitionID, graphDBMapCentralStores);
    }

    graphDB = graphDBMapLocalStores[graphID + "_" + partitionID];
    centralDB = graphDBMapCentralStores[graphID + "_centralstore_" + partitionID];

    map<long, long> degreeDistributionLocal = graphDB.getOutDegreeDistributionHashMap();
    std::map<long, long>::iterator itlocal;

    std::map<long, unordered_set<long>>::iterator itcentral;

    map<long, long> degreeDistributionCentralTotal;

    map<long, unordered_set<long>> centralGraphMap = centralDB.getUnderlyingHashMap();
    map<long, unordered_set<long>> localGraphMap = graphDB.getUnderlyingHashMap();

    for (itcentral = centralGraphMap.begin(); itcentral != centralGraphMap.end(); ++itcentral) {
        long distribution = (itcentral->second).size();
        map<long, long>::iterator degreeDistributionLocalItr = degreeDistributionLocal.find(itcentral->first);
        if (degreeDistributionLocalItr != degreeDistributionLocal.end()) {
            long degreeDistributionValue = degreeDistributionLocalItr->second;
            degreeDistributionLocal[degreeDistributionLocalItr->first] = degreeDistributionValue + distribution;
        }
    }

    auto t_end = std::chrono::high_resolution_clock::now();
    double elapsed_time_ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();

    instance_logger.info("Elapsed time out degree distribution (in ms) ----------: " + to_string(elapsed_time_ms));
    return degreeDistributionLocal;
}

map<long, long> calculateLocalInDegreeDist(
    string graphID, string partitionID, std::map<std::string, JasmineGraphHashMapLocalStore> &graphDBMapLocalStores,
    std::map<std::string, JasmineGraphHashMapCentralStore> &graphDBMapCentralStores) {
    JasmineGraphHashMapLocalStore graphDB;

    std::map<std::string, JasmineGraphHashMapLocalStore>::iterator it;

    if (JasmineGraphInstanceService::isGraphDBExists(graphID, partitionID)) {
        JasmineGraphInstanceService::loadLocalStore(graphID, partitionID, graphDBMapLocalStores);
    }

    graphDB = graphDBMapLocalStores[graphID + "_" + partitionID];

    map<long, long> degreeDistribution = graphDB.getInDegreeDistributionHashMap();
    std::map<long, long>::iterator its;

    return degreeDistribution;
}

map<long, long> calculateInDegreeDist(string graphID, string partitionID, int serverPort,
                                      std::map<std::string, JasmineGraphHashMapLocalStore> &graphDBMapLocalStores,
                                      std::map<std::string, JasmineGraphHashMapCentralStore> &graphDBMapCentralStores,
                                      std::vector<string> &workerSockets, string workerList) {
    auto t_start = std::chrono::high_resolution_clock::now();

    map<long, long> degreeDistribution =
        calculateLocalInDegreeDist(graphID, partitionID, graphDBMapLocalStores, graphDBMapCentralStores);

    for (vector<string>::iterator workerIt = workerSockets.begin(); workerIt != workerSockets.end(); ++workerIt) {
        instance_logger.info("Worker pair " + *workerIt);

        std::vector<string> workerSocketPair;
        stringstream wl(*workerIt);
        string intermediate;
        while (getline(wl, intermediate, ':')) {
            workerSocketPair.push_back(intermediate);
        }
        string workerPartitionID = workerSocketPair[2];

        JasmineGraphHashMapCentralStore centralDB;

        std::map<std::string, JasmineGraphHashMapCentralStore>::iterator itcen;

        if (JasmineGraphInstanceService::isInstanceCentralStoreExists(graphID, workerPartitionID)) {
            JasmineGraphInstanceService::loadInstanceCentralStore(graphID, workerPartitionID, graphDBMapCentralStores);
        }
        centralDB = graphDBMapCentralStores[graphID + "_centralstore_" + workerPartitionID];

        map<long, long> degreeDistributionCentral = centralDB.getInDegreeDistributionHashMap();
        std::map<long, long>::iterator itcentral;
        std::map<long, long>::iterator its;

        for (its = degreeDistributionCentral.begin(); its != degreeDistributionCentral.end(); ++its) {
            bool centralNodeFound = false;
            map<long, long>::iterator degreeDistributionLocalItr = degreeDistribution.find(its->first);
            if (degreeDistributionLocalItr != degreeDistribution.end()) {
                long degreeDistributionValue = degreeDistributionLocalItr->second;
                degreeDistribution[degreeDistributionLocalItr->first] = degreeDistributionValue + its->second;
            } else {
                degreeDistribution[its->first] = its->second;
            }
        }

        graphDBMapLocalStores.clear();
        graphDBMapCentralStores.clear();
        degreeDistributionCentral.clear();
        instance_logger.info("Worker partition idd combined " + workerPartitionID);
    }

    auto t_end = std::chrono::high_resolution_clock::now();
    double elapsed_time_ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();

    instance_logger.info("Elapsed time in degree distribution (in ms) ----------: " + to_string(elapsed_time_ms));

    instance_logger.info("In Degree Dist size: " + to_string(degreeDistribution.size()));

    string instanceDataFolderLocation = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    string attributeFilePart = instanceDataFolderLocation + "/" + graphID + "_idd_" + partitionID;
    ofstream partfile;
    partfile.open(attributeFilePart, std::fstream::trunc);
    for (map<long, long>::iterator it = degreeDistribution.begin(); it != degreeDistribution.end(); ++it) {
        partfile << to_string(it->first) << "\t" << to_string(it->second) << endl;
    }
    partfile.close();

    degreeDistribution.clear();
    return degreeDistribution;
}

map<long, map<long, unordered_set<long>>> calculateLocalEgoNet(string graphID, string partitionID, int serverPort,
                                                               JasmineGraphHashMapLocalStore localDB,
                                                               JasmineGraphHashMapCentralStore centralDB,
                                                               std::vector<string> &workerSockets) {
    std::map<long, map<long, unordered_set<long>>> egonetMap;

    map<long, unordered_set<long>> centralGraphMap = centralDB.getUnderlyingHashMap();
    map<long, unordered_set<long>> localGraphMap = localDB.getUnderlyingHashMap();

    for (map<long, unordered_set<long>>::iterator it = localGraphMap.begin(); it != localGraphMap.end(); ++it) {
        unordered_set<long> neighbours = it->second;

        map<long, unordered_set<long>> individualEgoNet;
        individualEgoNet[it->first] = neighbours;

        for (unordered_set<long>::iterator neighbour = neighbours.begin(); neighbour != neighbours.end(); ++neighbour) {
            unordered_set<long> neighboursOfNeighboursInSameEgoNet;

            map<long, unordered_set<long>>::iterator localGraphMapItr = localGraphMap.find(*neighbour);
            if (localGraphMapItr != localGraphMap.end()) {
                unordered_set<long> neighboursOfNeighbour = localGraphMapItr->second;

                for (auto neighboursOfNeighbourItr = neighboursOfNeighbour.begin();
                     neighboursOfNeighbourItr != neighboursOfNeighbour.end(); ++neighboursOfNeighbourItr) {
                    unordered_set<long>::iterator neighboursItr = neighbours.find(*neighboursOfNeighbourItr);
                    if (neighboursItr != neighbours.end()) {
                        neighboursOfNeighboursInSameEgoNet.insert(*neighboursItr);
                    }
                }
            }
            individualEgoNet[*neighbour] = neighboursOfNeighboursInSameEgoNet;
        }

        egonetMap[it->first] = individualEgoNet;
    }

    for (map<long, unordered_set<long>>::iterator it = centralGraphMap.begin(); it != centralGraphMap.end(); ++it) {
        unordered_set<long> distribution = it->second;

        map<long, map<long, unordered_set<long>>>::iterator egonetMapItr = egonetMap.find(it->first);

        if (egonetMapItr == egonetMap.end()) {
            map<long, unordered_set<long>> vertexMapFromCentralStore;
            vertexMapFromCentralStore.insert(
                std::make_pair(it->first,
                               distribution));  // Here we do not have the relation information among neighbours
            egonetMap[it->first] = vertexMapFromCentralStore;

        } else {
            map<long, unordered_set<long>> egonetSubGraph = egonetMapItr->second;

            map<long, unordered_set<long>>::iterator egonetSubGraphItr = egonetSubGraph.find(it->first);
            if (egonetSubGraphItr != egonetSubGraph.end()) {
                unordered_set<long> egonetSubGraphNeighbours = egonetSubGraphItr->second;
                egonetSubGraphNeighbours.insert(distribution.begin(), distribution.end());
                egonetSubGraphItr->second = egonetSubGraphNeighbours;
            }
        }
    }

    for (vector<string>::iterator workerIt = workerSockets.begin(); workerIt != workerSockets.end(); ++workerIt) {
        std::vector<string> workerSocketPair;
        stringstream wl(*workerIt);
        string intermediate;
        while (getline(wl, intermediate, ':')) {
            workerSocketPair.push_back(intermediate);
        }

        std::string dataDirPath = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
        std::string centralGraphIdentifier = graphID + "_centralstore_" + workerSocketPair[2];

        std::string centralStoreFile = dataDirPath + "/" + centralGraphIdentifier;
        instance_logger.info("###INSTANCE### centralstore " + centralStoreFile);

        struct stat centralStoreFileStat;
        if (stat(centralStoreFile.c_str(), &centralStoreFileStat) != 0) {
            instance_logger.error("stat failed for " + centralStoreFile);
            continue;
        }
        if (!S_ISREG(centralStoreFileStat.st_mode)) {
            instance_logger.error(centralStoreFile + " is not a regular file");
            continue;
        }
        JasmineGraphHashMapCentralStore *centralStore = JasmineGraphInstanceService::loadCentralStore(centralStoreFile);
        const auto &centralGraphMap = centralStore->getUnderlyingHashMap();

        for (auto centralGraphMapIterator = centralGraphMap.begin(); centralGraphMapIterator != centralGraphMap.end();
             ++centralGraphMapIterator) {
            long startVid = centralGraphMapIterator->first;
            const unordered_set<long> &endVidSet = centralGraphMapIterator->second;

            for (auto itr = endVidSet.begin(); itr != endVidSet.end(); ++itr) {
                auto egonetMapItr = egonetMap.find(*itr);
                if (egonetMapItr != egonetMap.end()) {
                    map<long, unordered_set<long>> &egonetSubGraph = egonetMapItr->second;
                    auto egonetSubGraphItr = egonetSubGraph.find(*itr);
                    if (egonetSubGraphItr != egonetSubGraph.end()) {
                        unordered_set<long> &egonetSubGraphNeighbours = egonetSubGraphItr->second;
                        egonetSubGraphNeighbours.insert(startVid);
                    }
                }
            }
        }
        delete centralStore;
    }

    return egonetMap;
}

void calculateEgoNet(string graphID, string partitionID, int serverPort, JasmineGraphHashMapLocalStore localDB,
                     JasmineGraphHashMapCentralStore centralDB, string workerList) {
    std::vector<string> workerSockets;
    stringstream wl(workerList);
    string intermediate;
    while (getline(wl, intermediate, ',')) {
        workerSockets.push_back(intermediate);
    }
    map<long, map<long, unordered_set<long>>> egonetMap =
        calculateLocalEgoNet(graphID, partitionID, serverPort, localDB, centralDB, workerSockets);

    string instanceDataFolderLocation = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    string attributeFilePart = instanceDataFolderLocation + "/" + graphID + "_egonet_" + partitionID;
    ofstream partfile;
    partfile.open(attributeFilePart, std::fstream::trunc);
    for (map<long, map<long, unordered_set<long>>>::iterator it = egonetMap.begin(); it != egonetMap.end(); ++it) {
        map<long, unordered_set<long>> egonetInternalMap = it->second;
        for (map<long, unordered_set<long>>::iterator itm = egonetInternalMap.begin(); itm != egonetInternalMap.end();
             ++itm) {
            unordered_set<long> egonetInternalMapEdges = itm->second;
            for (unordered_set<long>::iterator ite = egonetInternalMapEdges.begin();
                 ite != egonetInternalMapEdges.end(); ++ite) {
                partfile << to_string(it->first) << "\t" << to_string(itm->first) << "\t" << to_string(*ite) << endl;
            }
        }
    }
    partfile.close();

    // todo  invoke other workers asynchronously
    for (vector<string>::iterator workerIt = workerSockets.begin(); workerIt != workerSockets.end(); ++workerIt) {
        instance_logger.info("Worker pair " + *workerIt);

        std::vector<string> workerSocketPair;
        stringstream wl(*workerIt);
        string intermediate;
        while (getline(wl, intermediate, ':')) {
            workerSocketPair.push_back(intermediate);
        }

        if (std::to_string(serverPort).compare(workerSocketPair[1]) == 0) {
            continue;
        }

        string host = workerSocketPair[0];
        int port = stoi(workerSocketPair[1]);
        int sockfd;
        char data[DATA_BUFFER_SIZE];
        bool loop = false;
        socklen_t len;
        struct sockaddr_in serv_addr;
        struct hostent *server;

        sockfd = socket(AF_INET, SOCK_STREAM, 0);

        if (sockfd < 0) {
            instance_logger.error("Cannot create socket");
            return;
        }

        server = gethostbyname(host.c_str());
        if (server == NULL) {
            instance_logger.error("ERROR, no host named " + host);
            return;
        }

        bzero((char *)&serv_addr, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
        serv_addr.sin_port = htons(port);
        if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
            return;
        }

        if (!Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::WORKER_EGO_NET)) {
            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
            close(sockfd);
            continue;
        }
        instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::WORKER_EGO_NET);

        string response = Utils::read_str_trim_wrapper(sockfd, data, INSTANCE_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
            instance_logger.error("Error reading from socket");
            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
            close(sockfd);
            continue;
        }
        instance_logger.info("Received : " + response);

        if (Utils::send_str_wrapper(sockfd, graphID)) {
            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
            close(sockfd);
            continue;
        }

        response = Utils::read_str_trim_wrapper(sockfd, data, INSTANCE_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
            instance_logger.error("Error reading from socket");
            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
            close(sockfd);
            continue;
        }
        instance_logger.info("Received : " + response);
        instance_logger.info("Partition ID  : " + workerSocketPair[2]);

        string egonetString;

        if (!Utils::send_str_wrapper(sockfd, workerSocketPair[2])) {
            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
            close(sockfd);
            continue;
        }
        instance_logger.info("Sent : Partition ID " + workerSocketPair[2]);

        response = Utils::read_str_trim_wrapper(sockfd, data, INSTANCE_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
            instance_logger.error("Error reading from socket");
            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
            close(sockfd);
            continue;
        }

        if (!Utils::send_str_wrapper(sockfd, workerList)) {
            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
            close(sockfd);
            continue;
        }
        instance_logger.info("Sent : Host List");

        response = Utils::read_str_trim_wrapper(sockfd, data, INSTANCE_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
            instance_logger.error("Error reading from socket");
            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
            close(sockfd);
            continue;
        }
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
    }
}

map<long, double> calculateLocalPageRank(string graphID, double alpha, string partitionID, int serverPort,
                                         int top_k_page_rank_value, string graphVertexCount,
                                         JasmineGraphHashMapLocalStore localDB,
                                         JasmineGraphHashMapCentralStore centralDB, std::vector<string> &workerSockets,
                                         int iterations) {
    auto t_start = std::chrono::high_resolution_clock::now();

    map<long, unordered_set<long>> centralGraphMap = centralDB.getUnderlyingHashMap();
    map<long, unordered_set<long>> localGraphMap = localDB.getUnderlyingHashMap();
    map<long, unordered_set<long>>::iterator localGraphMapIterator;
    map<long, unordered_set<long>>::iterator centralGraphMapIterator;

    std::vector<long> vertexVector;
    for (localGraphMapIterator = localGraphMap.begin(); localGraphMapIterator != localGraphMap.end();
         ++localGraphMapIterator) {
        long startVid = localGraphMapIterator->first;
        unordered_set<long> endVidSet = localGraphMapIterator->second;

        for (auto itr = endVidSet.begin(); itr != endVidSet.end(); ++itr) {
            if (localGraphMap.find(*itr) == localGraphMap.end()) {
                unordered_set<long> valueSet;
                localGraphMap[*itr] = valueSet;
            }
        }
    }

    long partitionVertexCount = localGraphMap.size();
    long worldOnlyVertexCount = atol(graphVertexCount.c_str()) - partitionVertexCount;

    double damp = 1 - alpha;
    int M = partitionVertexCount + 1;

    long adjacencyIndex[M];
    int counter = 0;

    for (localGraphMapIterator = localGraphMap.begin(); localGraphMapIterator != localGraphMap.end();
         ++localGraphMapIterator) {
        long startVid = localGraphMapIterator->first;

        adjacencyIndex[counter] = startVid;
        counter++;
    }

    adjacencyIndex[partitionVertexCount] = -1;

    long entireGraphSize = atol(graphVertexCount.c_str());
    float mu = damp / entireGraphSize;
    unordered_map<float, float> resultTreeMap;
    // calculating local pagerank
    map<long, double> rankMap;

    std::map<long, long> inDegreeDistribution;

    std::string dataDirPath = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");

    std::string partitionCount = Utils::getJasmineGraphProperty("org.jasminegraph.server.npartitions");
    int parCount = std::stoi(partitionCount);

    for (int partitionID = 0; partitionID < parCount; ++partitionID) {
        std::string iddFilePath = dataDirPath + "/" + graphID + "_idd_" + std::to_string(partitionID);
        std::ifstream dataFile;
        dataFile.open(iddFilePath);

        while (!dataFile.eof()) {
            std::string line;
            std::getline(dataFile, line);
            std::stringstream buffer(line);
            std::string temp;
            std::vector<long> values;

            while (getline(buffer, temp, '\t')) {
                values.push_back(::strtod(temp.c_str(), nullptr));
            }

            if (values.size() == 2) {
                long nodeID = values[0];
                long iddValue = values[1];

                inDegreeDistribution[nodeID] = std::max(inDegreeDistribution[nodeID], iddValue);
            }
        }

        dataFile.close();
    }

    string instanceDataFolderLocation = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    string attributeFilePart = instanceDataFolderLocation + "/" + graphID + "_idd_combine";
    ofstream partfile;
    partfile.open(attributeFilePart, std::fstream::trunc);
    for (map<long, long>::iterator it = inDegreeDistribution.begin(); it != inDegreeDistribution.end(); ++it) {
        partfile << to_string(it->first) << "\t" << to_string(it->second) << endl;
    }
    partfile.close();

    for (localGraphMapIterator = localGraphMap.begin(); localGraphMapIterator != localGraphMap.end();
         ++localGraphMapIterator) {
        auto inDegreeDistributionItr = inDegreeDistribution.find(localGraphMapIterator->first);

        if (inDegreeDistributionItr != inDegreeDistribution.end()) {
            long inDegree = inDegreeDistributionItr->second;
            double authorityScore = (alpha * 1 + mu) * inDegree;
            rankMap[inDegreeDistributionItr->first] = authorityScore;
        }
    }

    int count = 0;
    while (count < iterations) {
        for (localGraphMapIterator = localGraphMap.begin(); localGraphMapIterator != localGraphMap.end();
             ++localGraphMapIterator) {
            long startVid = localGraphMapIterator->first;
            unordered_set<long> endVidSet = localGraphMapIterator->second;
            double existingParentRank = 1;

            auto rankMapItr = rankMap.find(startVid);
            if (rankMapItr != rankMap.end()) {
                existingParentRank = rankMapItr->second;
            } else {
                rankMap[startVid] = existingParentRank;
            }

            long degree = endVidSet.size();
            double distributedRank = alpha * (existingParentRank / degree) + mu;

            for (long itr : endVidSet) {
                auto rankMapItr = rankMap.find(itr);

                double existingChildRank = 0;
                double finalRank = 0;
                if (rankMapItr != rankMap.end()) {
                    existingChildRank = rankMapItr->second;
                    finalRank = existingChildRank + distributedRank;

                    rankMapItr->second = finalRank;
                } else {
                    finalRank = existingChildRank + distributedRank;
                    rankMap[itr] = finalRank;
                }
            }
        }

        count++;
    }

    map<double, long> rankMapResults;
    map<long, double> finalPageRankResults;
    if (top_k_page_rank_value == -1) {
        instance_logger.info("PageRank is not implemented");
    } else {
        int count = 0;
        for (map<long, double>::iterator rankMapItr = rankMap.begin(); rankMapItr != rankMap.end(); ++rankMapItr) {
            finalPageRankResults[rankMapItr->first] = rankMapItr->second;
            count++;
        }
    }

    auto t_end = std::chrono::high_resolution_clock::now();
    double elapsed_time_ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();

    centralGraphMap.clear();
    localGraphMap.clear();
    resultTreeMap.clear();
    inDegreeDistribution.clear();
    rankMap.clear();
    rankMapResults.clear();
    instance_logger.info("Elapsed time for calculating PageRank (in ms) -----: " + to_string(elapsed_time_ms));
    return finalPageRankResults;
}

map<long, unordered_set<long>> getEdgesWorldToLocal(string graphID, string partitionID, int serverPort,
                                                    string graphVertexCount, JasmineGraphHashMapLocalStore localDB,
                                                    JasmineGraphHashMapCentralStore centralDB,
                                                    map<long, unordered_set<long>> &graphVertexMap,
                                                    std::vector<string> &workerSockets) {
    map<long, unordered_set<long>> worldToLocalVertexMap;
    for (vector<string>::iterator workerIt = workerSockets.begin(); workerIt != workerSockets.end(); ++workerIt) {
        std::vector<string> workerSocketPair;
        stringstream wl(*workerIt);
        string intermediate;
        while (getline(wl, intermediate, ':')) {
            workerSocketPair.push_back(intermediate);
        }

        std::string dataDirPath = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
        std::string centralGraphIdentifier = graphID + "_centralstore_" + workerSocketPair[2];

        std::string centralStoreFile = dataDirPath + "/" + centralGraphIdentifier;
        instance_logger.info("###INSTANCE### centralstore " + centralStoreFile);

        if (access(centralStoreFile.c_str(), R_OK) != 0) {
            instance_logger.error("Read permission denied for " + centralStoreFile);
            continue;
        }
        JasmineGraphHashMapCentralStore *centralStore = JasmineGraphInstanceService::loadCentralStore(centralStoreFile);
        const auto &centralGraphMap = centralStore->getUnderlyingHashMap();

        for (auto centralGraphMapIterator = centralGraphMap.begin(); centralGraphMapIterator != centralGraphMap.end();
             ++centralGraphMapIterator) {
            long startVid = centralGraphMapIterator->first;
            const unordered_set<long> &endVidSet = centralGraphMapIterator->second;

            for (auto itr = endVidSet.begin(); itr != endVidSet.end(); ++itr) {
                if (graphVertexMap.find(*itr) != graphVertexMap.end()) {
                    auto toIDIterator = worldToLocalVertexMap.find(*itr);
                    if (toIDIterator != worldToLocalVertexMap.end()) {
                        unordered_set<long> &fromIDs = toIDIterator->second;
                        fromIDs.insert(startVid);
                    } else {
                        unordered_set<long> fromIDs;
                        fromIDs.insert(startVid);
                        worldToLocalVertexMap[*itr] = fromIDs;
                    }
                }
            }
        }
        delete centralStore;
    }

    return worldToLocalVertexMap;
}

void JasmineGraphInstanceService::startCollectingLoadAverage() {
    int elapsedTime = 0;
    time_t start;

    start = time(0);
    while (collectValid) {
        time_t elapsed = time(0) - start;
        if (elapsed >= Conts::LOAD_AVG_COLLECTING_GAP) {
            elapsedTime += Conts::LOAD_AVG_COLLECTING_GAP * 1000;
            double loadAgerage = StatisticCollector::getLoadAverage();
            loadAverageVector.push_back(std::to_string(loadAgerage));
            start = start + Conts::LOAD_AVG_COLLECTING_GAP;
        } else {
            sleep(Conts::LOAD_AVG_COLLECTING_GAP - elapsed);
        }
    }
}

void JasmineGraphInstanceService::initServer(string trainData) {
    std::vector<std::string> trainargs = Utils::split(trainData, ' ');
    string graphID;
    string partitionID = trainargs[trainargs.size() - 1];

    for (int i = 0; i < trainargs.size(); i++) {
        if (trainargs[i] == "--graph_id") {
            graphID = trainargs[i + 1];
            break;
        }
    }

    std::vector<char *> vc;
    std::transform(trainargs.begin(), trainargs.end(), std::back_inserter(vc), converter);

    std::string log_file = "/tmp/jasminegraph/fl_server_" + partitionID + ".log";
    std::string path = "cd " + Utils::getJasmineGraphProperty("org.jasminegraph.fl.location") + " && ";
    std::string command =
        path + "python3 fl_server.py " + Utils::getJasmineGraphProperty("org.jasminegraph.fl.weights") + " " +
        Utils::getJasmineGraphProperty("org.jasminegraph.fl.dataDir") + " " +
        Utils::getJasmineGraphProperty("org.jasminegraph.fl.dataDir") + " " + graphID + " 0 " +
        Utils::getJasmineGraphProperty("org.jasminegraph.fl_clients") + " " +
        Utils::getJasmineGraphProperty("org.jasminegraph.fl.epochs") + " localhost 5000" + " >>" + log_file + " 2>&1";
    instance_logger.info("Executing : " + command);
    int exit_status = system(command.c_str());
    chmod(log_file.c_str(), 0666);
    if (exit_status == -1) {
        instance_logger.error("Failed executing python server for query");
    }
}

void JasmineGraphInstanceService::initOrgServer(string trainData) {
    std::vector<std::string> trainargs = Utils::split(trainData, ' ');
    std::string graphID;
    string partitionID = trainargs[trainargs.size() - 1];

    for (int i = 0; i < trainargs.size(); i++) {
        if (trainargs[i] == "--graph_id") {
            graphID = trainargs[i + 1];
            break;
        }
    }

    std::vector<char *> vc;
    std::transform(trainargs.begin(), trainargs.end(), std::back_inserter(vc), converter);

    std::string path = "cd " + Utils::getJasmineGraphProperty("org.jasminegraph.fl.location") + " && ";
    std::string command = path + "python3 org_server.py " + graphID + " " +
                          Utils::getJasmineGraphProperty("org.jasminegraph.fl_clients") + " " +
                          Utils::getJasmineGraphProperty("org.jasminegraph.fl.epochs") +
                          " localhost 5050 > /var/tmp/jasminegraph/logs/org_server_logs-" +
                          Utils::getCurrentTimestamp() + ".txt";
    instance_logger.info("Executing : " + command);
    int exit_status = system(command.c_str());
    if (exit_status == -1) {
        instance_logger.error("Failed executing python server for multi-organization query");
    }
}

void JasmineGraphInstanceService::initAgg(string trainData) {
    std::vector<std::string> trainargs = Utils::split(trainData, ' ');
    string graphID;
    string partitionID = trainargs[trainargs.size() - 1];

    for (int i = 0; i < trainargs.size(); i++) {
        if (trainargs[i] == "--graph_id") {
            graphID = trainargs[i + 1];
            break;
        }
    }

    std::vector<char *> vc;
    std::transform(trainargs.begin(), trainargs.end(), std::back_inserter(vc), converter);

    std::string path = "cd " + Utils::getJasmineGraphProperty("org.jasminegraph.fl.location") + " && ";
    std::string command = path + "python3 org_agg.py " + " " +
                          Utils::getJasmineGraphProperty("org.jasminegraph.fl.dataDir") + " " +
                          Utils::getJasmineGraphProperty("org.jasminegraph.fl.dataDir") + " " + "4" + " 0 " +
                          Utils::getJasmineGraphProperty("org.jasminegraph.fl.num.orgs") + " " +
                          Utils::getJasmineGraphProperty("org.jasminegraph.fl.epochs") + " localhost 5000 > " +
                          "/var/tmp/jasminegraph/logs/agg_logs-" + Utils::getCurrentTimestamp() + ".txt";
    instance_logger.info("Executing : " + command);
    int exit_status = system(command.c_str());
    if (exit_status == -1) {
        instance_logger.error("Failed to execute organization level aggregations");
    }
}

void JasmineGraphInstanceService::initClient(string trainData) {
    std::vector<std::string> trainargs = Utils::split(trainData, ' ');
    string graphID;
    string partitionID = trainargs[trainargs.size() - 1];

    for (int i = 0; i < trainargs.size(); i++) {
        if (trainargs[i] == "--graph_id") {
            graphID = trainargs[i + 1];
            break;
        }
    }

    std::vector<char *> vc;
    std::transform(trainargs.begin(), trainargs.end(), std::back_inserter(vc), converter);

    std::string log_file = "/tmp/jasminegraph/fl_client_" + partitionID + ".log";
    std::string path = "cd " + Utils::getJasmineGraphProperty("org.jasminegraph.fl.location") + " && ";
    std::string command =
        path + "python3 fl_client.py " + Utils::getJasmineGraphProperty("org.jasminegraph.fl.weights") + " " +
        Utils::getJasmineGraphProperty("org.jasminegraph.fl.dataDir") + " " +
        Utils::getJasmineGraphProperty("org.jasminegraph.fl.dataDir") + " " + graphID + " " + partitionID + " " +
        Utils::getJasmineGraphProperty("org.jasminegraph.fl.epochs") + " localhost " +
        Utils::getJasmineGraphProperty("org.jasminegraph.fl.org.port") + " >>" + log_file + " 2>&1";

    instance_logger.info("Executing : " + command);
    int exit_status = system(command.c_str());
    chmod(log_file.c_str(), 0666);
    if (exit_status == -1) {
        instance_logger.error("Could not start python client");
    }
}

void JasmineGraphInstanceService::mergeFiles(string trainData) {
    std::vector<std::string> trainargs = Utils::split(trainData, ' ');
    string graphID = trainargs[1];
    string partitionID = trainargs[2];
    int exit_status;

    std::string log_file = "/tmp/jasminegraph/merge_" + partitionID + ".log";
    std::string path = "cd " + Utils::getJasmineGraphProperty("org.jasminegraph.fl.location") + " && ";
    std::string command = path + "python3 merge.py " +
                          Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + " " +
                          Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.trainedmodelfolder") + " " +
                          Utils::getJasmineGraphProperty("org.jasminegraph.fl.dataDir") + " " + graphID + " " +
                          partitionID + " >>" + log_file + " 2>&1";

    instance_logger.info("Executing : " + command);
    exit_status = system(command.c_str());
    chmod(log_file.c_str(), 0666);
    if (exit_status == -1) {
        instance_logger.error("Merge Command Execution Failed for Graph ID - Patition ID: " + graphID + " - " +
                              partitionID + "; Error : " + strerror(errno));
    }
}

static void handshake_command(int connFd, bool *loop_exit_p) {
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::HANDSHAKE_OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK);

    char data[DATA_BUFFER_SIZE];
    masterIP = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received hostname : " + masterIP);

    instance_logger.info("Sending : " + JasmineGraphInstanceProtocol::HOST_OK);
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::HOST_OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("ServerName : " + masterIP);
}

static inline void close_command(int connFd, bool *loop_exit_p) {
    *loop_exit_p = true;
    close(connFd);
}

static inline void shutdown_command(int connFd) {
    close(connFd);
    pid_t ppid = getppid();
    kill(ppid, SIGTERM);
    exit(0);
}

static inline void ready_command(int connFd, bool *loop_exit_p) {
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
    }
}

static void batch_upload_common(int connFd, bool *loop_exit_p, bool batch_upload) {
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    char data[DATA_BUFFER_SIZE];
    string line;
    string graphID = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Graph ID: " + graphID);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::SEND_FILE_NAME)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME);

    string fileName = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
    instance_logger.info("Received File name: " + fileName);
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::SEND_FILE_LEN)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN);

    string size = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
    instance_logger.info("Received file size in bytes: " + size);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::SEND_FILE_CONT)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_CONT);

    string fullFilePath =
        Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + fileName;

    int fileSize = stoi(size);
    while (!Utils::fileExists(fullFilePath)) {
        instance_logger.info("Instance data file " + fullFilePath + " does not exist");
        sleep(1);
    }
    while (Utils::getFileSize(fullFilePath) < fileSize) {
        line = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
        if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) != 0) {
            instance_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::FILE_RECV_CHK +
                                  " ; Received: " + line);
            close(connFd);
            return;
        }
        if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::FILE_RECV_WAIT)) {
            *loop_exit_p = true;
            return;
        }
        instance_logger.info("Waiting for file to be received to " + fullFilePath);
    }

    line = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
    if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) != 0) {
        instance_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::FILE_RECV_CHK +
                              " ; Received: " + line);
        close(connFd);
        return;
    }
    instance_logger.info("Received : " + line);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::FILE_ACK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::FILE_ACK);

    instance_logger.info("File received and saved to " + fullFilePath);
    *loop_exit_p = true;

    string rawname = fileName;
    if (fullFilePath.compare(fullFilePath.size() - 3, 3, ".gz") == 0) {
        Utils::unzipFile(fullFilePath);
        size_t lastindex = fileName.find_last_of(".");
        rawname = fileName.substr(0, lastindex);
    }

    fullFilePath = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + rawname;

    if (batch_upload) {
        string partitionID = rawname.substr(rawname.find_last_of("_") + 1);
        pthread_mutex_lock(&file_lock);
        writeCatalogRecord(graphID + ":" + partitionID);
        pthread_mutex_unlock(&file_lock);
    }

    while (!Utils::fileExists(fullFilePath)) {
        line = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
        if (line.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK) != 0) {
            instance_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK +
                                  " ; Received: " + line);
            close(connFd);
            return;
        }
        instance_logger.info("Received : " + line);
        if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT)) {
            *loop_exit_p = true;
            return;
        }
        instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT);
    }

    line = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
    if (line.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK) != 0) {
        instance_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK +
                              " ; Received: " + line);
        close(connFd);
        return;
    }
    instance_logger.info("Received : " + line);
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK);
}

static void batch_upload_command(int connFd, bool *loop_exit_p) { batch_upload_common(connFd, loop_exit_p, true); }

static void batch_upload_central_command(int connFd, bool *loop_exit_p) {
    batch_upload_common(connFd, loop_exit_p, false);
}

static void batch_upload_composite_central_command(int connFd, bool *loop_exit_p) {
    batch_upload_common(connFd, loop_exit_p, false);
}

static void upload_rdf_attributes_command(int connFd, bool *loop_exit_p) {
    batch_upload_common(connFd, loop_exit_p, false);
}

static void upload_rdf_attributes_central_command(int connFd, bool *loop_exit_p) {
    batch_upload_common(connFd, loop_exit_p, false);
}

static void delete_graph_command(int connFd, bool *loop_exit_p) {
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    char data[DATA_BUFFER_SIZE];
    string graphID = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Graph ID: " + graphID);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::SEND_PARTITION_ID)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::SEND_PARTITION_ID);

    string partitionID = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
    instance_logger.info("Received partition ID: " + partitionID);
    deleteGraphPartition(graphID, partitionID);
    deleteStreamingGraphPartition(graphID, partitionID);
    // pthread_mutex_lock(&file_lock);
    // TODO :: Update catalog file
    // pthread_mutex_unlock(&file_lock);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }

    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);
}

static void delete_graph_fragment_command(int connFd, bool *loop_exit_p) {
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    char data[DATA_BUFFER_SIZE];
    string graphID = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Graph ID: " + graphID);
    // Method call for graph fragment deletion
    removeGraphFragments(graphID);
    // pthread_mutex_lock(&file_lock);
    // TODO :: Update catalog file
    // pthread_mutex_unlock(&file_lock);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);
}

static void duplicate_centralstore_command(int connFd, int serverPort, bool *loop_exit_p) {
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    char data[DATA_BUFFER_SIZE];
    string graphID = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Graph ID: " + graphID);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    string partitionID = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Partition ID: " + partitionID);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    string workerList = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Worker List " + workerList);

    std::vector<string> workerSockets;
    stringstream wl(workerList);
    string intermediate;
    while (getline(wl, intermediate, ',')) {
        workerSockets.push_back(intermediate);
    }

    JasmineGraphInstanceService::duplicateCentralStore(serverPort, stoi(graphID), stoi(partitionID), workerSockets,
                                                       masterIP);
}

static void worker_in_degree_distribution_command(
    int connFd, std::map<std::string, JasmineGraphHashMapLocalStore> &graphDBMapLocalStores,
    std::map<std::string, JasmineGraphHashMapCentralStore> &graphDBMapCentralStores, bool *loop_exit_p) {
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    char data[DATA_BUFFER_SIZE];
    string graphID = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Graph ID: " + graphID);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    string partitionID = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Partition ID: " + partitionID);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    string workerList = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received WorkerList: " + workerList);

    std::vector<string> workerSockets;
    stringstream wl(workerList);
    string intermediate;
    while (getline(wl, intermediate, ',')) {
        workerSockets.push_back(intermediate);
    }

    auto t_start = std::chrono::high_resolution_clock::now();

    map<long, long> degreeDistribution =
        calculateLocalInDegreeDist(graphID, partitionID, graphDBMapLocalStores, graphDBMapCentralStores);

    instance_logger.info("In Degree Dist size: " + to_string(degreeDistribution.size()));

    for (vector<string>::iterator workerIt = workerSockets.begin(); workerIt != workerSockets.end(); ++workerIt) {
        instance_logger.info("Worker pair " + *workerIt);

        std::vector<string> workerSocketPair;
        stringstream wl(*workerIt);
        string intermediate;
        while (getline(wl, intermediate, ':')) {
            workerSocketPair.push_back(intermediate);
        }
        string workerPartitionID = workerSocketPair[2];

        JasmineGraphHashMapCentralStore centralDB;

        std::map<std::string, JasmineGraphHashMapCentralStore>::iterator itcen;

        if (JasmineGraphInstanceService::isInstanceCentralStoreExists(graphID, workerPartitionID)) {
            JasmineGraphInstanceService::loadInstanceCentralStore(graphID, workerPartitionID, graphDBMapCentralStores);
        }
        centralDB = graphDBMapCentralStores[graphID + "_centralstore_" + workerPartitionID];

        map<long, long> degreeDistributionCentral = centralDB.getInDegreeDistributionHashMap();
        std::map<long, long>::iterator itcentral;
        std::map<long, long>::iterator its;

        for (its = degreeDistributionCentral.begin(); its != degreeDistributionCentral.end(); ++its) {
            bool centralNodeFound = false;
            map<long, long>::iterator degreeDistributionLocalItr = degreeDistribution.find(its->first);
            if (degreeDistributionLocalItr != degreeDistribution.end()) {
                long degreeDistributionValue = degreeDistributionLocalItr->second;
                degreeDistribution[degreeDistributionLocalItr->first] = degreeDistributionValue + its->second;
            }
        }

        instance_logger.info("Worker partition idd combined " + workerPartitionID);
    }

    auto t_end = std::chrono::high_resolution_clock::now();
    double elapsed_time_ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();

    instance_logger.info("Elapsed time idd in (ms) --------: " + to_string(elapsed_time_ms));

    string instanceDataFolderLocation = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    string attributeFilePart = instanceDataFolderLocation + "/" + graphID + "_idd_" + partitionID;
    ofstream partfile;
    partfile.open(attributeFilePart, std::fstream::trunc);
    for (map<long, long>::iterator it = degreeDistribution.begin(); it != degreeDistribution.end(); ++it) {
        partfile << to_string(it->first) << "\t" << to_string(it->second) << endl;
    }
    partfile.close();

    *loop_exit_p = true;
}

static void degree_distribution_common(int connFd, int serverPort,
                                       std::map<std::string, JasmineGraphHashMapLocalStore> &graphDBMapLocalStores,
                                       std::map<std::string, JasmineGraphHashMapCentralStore> &graphDBMapCentralStores,
                                       bool *loop_exit_p, bool in) {
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    char data[DATA_BUFFER_SIZE];
    string graphID = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Graph ID: " + graphID);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    string partitionID = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Partition ID: " + partitionID);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    string workerList = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Worker List " + workerList);

    std::vector<string> workerSockets;
    stringstream wl(workerList);
    string intermediate;
    while (getline(wl, intermediate, ',')) {
        workerSockets.push_back(intermediate);
    }

    // Calculate the degree distribution
    map<long, long> degreeDistribution;
    if (in) {
        degreeDistribution = calculateInDegreeDist(graphID, partitionID, serverPort, graphDBMapLocalStores,
                                                   graphDBMapCentralStores, workerSockets, workerList);
    } else {
        degreeDistribution = calculateOutDegreeDist(graphID, partitionID, serverPort, graphDBMapLocalStores,
                                                    graphDBMapCentralStores, workerSockets);
    }
    degreeDistribution.clear();
    *loop_exit_p = true;
}

static void in_degree_distribution_command(
    int connFd, int serverPort, std::map<std::string, JasmineGraphHashMapLocalStore> &graphDBMapLocalStores,
    std::map<std::string, JasmineGraphHashMapCentralStore> &graphDBMapCentralStores, bool *loop_exit_p) {
    degree_distribution_common(connFd, serverPort, graphDBMapLocalStores, graphDBMapCentralStores, loop_exit_p, true);
}

static void worker_out_degree_distribution_command(
    int connFd, std::map<std::string, JasmineGraphHashMapLocalStore> &graphDBMapLocalStores,
    std::map<std::string, JasmineGraphHashMapCentralStore> &graphDBMapCentralStores, bool *loop_exit_p) {
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    char data[DATA_BUFFER_SIZE];
    string graphID = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Graph ID: " + graphID);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    string partitionID = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Partition ID: " + partitionID);

    map<long, long> degreeDistribution =
        calculateLocalOutDegreeDist(graphID, partitionID, graphDBMapLocalStores, graphDBMapCentralStores);
    instance_logger.info("Degree Dist size: " + to_string(degreeDistribution.size()));

    string instanceDataFolderLocation = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    string attributeFilePart = instanceDataFolderLocation + "/" + graphID + "_odd_" + partitionID;
    ofstream partfile;
    partfile.open(attributeFilePart, std::fstream::trunc);
    for (map<long, long>::iterator it = degreeDistribution.begin(); it != degreeDistribution.end(); ++it) {
        partfile << to_string(it->first) << "\t" << to_string(it->second) << endl;
    }
    partfile.close();
}

static void out_degree_distribution_command(
    int connFd, int serverPort, std::map<std::string, JasmineGraphHashMapLocalStore> &graphDBMapLocalStores,
    std::map<std::string, JasmineGraphHashMapCentralStore> &graphDBMapCentralStores, bool *loop_exit_p) {
    degree_distribution_common(connFd, serverPort, graphDBMapLocalStores, graphDBMapCentralStores, loop_exit_p, false);
}

static void page_rank_command(int connFd, int serverPort,
                              std::map<std::string, JasmineGraphHashMapCentralStore> &graphDBMapCentralStores,
                              bool *loop_exit_p) {
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    char data[DATA_BUFFER_SIZE];
    string graphID = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Graph ID: " + graphID);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    string partitionID = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Partition ID: " + partitionID);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    string workerList = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Worker List " + workerList);

    std::vector<string> workerSockets;
    stringstream wl(workerList);
    string intermediate;
    while (getline(wl, intermediate, ',')) {
        workerSockets.push_back(intermediate);
    }

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    string graphVertexCount = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Graph ID:" + graphID + " Vertex Count: " + graphVertexCount);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    string alphaValue = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received alpha: " + alphaValue);

    double alpha = std::stod(alphaValue);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    string iterationsValue = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received iteration count: " + iterationsValue);

    int iterations = stoi(iterationsValue);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    JasmineGraphHashMapLocalStore graphDB;
    JasmineGraphHashMapCentralStore centralDB;

    std::map<std::string, JasmineGraphHashMapLocalStore> graphDBMapLocalStoresPgrnk;
    if (JasmineGraphInstanceService::isGraphDBExists(graphID, partitionID)) {
        JasmineGraphInstanceService::loadLocalStore(graphID, partitionID, graphDBMapLocalStoresPgrnk);
    }

    if (JasmineGraphInstanceService::isInstanceCentralStoreExists(graphID, partitionID)) {
        JasmineGraphInstanceService::loadInstanceCentralStore(graphID, partitionID, graphDBMapCentralStores);
    }

    graphDB = graphDBMapLocalStoresPgrnk[graphID + "_" + partitionID];
    centralDB = graphDBMapCentralStores[graphID + "_centralstore_" + partitionID];

    instance_logger.info("Start : Calculate Local PageRank");

    map<long, double> pageRankResults =
        calculateLocalPageRank(graphID, alpha, partitionID, serverPort, TOP_K_PAGE_RANK, graphVertexCount, graphDB,
                               centralDB, workerSockets, iterations);
    instance_logger.info("PageRank size: " + to_string(pageRankResults.size()));

    map<long, double> pageRankLocalstore;
    map<long, unordered_set<long>> localGraphMap = graphDB.getUnderlyingHashMap();
    map<long, unordered_set<long>>::iterator localGraphMapIterator;
    std::vector<long> vertexVector;
    for (localGraphMapIterator = localGraphMap.begin(); localGraphMapIterator != localGraphMap.end();
         ++localGraphMapIterator) {
        long startVid = localGraphMapIterator->first;
        unordered_set<long> endVidSet = localGraphMapIterator->second;

        map<long, double>::iterator pageRankValue = pageRankResults.find(startVid);
        if (pageRankValue == pageRankResults.end()) {
            pageRankLocalstore[startVid] = 0.0;
        } else {
            double value = pageRankValue->second;
            pageRankLocalstore[startVid] = value;
        }

        for (auto a = endVidSet.begin(); a != endVidSet.end(); ++a) {
            long endVid = *a;
            map<long, double>::iterator pageRankValue = pageRankResults.find(endVid);
            if (pageRankLocalstore.find(endVid) == pageRankLocalstore.end()) {
                if (pageRankValue == pageRankResults.end()) {
                    pageRankLocalstore[endVid] = 0.0;
                } else {
                    double value = pageRankValue->second;
                    pageRankLocalstore[endVid] = value;
                }
            }
        }
    }

    string instanceDataFolderLocation = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    string attributeFilePart = instanceDataFolderLocation + "/" + graphID + "_pgrnk_" + partitionID;
    ofstream partfile;
    partfile.open(attributeFilePart, std::fstream::trunc);
    for (map<long, double>::iterator it = pageRankLocalstore.begin(); it != pageRankLocalstore.end(); ++it) {
        partfile << to_string(it->first) << "\t" << to_string(it->second) << endl;
    }
    partfile.close();

    *loop_exit_p = true;
    pageRankResults.clear();
    localGraphMap.clear();
    pageRankLocalstore.clear();
    graphDBMapCentralStores.clear();
    graphDBMapLocalStoresPgrnk.clear();

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    instance_logger.info("Finish : Calculate Local PageRank.");
}

static void worker_page_rank_distribution_command(
    int connFd, int serverPort, std::map<std::string, JasmineGraphHashMapCentralStore> &graphDBMapCentralStores,
    bool *loop_exit_p) {
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    char data[DATA_BUFFER_SIZE];
    string graphID = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Graph ID: " + graphID);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    string partitionID = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Partition ID: " + partitionID);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    string workerList = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Worker List " + workerList);

    std::vector<string> workerSockets;
    stringstream wl(workerList);
    string intermediate;
    while (getline(wl, intermediate, ',')) {
        workerSockets.push_back(intermediate);
    }

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    string graphVertexCount = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Graph ID:" + graphID + " Vertex Count: " + graphVertexCount);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    string alphaValue = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received alpha: " + alphaValue);

    double alpha = std::stod(alphaValue);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    string iterationsValue = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received iterations: " + iterationsValue);

    int iterations = stoi(iterationsValue);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    JasmineGraphHashMapLocalStore graphDB;
    JasmineGraphHashMapCentralStore centralDB;

    std::map<std::string, JasmineGraphHashMapLocalStore> graphDBMapLocalStoresPgrnk;
    if (JasmineGraphInstanceService::isGraphDBExists(graphID, partitionID)) {
        JasmineGraphInstanceService::loadLocalStore(graphID, partitionID, graphDBMapLocalStoresPgrnk);
    }

    if (JasmineGraphInstanceService::isInstanceCentralStoreExists(graphID, partitionID)) {
        JasmineGraphInstanceService::loadInstanceCentralStore(graphID, partitionID, graphDBMapCentralStores);
    }

    graphDB = graphDBMapLocalStoresPgrnk[graphID + "_" + partitionID];
    centralDB = graphDBMapCentralStores[graphID + "_centralstore_" + partitionID];

    map<long, double> pageRankResults =
        calculateLocalPageRank(graphID, alpha, partitionID, serverPort, TOP_K_PAGE_RANK, graphVertexCount, graphDB,
                               centralDB, workerSockets, iterations);

    instance_logger.info("PageRank size: " + to_string(pageRankResults.size()));

    map<long, double> pageRankLocalstore;
    map<long, unordered_set<long>> localGraphMap = graphDB.getUnderlyingHashMap();
    map<long, unordered_set<long>>::iterator localGraphMapIterator;
    std::vector<long> vertexVector;
    for (localGraphMapIterator = localGraphMap.begin(); localGraphMapIterator != localGraphMap.end();
         ++localGraphMapIterator) {
        long startVid = localGraphMapIterator->first;
        unordered_set<long> endVidSet = localGraphMapIterator->second;

        map<long, double>::iterator pageRankValue = pageRankResults.find(startVid);
        if (pageRankValue == pageRankResults.end()) {
            pageRankLocalstore[startVid] = 1.0;
        } else {
            double value = pageRankValue->second;
            pageRankLocalstore[startVid] = value;
        }
    }

    string instanceDataFolderLocation = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    string attributeFilePart = instanceDataFolderLocation + "/" + graphID + "_pgrnk_" + partitionID;
    ofstream partfile;
    partfile.open(attributeFilePart, std::fstream::trunc);
    for (map<long, double>::iterator it = pageRankLocalstore.begin(); it != pageRankLocalstore.end(); ++it) {
        partfile << to_string(it->first) << "\t" << to_string(it->second) << endl;
    }
    partfile.close();

    pageRankResults.clear();
    pageRankLocalstore.clear();
    localGraphMap.clear();
}

static void egonet_command(int connFd, int serverPort,
                           std::map<std::string, JasmineGraphHashMapCentralStore> &graphDBMapCentralStores,
                           bool *loop_exit_p) {
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    char data[DATA_BUFFER_SIZE];
    string graphID = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Graph ID: " + graphID);

    if (Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);
    }

    string partitionID = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Partition ID: " + partitionID);

    if (Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);
    }

    string workerList = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Worker List " + workerList);

    if (Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);
    }

    JasmineGraphHashMapLocalStore graphDB;
    JasmineGraphHashMapCentralStore centralDB;

    std::map<std::string, JasmineGraphHashMapLocalStore> graphDBMapLocalStoresPgrnk;
    if (JasmineGraphInstanceService::isGraphDBExists(graphID, partitionID)) {
        JasmineGraphInstanceService::loadLocalStore(graphID, partitionID, graphDBMapLocalStoresPgrnk);
    }

    if (JasmineGraphInstanceService::isInstanceCentralStoreExists(graphID, partitionID)) {
        JasmineGraphInstanceService::loadInstanceCentralStore(graphID, partitionID, graphDBMapCentralStores);
    }

    graphDB = graphDBMapLocalStoresPgrnk[graphID + "_" + partitionID];
    centralDB = graphDBMapCentralStores[graphID + "_centralstore_" + partitionID];

    calculateEgoNet(graphID, partitionID, serverPort, graphDB, centralDB, workerList);
}

static void worker_egonet_command(int connFd, int serverPort,
                                  std::map<std::string, JasmineGraphHashMapCentralStore> &graphDBMapCentralStores,
                                  bool *loop_exit_p) {
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    char data[DATA_BUFFER_SIZE];
    string graphID = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Graph ID: " + graphID);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    string partitionID = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Partition ID: " + partitionID);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    string workerList = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Worker List " + workerList);

    std::vector<string> workerSockets;
    stringstream wl(workerList);
    string intermediate;
    while (getline(wl, intermediate, ',')) {
        workerSockets.push_back(intermediate);
    }

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    JasmineGraphHashMapLocalStore graphDB;
    JasmineGraphHashMapCentralStore centralDB;

    std::map<std::string, JasmineGraphHashMapLocalStore> graphDBMapLocalStoresPgrnk;
    if (JasmineGraphInstanceService::isGraphDBExists(graphID, partitionID)) {
        JasmineGraphInstanceService::loadLocalStore(graphID, partitionID, graphDBMapLocalStoresPgrnk);
    }

    if (JasmineGraphInstanceService::isInstanceCentralStoreExists(graphID, partitionID)) {
        JasmineGraphInstanceService::loadInstanceCentralStore(graphID, partitionID, graphDBMapCentralStores);
    }

    graphDB = graphDBMapLocalStoresPgrnk[graphID + "_" + partitionID];
    centralDB = graphDBMapCentralStores[graphID + "_centralstore_" + partitionID];

    map<long, map<long, unordered_set<long>>> egonetMap =
        calculateLocalEgoNet(graphID, partitionID, serverPort, graphDB, centralDB, workerSockets);

    string instanceDataFolderLocation = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    string attributeFilePart = instanceDataFolderLocation + "/" + graphID + "_egonet_" + partitionID;
    ofstream partfile;
    partfile.open(attributeFilePart, std::fstream::trunc);
    for (map<long, map<long, unordered_set<long>>>::iterator it = egonetMap.begin(); it != egonetMap.end(); ++it) {
        map<long, unordered_set<long>> egonetInternalMap = it->second;
        for (map<long, unordered_set<long>>::iterator itm = egonetInternalMap.begin(); itm != egonetInternalMap.end();
             ++itm) {
            unordered_set<long> egonetInternalMapEdges = itm->second;
            for (unordered_set<long>::iterator ite = egonetInternalMapEdges.begin();
                 ite != egonetInternalMapEdges.end(); ++ite) {
                partfile << to_string(it->first) << "\t" << to_string(itm->first) << "\t" << to_string(*ite) << endl;
            }
        }
    }
    partfile.close();

    instance_logger.info("Egonet calculation completed");
}

static void triangles_command(
    int connFd, int serverPort, std::map<std::string, JasmineGraphHashMapLocalStore> &graphDBMapLocalStores,
    std::map<std::string, JasmineGraphHashMapCentralStore> &graphDBMapCentralStores,
    std::map<std::string, JasmineGraphHashMapDuplicateCentralStore> &graphDBMapDuplicateCentralStores,
    bool *loop_exit_p) {
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    char data[DATA_BUFFER_SIZE];
    string graphID = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Graph ID: " + graphID);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }

    string partitionId = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Partition ID: " + partitionId);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }

    string priority = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Priority : " + priority);

    int threadPriority = stoi(priority);

    if (threadPriority > Conts::DEFAULT_THREAD_PRIORITY) {
        threadPriorityMutex.lock();
        workerHighPriorityTaskCount++;
        highestPriority = threadPriority;
        threadPriorityMutex.unlock();
    }

    std::thread perfThread = std::thread(&PerformanceUtil::collectPerformanceStatistics);
    perfThread.detach();
    long localCount = countLocalTriangles(graphID, partitionId, graphDBMapLocalStores, graphDBMapCentralStores,
                                          graphDBMapDuplicateCentralStores, threadPriority);

    if (threadPriority > Conts::DEFAULT_THREAD_PRIORITY) {
        threadPriorityMutex.lock();
        workerHighPriorityTaskCount--;

        if (workerHighPriorityTaskCount == 0) {
            highestPriority = Conts::DEFAULT_THREAD_PRIORITY;
        }
        threadPriorityMutex.unlock();
    }

    std::string result = to_string(localCount);
    if (!Utils::send_str_wrapper(connFd, result)) {
        *loop_exit_p = true;
    }
}

static void streaming_triangles_command(
    int connFd, int serverPort, std::map<std::string, JasmineGraphIncrementalLocalStore *> &incrementalLocalStoreMap,
    bool *loop_exit_p) {
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    char data[DATA_BUFFER_SIZE];
    string graphID = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Graph ID: " + graphID);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }

    string partitionId = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Partition ID: " + partitionId);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }

    string oldLocalRelationCount = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received oldLocalRelationCount: " + oldLocalRelationCount);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }

    string oldCentralRelationCount = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received oldCentralRelationCount: " + oldCentralRelationCount);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }

    string mode = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received mode: " + mode);

    std::string graphIdentifier = graphID + "_" + partitionId;
    JasmineGraphIncrementalLocalStore *incrementalLocalStoreInstance;

    if (incrementalLocalStoreMap.find(graphIdentifier) == incrementalLocalStoreMap.end()) {
        incrementalLocalStoreInstance =
            JasmineGraphInstanceService::loadStreamingStore(graphID, partitionId, incrementalLocalStoreMap, "app", false);
    } else {
        incrementalLocalStoreInstance = incrementalLocalStoreMap[graphIdentifier];
    }

    NativeStoreTriangleResult localCount;
    if (mode == "0") {
        localCount = StreamingTriangles::countLocalStreamingTriangles(incrementalLocalStoreInstance);
    } else {
        localCount = StreamingTriangles::countDynamicLocalTriangles(
            incrementalLocalStoreInstance, std::stol(oldLocalRelationCount), std::stol(oldCentralRelationCount));
    }

    long newLocalRelationCount, newCentralRelationCount, result;
    newLocalRelationCount = localCount.localRelationCount;
    newCentralRelationCount = localCount.centralRelationCount;
    result = localCount.result;

    if (!Utils::send_str_wrapper(connFd, std::to_string(newLocalRelationCount))) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent New local relation count: " + std::to_string(newLocalRelationCount));

    string response = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        instance_logger.error("Received : " + response + " instead of : " + JasmineGraphInstanceProtocol::HOST_OK);
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);

    if (!Utils::send_str_wrapper(connFd, std::to_string(newCentralRelationCount))) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent New central relation count: " + std::to_string(newCentralRelationCount));

    response = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        instance_logger.error("Received : " + response + " instead of : " + JasmineGraphInstanceProtocol::HOST_OK);
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);

    if (!Utils::send_str_wrapper(connFd, std::to_string(result))) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent result: " + std::to_string(result));

    instance_logger.info("Streaming triangle count sent successfully");
}

static void streaming_kg_construction(
    int connFd, int serverPort,
    std::map<std::string, JasmineGraphIncrementalLocalStore *>
        &incrementalLocalStoreMap,
    bool *loop_exit_p) {
  if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
    *loop_exit_p = true;
    return;
  }
  instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

  char data[DATA_BUFFER_SIZE];
  string graphID =
      Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
  instance_logger.info("Received Graph ID: " + graphID);

  if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
    *loop_exit_p = true;
    return;
  }
  instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

  string isResume =
      Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
  instance_logger.info("Received isResume: " + isResume);

  if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
    *loop_exit_p = true;
    return;
  }
  instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

  long startFromBytes;
  if (isResume == "y") {
    startFromBytes = std::stol(
        Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH));
    instance_logger.info("Received uploadedBytes: " +
                         std::to_string(startFromBytes));

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
      *loop_exit_p = true;
      return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);
  } else {
    startFromBytes = 0;
  }

  string llm_runner =
      Utils::read_str_trim_wrapper(connFd, data, INSTANCE_LONG_DATA_LENGTH);
  instance_logger.info("Received LLM Runner: " + llm_runner);

  if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
    *loop_exit_p = true;
    return;
  }
  instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

  string llm_inference_engine =
      Utils::read_str_trim_wrapper(connFd, data, INSTANCE_LONG_DATA_LENGTH);
  instance_logger.info("Received LLM Inference Engine: " +
                       llm_inference_engine);

  if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
    *loop_exit_p = true;
    return;
  }
  instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

  string llm =
      Utils::read_str_trim_wrapper(connFd, data, INSTANCE_LONG_DATA_LENGTH);
  instance_logger.info("Received LLM : " + llm);

  if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
    *loop_exit_p = true;
    return;
  }
  instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

  string chunkSize =
      Utils::read_str_trim_wrapper(connFd, data, INSTANCE_LONG_DATA_LENGTH);
  instance_logger.info("Received Chunk Size : " + chunkSize);

  if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
    *loop_exit_p = true;
    return;
  }
  instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

  string chunksPerBatch =
      Utils::read_str_trim_wrapper(connFd, data, INSTANCE_LONG_DATA_LENGTH);
  instance_logger.info("Received chunksPerBatch : " + chunksPerBatch);

  if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
    *loop_exit_p = true;
    return;
  }
  instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

  int noOfPartitions =
      stoi(Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH));
  instance_logger.info("Received Number of Partitions: " +
                       to_string(noOfPartitions));
  if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
    *loop_exit_p = true;
    return;
  }
  instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

  string hdfsServerUrl =
      Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
  instance_logger.info("Received HDFS Server URL: " + hdfsServerUrl);
  if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
    *loop_exit_p = true;
    return;
  }

  string hdfsPort =
      Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
  instance_logger.info("Received HDFS Port: " + hdfsPort);
  if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
    *loop_exit_p = true;
    return;
  }

  masterIP = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
  instance_logger.info("Received MasterIP: " + masterIP);
  if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
    *loop_exit_p = true;
    return;
  }

  // read workerIP:port in comma separated format
  string workersIP =
      Utils::read_str_trim_wrapper(connFd, data, INSTANCE_LONG_DATA_LENGTH);
  instance_logger.info("Received Worker IP: " + workersIP);
  if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
    *loop_exit_p = true;
    return;
  }
  std::vector<string> workerSockets;
  stringstream wl(workersIP);
  string intermediate;
  while (getline(wl, intermediate, ',')) {
    workerSockets.push_back(intermediate);
  }
  std::vector<JasmineGraphServer::worker> workers;
  for (const auto &workerSocket : workerSockets) {
    JasmineGraphServer::worker worker;
    size_t pos = workerSocket.find(":");
    if (pos != string::npos) {
      worker.hostname = workerSocket.substr(0, pos);
      worker.port = stoi(workerSocket.substr(pos + 1));
      worker.dataPort =
          worker.port +
          1;  // Assuming data port is one more than the worker port
    } else {
      instance_logger.error("Invalid worker socket format: " + workerSocket);
      *loop_exit_p = true;
      return;
    }
    workers.push_back(worker);
  }

  std::vector<string> llmRunnerSockets;
  stringstream llm_(llm_runner);
  string intermediate_llm;
  while (getline(llm_, intermediate_llm, ',')) {
    llmRunnerSockets.push_back(intermediate_llm);
  }

  string hdfsPath =
      Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
  instance_logger.info("Received HDFS Path: " + hdfsPath);
  if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
    *loop_exit_p = true;
    return;
  }
  // instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);
  HDFSConnector *hdfsConnector = new HDFSConnector(hdfsServerUrl, hdfsPort);

  Pipeline *streamHandler = new Pipeline(
      connFd, hdfsConnector->getFileSystem(), hdfsPath, noOfPartitions,
      std::stoi(graphID), masterIP, workers, llmRunnerSockets,
      llm_inference_engine, llm, chunkSize, chunksPerBatch, startFromBytes);
  instance_logger.info("Started listening to " + hdfsPath);

  streamHandler->init();

  // int conResultWr = write( , DONE.c_str(), DONE.length());
  // if (!Utils::send_str_wrapper(connFd, DONE.c_str()))
  // {
  //     *loop_exit_p     = true;
  //     return;
  //
  // }
  *loop_exit_p = true;

  close(connFd);
}

static void streaming_tuple_extraction(
    int connFd, int serverPort,
    std::map<std::string, JasmineGraphIncrementalLocalStore *>
        &incrementalLocalStoreMap,
    bool *loop_exit_p) {
  char data[DATA_BUFFER_SIZE];

  instance_logger.info("in streaming_tuple_extraction");
  Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK);
  // instance_logger.info("in streaming_tuple_extraction");

  // 2. Expect graphID
  std::string graphID =
      Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
  Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK);

  // 3. Expect LLM runner hostname and port
  std::string llmHost =
      Utils::read_str_trim_wrapper(connFd, data, INSTANCE_LONG_DATA_LENGTH);
  Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK);

  std::string llmInferenceEngine =
      Utils::read_str_trim_wrapper(connFd, data, INSTANCE_LONG_DATA_LENGTH);
  Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK);

  std::string llm =
      Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
  Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK);
  // instance_logger.info("LLM Host and Port: " + llmHostPort);
  // split llmHostPort into hostname and port
  // size_t pos = llmHostPort.find(":");
  // if (pos == std::string::npos) {
  //     instance_logger.error("Invalid LLM host and port format");
  //     *loop_exit_p = true;
  //     return;
  // // }
  // std::string llmHost = llmHostPort.substr(0, pos);
  // int llmPort = std::stoi(llmHostPort.substr(pos + 1));
  instance_logger.info("LLM Host: " + llmHost);
  instance_logger.info("LLM : " + llm);
  std::unique_ptr<TupleStreamer> streamer;

  if (llmInferenceEngine == "ollama") {
    streamer = std::make_unique<OllamaTupleStreamer>(llm, llmHost);
  } else {
    streamer = std::make_unique<VLLMTupleStreamer>(llm, llmHost);
  }
  // VLLMTupleStreamer streamer("meta-llama/Llama-3.2-3B-Instruct", llmHost);

  // // VLLMTupleStreamer streamer("numind/NuExtract-2.0-4B", llmHost);
  // VLLMTupleStreamer streamer("meta-llama/Meta-Llama-3-8B", llmHost);
  // VLLMTupleStreamer streamer("SciPhi/Triplex", llmHost);
  // VLLMTupleStreamer streamer(llm, llmHost);

  SharedBuffer sharedBuffer(5);
  SharedBuffer tupleBuffer(5);
  std::condition_variable dataBufferCV;
  std::mutex dataBufferMutex;

  while (true) {
    std::string command =
        Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);

    if (command == JasmineGraphInstanceProtocol::CHUNK_STREAM_END) {
      Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK);
      break;
    }
    instance_logger.info("Received command: " + command);
    if (command != JasmineGraphInstanceProtocol::QUERY_DATA_START) {
      break;
    }
    Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK);

    int content_length;
    recv(connFd, &content_length, sizeof(int), 0);
    content_length = ntohl(content_length);
    Utils::send_str_wrapper(
        connFd, JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK);
    instance_logger.info("Received content length: " +
                         std::to_string(content_length));
    std::string chunk(content_length, 0);
    recv(connFd, &chunk[0], content_length, 0);
    Utils::send_str_wrapper(connFd,
                            JasmineGraphInstanceProtocol::GRAPH_DATA_SUCCESS);
    instance_logger.info(chunk);
    // Process and add to buffers
    // sharedBuffer.add(chunk);

    // Consumer thread that prints tuples from buffer
    std::thread consumer([&]() {
      Utils::send_str_wrapper(connFd,
                              JasmineGraphInstanceProtocol::QUERY_DATA_START);
      if (!Utils::expect_str_wrapper(connFd,
                                     JasmineGraphInstanceProtocol::OK)) {
        instance_logger.error("Error in receving query-start-ack");
        *loop_exit_p = true;
        close(connFd);
      };
      instance_logger.info("3200");
      int idleTimeoutSec = 120;  // e.g., break if no tuple for 30s

      while (true) {
        auto optTupleData = tupleBuffer.getWithTimeout(idleTimeoutSec);
        std::string tupleData;
        if (!optTupleData.has_value()) {
          instance_logger.error("No tuple received for " +
                                std::to_string(idleTimeoutSec) +
                                "s. Exiting...");
          tupleData = "-1";  // End signal
        } else {
          tupleData = *optTupleData;
        }

        int tuple_length = tupleData.length();
        int converted_number = htonl(tuple_length);

        if (!Utils::sendIntExpectResponse(
                connFd, data,
                JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK
                    .length(),
                converted_number,
                JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK)) {
          instance_logger.error("Error in receiving GRAPH_STREAM_C_length_ACK");
          *loop_exit_p = true;
          close(connFd);
          break;
        }

        // instance_logger.debug("3208 : " + tupleData);
        Utils::send_str_wrapper(connFd, tupleData);
        char ack1[FED_DATA_LENGTH + 1];

        string response =
            Utils::read_str_wrapper(connFd, ack1, FED_DATA_LENGTH);
        if (response == "stop") {
          tupleBuffer.clear();
          break;
        }

        // Utils::expect_str_wrapper(connFd,
        // JasmineGraphInstanceProtocol::GRAPH_DATA_SUCCESS);

        if (tupleData == "-1") {
          instance_logger.info("Received end signal from producer");
          tupleBuffer.clear();
          break;
        }
      }
    });
    streamer->streamChunk("chunk1", chunk, tupleBuffer);
    // streamer.processChunk("chunk1", chunk, tupleBuffer);

    consumer.join();

    // tupleBuffer.add("ProcessedTupleFor:" + sharedBuffer.get()); // Example
    instance_logger.info("3196");
    // Send tuple back to client
  }

  // Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::CLOSE);
  // close(connFd);
}



static void send_centralstore_to_aggregator_command(int connFd, bool *loop_exit_p) {
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::SEND_FILE_NAME)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME);

    char data[DATA_BUFFER_SIZE];
    string fileName = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
    instance_logger.info("Received File name: " + fileName);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::SEND_FILE_LEN)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN);

    string size = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
    instance_logger.info("Received file size in bytes: " + size);
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::SEND_FILE_CONT)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_CONT);
    string fullFilePath =
        Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + fileName;
    string line;
    int fileSize = stoi(size);
    while (!Utils::fileExists(fullFilePath)) {
        sleep(1);
    }
    while (Utils::getFileSize(fullFilePath) < fileSize) {
        line = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
        if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) != 0) {
            *loop_exit_p = true;
            return;
        }
        if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::FILE_RECV_WAIT)) {
            *loop_exit_p = true;
            return;
        }
    }

    line = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
    if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) != 0) {
        instance_logger.error("Received : " + line);
    }
    instance_logger.info("Received : " + line);
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::FILE_ACK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::FILE_ACK);

    instance_logger.info("File received and saved to " + fullFilePath);
    *loop_exit_p = true;

    Utils::unzipFile(fullFilePath);
    size_t lastindex = fileName.find_last_of(".");
    string rawname = fileName.substr(0, lastindex);
    fullFilePath = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + rawname;
    std::string aggregatorDirPath = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.aggregatefolder");

    if (Utils::copyToDirectory(fullFilePath, aggregatorDirPath)) {
        instance_logger.error("Copying " + fullFilePath + " into " + aggregatorDirPath + " failed");
    }

    line = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
    if (line.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK) == 0) {
        instance_logger.info("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK);
        if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK)) {
            *loop_exit_p = true;
            return;
        }
        instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK);
    }
}

static void send_composite_centralstore_to_aggregator_command(int connFd, bool *loop_exit_p) {
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::SEND_FILE_NAME)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME);

    char data[DATA_BUFFER_SIZE];
    string fileName = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
    instance_logger.info("Received File name: " + fileName);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::SEND_FILE_LEN)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN);

    string size = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
    instance_logger.info("Received file size in bytes: " + size);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::SEND_FILE_CONT)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_CONT);

    string fullFilePath =
        Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + fileName;
    string line;
    int fileSize = stoi(size);
    while (!Utils::fileExists(fullFilePath)) {
        sleep(1);
    }
    while (Utils::getFileSize(fullFilePath) < fileSize) {
        line = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
        if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) != 0) {
            *loop_exit_p = true;
            return;
        }
        if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::FILE_RECV_WAIT)) {
            *loop_exit_p = true;
            return;
        }
    }

    line = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
    if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) != 0) {
        instance_logger.error("Received : " + line);
    }
    instance_logger.info("Received : " + line);
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::FILE_ACK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::FILE_ACK);

    instance_logger.info("File received and saved to " + fullFilePath);
    *loop_exit_p = true;

    Utils::unzipFile(fullFilePath);
    size_t lastindex = fileName.find_last_of(".");
    string rawname = fileName.substr(0, lastindex);
    fullFilePath = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + rawname;
    std::string aggregatorDirPath = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.aggregatefolder");

    if (Utils::copyToDirectory(fullFilePath, aggregatorDirPath)) {
        instance_logger.error("Copying " + fullFilePath + " into " + aggregatorDirPath + " failed");
    }

    line = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
    if (line.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK) == 0) {
        instance_logger.info("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK);
        if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK)) {
            *loop_exit_p = true;
            return;
        }
        instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK);
    }
}

static void aggregate_centralstore_triangles_command(int connFd, bool *loop_exit_p) {
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    char data[DATA_BUFFER_SIZE];
    string graphId = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Graph ID: " + graphId);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }

    string partitionId = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Partition ID: " + partitionId);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }

    string partitionIdList = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Partition ID List : " + partitionIdList);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }

    string priority = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received priority: " + priority);

    int threadPriority = stoi(priority);

    if (threadPriority > Conts::DEFAULT_THREAD_PRIORITY) {
        threadPriorityMutex.lock();
        workerHighPriorityTaskCount++;
        highestPriority = threadPriority;
        threadPriorityMutex.unlock();
    }

    const std::string &aggregatedTriangles =
        aggregateCentralStoreTriangles(graphId, partitionId, partitionIdList, threadPriority);

    if (threadPriority > Conts::DEFAULT_THREAD_PRIORITY) {
        threadPriorityMutex.lock();
        workerHighPriorityTaskCount--;

        if (workerHighPriorityTaskCount == 0) {
            highestPriority = Conts::DEFAULT_THREAD_PRIORITY;
        }
        threadPriorityMutex.unlock();
    }

    std::vector<std::string> chunksVector;

    for (unsigned i = 0; i < aggregatedTriangles.length(); i += CHUNK_OFFSET) {
        std::string chunk = aggregatedTriangles.substr(i, CHUNK_OFFSET);
        if (i + CHUNK_OFFSET < aggregatedTriangles.length()) {
            chunk += "/SEND";
        } else {
            chunk += "/CMPT";
        }
        chunksVector.push_back(chunk);
    }

    for (int loopCount = 0; loopCount < chunksVector.size(); loopCount++) {
        if (loopCount == 0) {
            std::string chunk = chunksVector.at(loopCount);
            if (!Utils::send_str_wrapper(connFd, chunk)) {
                *loop_exit_p = true;
                break;
            }
        } else {
            string chunkStatus = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
            std::string chunk = chunksVector.at(loopCount);
            if (!Utils::send_str_wrapper(connFd, chunk)) {
                *loop_exit_p = true;
            }
        }
    }
    chunksVector.clear();
    chunksVector.shrink_to_fit();
}

static void aggregate_streaming_centralstore_triangles_command(
    int connFd, std::map<std::string, JasmineGraphIncrementalLocalStore *> &incrementalLocalStoreMap,
    bool *loop_exit_p) {
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    char data[DATA_BUFFER_SIZE];
    string graphId = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Graph ID: " + graphId);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }

    string partitionId = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Partition ID: " + partitionId);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }

    string partitionIdList = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Partition ID List : " + partitionIdList);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }

    string centralCountList = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received central count list : " + centralCountList);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }

    string priority = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received priority: " + priority);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }

    string mode = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received mode: " + mode);

    int threadPriority = stoi(priority);

    if (threadPriority > Conts::DEFAULT_THREAD_PRIORITY) {
        threadPriorityMutex.lock();
        workerHighPriorityTaskCount++;
        highestPriority = threadPriority;
        threadPriorityMutex.unlock();
    }

    std::string aggregatedTriangles = JasmineGraphInstanceService::aggregateStreamingCentralStoreTriangles(
        graphId, partitionId, partitionIdList, centralCountList, threadPriority, incrementalLocalStoreMap, mode);

    if (threadPriority > Conts::DEFAULT_THREAD_PRIORITY) {
        threadPriorityMutex.lock();
        workerHighPriorityTaskCount--;

        if (workerHighPriorityTaskCount == 0) {
            highestPriority = Conts::DEFAULT_THREAD_PRIORITY;
        }
        threadPriorityMutex.unlock();
    }

    if (aggregatedTriangles.empty()) {
        Utils::send_str_wrapper(connFd, "/CMPT");
        *loop_exit_p = true;
        return;
    }

    std::vector<std::string> chunksVector;

    for (unsigned i = 0; i < aggregatedTriangles.length(); i += CHUNK_OFFSET) {
        std::string chunk = aggregatedTriangles.substr(i, CHUNK_OFFSET);
        if (i + CHUNK_OFFSET < aggregatedTriangles.length()) {
            chunk += "/SEND";
        } else {
            chunk += "/CMPT";
        }
        chunksVector.push_back(chunk);
    }

    for (int loopCount = 0; loopCount < chunksVector.size(); loopCount++) {
        if (loopCount == 0) {
            std::string chunk = chunksVector.at(loopCount);
            if (!Utils::send_str_wrapper(connFd, chunk)) {
                *loop_exit_p = true;
                return;
            }
        } else {
            string chunkStatus = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
            std::string chunk = chunksVector.at(loopCount);
            if (!Utils::send_str_wrapper(connFd, chunk)) {
                *loop_exit_p = true;
            }
        }
    }
}

static void aggregate_composite_centralstore_triangles_command(int connFd, bool *loop_exit_p) {
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    char data[DATA_BUFFER_SIZE];
    string availableFiles = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Available Files: " + availableFiles);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }

    static const int suffix_len = 5;
    string response = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    string status = response.substr(response.size() - suffix_len);
    std::string compositeFileList = response.substr(0, response.size() - suffix_len);

    while (status == "/SEND") {
        if (!Utils::send_str_wrapper(connFd, status)) {
            *loop_exit_p = true;
            return;
        }

        response = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
        status = response.substr(response.size() - suffix_len);
        std::string fileList = response.substr(0, response.size() - suffix_len);
        compositeFileList = compositeFileList + fileList;
    }
    response = compositeFileList;

    instance_logger.info("Received Composite File List : " + compositeFileList);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }

    string priority = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received priority: " + priority);

    int threadPriority = stoi(priority);

    if (threadPriority > Conts::DEFAULT_THREAD_PRIORITY) {
        threadPriorityMutex.lock();
        workerHighPriorityTaskCount++;
        highestPriority = threadPriority;
        threadPriorityMutex.unlock();
    }

    const std::string &aggregatedTriangles =
        JasmineGraphInstanceService::aggregateCompositeCentralStoreTriangles(response, availableFiles, threadPriority);

    if (threadPriority > Conts::DEFAULT_THREAD_PRIORITY) {
        threadPriorityMutex.lock();
        workerHighPriorityTaskCount--;

        if (workerHighPriorityTaskCount == 0) {
            highestPriority = Conts::DEFAULT_THREAD_PRIORITY;
        }
        threadPriorityMutex.unlock();
    }

    std::vector<std::string> chunksVector;

    for (unsigned i = 0; i < aggregatedTriangles.length(); i += CHUNK_OFFSET) {
        std::string chunk = aggregatedTriangles.substr(i, CHUNK_OFFSET);
        if (i + CHUNK_OFFSET < aggregatedTriangles.length()) {
            chunk += "/SEND";
        } else {
            chunk += "/CMPT";
        }
        chunksVector.push_back(chunk);
    }

    for (int loopCount = 0; loopCount < chunksVector.size(); loopCount++) {
        if (loopCount == 0) {
            std::string chunk = chunksVector.at(loopCount);
            if (!Utils::send_str_wrapper(connFd, chunk)) {
                *loop_exit_p = true;
                break;
            }
        } else {
            string chunkStatus = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
            std::string chunk = chunksVector.at(loopCount);
            if (!Utils::send_str_wrapper(connFd, chunk)) {
                *loop_exit_p = true;
            }
        }
    }
    chunksVector.clear();
    chunksVector.shrink_to_fit();
}

static void initiate_files_command(int connFd, bool *loop_exit_p) {
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    char data[DATA_BUFFER_SIZE];
    string trainData = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
    std::vector<std::string> trainargs = Utils::split(trainData, ' ');

    string graphID;
    string partitionID = trainargs[trainargs.size() - 1];

    for (int i = 0; i < trainargs.size(); i++) {
        if (trainargs[i] == "--graph_id") {
            graphID = trainargs[i + 1];
            break;
        }
    }

    std::thread workerThreads[2];
    workerThreads[0] = std::thread(&JasmineGraphInstanceService::createPartitionFiles, graphID, partitionID, "local");
    workerThreads[1] =
        std::thread(&JasmineGraphInstanceService::createPartitionFiles, graphID, partitionID, "centralstore");

    for (int threadCount = 0; threadCount < 2; threadCount++) {
        workerThreads[threadCount].join();
    }
}

static void initiate_fed_predict_command(int connFd, bool *loop_exit_p) { initiate_files_command(connFd, loop_exit_p); }

static std::string initiate_command_common(int connFd, bool *loop_exit_p) {
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return "";
    }
    instance_logger.info("Sent: " + JasmineGraphInstanceProtocol::OK);

    char data[DATA_BUFFER_SIZE];
    string trainData = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
    instance_logger.info("Received options: " + trainData);
    return trainData;
}

static void initiate_server_command(int connFd, bool *loop_exit_p) {
    string trainData = initiate_command_common(connFd, loop_exit_p);
    if (*loop_exit_p) return;
    JasmineGraphInstanceService::initServer(trainData);
}

static void initiate_org_server_command(int connFd, bool *loop_exit_p) {
    string trainData = initiate_command_common(connFd, loop_exit_p);
    if (*loop_exit_p) return;
    JasmineGraphInstanceService::initOrgServer(trainData);
}

static void initiate_aggregator_command(int connFd, bool *loop_exit_p) {
    string trainData = initiate_command_common(connFd, loop_exit_p);
    if (*loop_exit_p) return;
    JasmineGraphInstanceService::initAgg(trainData);
}

static void initiate_client_command(int connFd, bool *loop_exit_p) {
    string trainData = initiate_command_common(connFd, loop_exit_p);
    if (*loop_exit_p) return;
    JasmineGraphInstanceService::initClient(trainData);
}

static void initiate_merge_files_command(int connFd, bool *loop_exit_p) {
    string trainData = initiate_command_common(connFd, loop_exit_p);
    if (*loop_exit_p) return;
    JasmineGraphInstanceService::mergeFiles(trainData);
}

static inline void start_stat_collection_command(int connFd, bool *collectValid_p, bool *loop_exit_p) {
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    *collectValid_p = true;
    JasmineGraphInstanceService::startCollectingLoadAverage();
}

static void request_collected_stats_command(int connFd, bool *collectValid_p, bool *loop_exit_p) {
    collectValid = false;
    std::string loadAverageString;

    for (auto loadVectorIterator = loadAverageVector.begin(); loadVectorIterator != loadAverageVector.end();
         ++loadVectorIterator) {
        std::string tempLoadAverage = *loadVectorIterator;
        loadAverageString = loadAverageString + "," + tempLoadAverage;
    }
    loadAverageVector.clear();

    loadAverageString = loadAverageString.substr(1, loadAverageString.length() - 1);

    std::vector<std::string> chunksVector;
    for (unsigned i = 0; i < loadAverageString.length(); i += CHUNK_OFFSET) {
        std::string chunk = loadAverageString.substr(i, CHUNK_OFFSET);
        if (i + CHUNK_OFFSET < loadAverageString.length()) {
            chunk += "/SEND";
        } else {
            chunk += "/CMPT";
        }
        chunksVector.push_back(chunk);
    }

    for (int loopCount = 0; loopCount < chunksVector.size(); loopCount++) {
        std::string chunk;
        if (loopCount > 0) {
            char data[DATA_BUFFER_SIZE];
            string chunkStatus = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
        }
        chunk = chunksVector.at(loopCount);
        if (!Utils::send_str_wrapper(connFd, chunk)) {
            *loop_exit_p = true;
            return;
        }
    }
}

static void initiate_train_command(int connFd, bool *loop_exit_p) {
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    char data[DATA_BUFFER_SIZE];
    string trainData = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
    std::vector<std::string> trainargs = Utils::split(trainData, ' ');

    string graphID;
    string partitionID = trainargs[trainargs.size() - 1];

    for (int i = 0; i < trainargs.size(); i++) {
        if (trainargs[i] == "--graph_id") {
            graphID = trainargs[i + 1];
            break;
        }
    }

    std::thread workerThreads[2];
    workerThreads[0] = std::thread(&JasmineGraphInstanceService::createPartitionFiles, graphID, partitionID, "local");
    workerThreads[1] =
        std::thread(&JasmineGraphInstanceService::createPartitionFiles, graphID, partitionID, "centralstore");

    workerThreads[0].join();
    instance_logger.info("WorkerThread 0 joined");

    workerThreads[1].join();
    instance_logger.info("WorkerThread 1 joined");

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::SEND_PARTITION_ITERATION)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::SEND_PARTITION_ITERATION);

    string partIteration = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::SEND_PARTITION_ITERATION)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::SEND_PARTITION_COUNT);

    string partCount = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
    instance_logger.info("Received partition iteration - " + partIteration);
    JasmineGraphInstanceService::collectExecutionData(stoi(partIteration), trainData, partCount);
    instance_logger.info("After calling collector ");
}

static void initiate_predict_command(int connFd, instanceservicesessionargs *sessionargs, bool *loop_exit_p) {
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    char data[DATA_BUFFER_SIZE];
    string graphID = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Graph ID: " + graphID);

    string vertexCount = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received vertexCount: " + vertexCount);

    string ownPartitions = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Own Partitions No: " + ownPartitions);

    /*Receive hosts' detail*/
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::SEND_HOSTS)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::SEND_HOSTS);

    char dataBuffer[INSTANCE_LONG_DATA_LENGTH + 1];
    string hostList = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
    instance_logger.info("Received Hosts List: " + hostList);

    // Put all hosts to a map
    std::map<std::string, JasmineGraphInstanceService::workerPartitions> graphPartitionedHosts;
    std::vector<std::string> hosts = Utils::split(hostList, '|');
    int count = 0;
    int totalPartitions = 0;
    for (std::vector<std::string>::iterator it = hosts.begin(); it != hosts.end(); ++it) {
        if (count != 0) {
            std::vector<std::string> hostDetail = Utils::split(*it, ',');
            std::string hostName;
            int port = -1;
            int dataport = -1;
            std::vector<string> partitionIDs;
            for (int index = 0; index < hostDetail.size(); index++) {
                const std::string j = hostDetail.at(index);
                switch (index) {
                    case 0:
                        hostName = j;
                        break;

                    case 1:
                        port = stoi(j);
                        break;

                    case 2:
                        dataport = stoi(j);
                        break;

                    default:
                        partitionIDs.push_back(j);
                        totalPartitions += 1;
                        break;
                }
            }
            graphPartitionedHosts.insert(
                pair<string, JasmineGraphInstanceService::workerPartitions>(hostName, {port, dataport, partitionIDs}));
        }
        count++;
    }
    /*Receive file*/
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::SEND_FILE_NAME)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME);

    string fileName = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
    instance_logger.info("Received File name: " + fileName);
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::SEND_FILE_LEN)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN);

    string size = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
    instance_logger.info("Received file size in bytes: " + size);
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::SEND_FILE_CONT)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_CONT);

    string fullFilePath =
        Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + fileName;
    int fileSize = stoi(size);
    string line;
    while (!Utils::fileExists(fullFilePath) || Utils::getFileSize(fullFilePath) < fileSize) {
        line = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
        if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) == 0) {
            if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::FILE_RECV_WAIT)) {
                *loop_exit_p = true;
                return;
            }
        }
    }

    line = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
    if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) == 0) {
        instance_logger.info("Received : " + JasmineGraphInstanceProtocol::FILE_RECV_CHK);
        if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::FILE_ACK)) {
            *loop_exit_p = true;
            return;
        }
        instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::FILE_ACK);
    }
    if (totalPartitions != 0) {
        JasmineGraphInstanceService::collectTrainedModels(sessionargs, graphID, graphPartitionedHosts, totalPartitions);
    }
    std::vector<std::string> predictargs;
    predictargs.push_back(graphID);
    predictargs.push_back(vertexCount);
    predictargs.push_back(fullFilePath);
    predictargs.push_back(Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.trainedmodelfolder"));
    predictargs.push_back(to_string(totalPartitions + stoi(ownPartitions)));
    std::vector<char *> predict_agrs_vector;
    std::transform(predictargs.begin(), predictargs.end(), std::back_inserter(predict_agrs_vector), converter);

    std::string path = "cd " + Utils::getJasmineGraphProperty("org.jasminegraph.graphsage") + " && ";
    std::string command = path + "python3 predict.py ";

    int argc = predictargs.size();
    for (int i = 0; i < argc; ++i) {
        command += predictargs[i];
        command += " ";
    }

    instance_logger.error("Temporarily disabled the execution of prediction.");
    // TODO(miyurud): Temporarily commenting the execution of the following line
    // due to missing predict.py file. Removal of graphsage folder resulted in this situation.
    // Need to find a different way of executing Predict
    // system(command.c_str());
    *loop_exit_p = true;
}

static void initiate_model_collection_command(int connFd, bool *loop_exit_p) {
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    char data[DATA_BUFFER_SIZE];
    string serverHostName = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received HostName: " + serverHostName);

    string serverHostPort = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Port: " + serverHostPort);

    string serverHostDataPort = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Data Port: " + serverHostDataPort);

    string graphID = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Graph ID: " + graphID);

    string partitionID = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Partition ID: " + partitionID);

    std::string fileName = graphID + "_model_" + partitionID;
    std::string filePath =
        Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.trainedmodelfolder") + "/" + fileName;

    // zip the folder
    Utils::compressDirectory(filePath);
    fileName = fileName + ".tar.gz";
    filePath = filePath + ".tar.gz";

    int fileSize = Utils::getFileSize(filePath);
    std::string fileLength = to_string(fileSize);
    // send file name
    string line = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
    if (line.compare(JasmineGraphInstanceProtocol::SEND_FILE_NAME) == 0) {
        if (!Utils::send_str_wrapper(connFd, fileName)) {
            *loop_exit_p = true;
            return;
        }
        instance_logger.info("Sent : File name " + fileName);

        line = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
        // send file length
        if (line.compare(JasmineGraphInstanceProtocol::SEND_FILE_LEN) == 0) {
            instance_logger.info("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN);
            if (!Utils::send_str_wrapper(connFd, fileLength)) {
                *loop_exit_p = true;
                return;
            }
            instance_logger.info("Sent : File length in bytes " + fileLength);

            line = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
            // send content
            if (line.compare(JasmineGraphInstanceProtocol::SEND_FILE_CONT) == 0) {
                instance_logger.info("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_CONT);
                instance_logger.info("Going to send file through service");
                fileName = "jasminegraph-local_trained_model_store/" + fileName;
                Utils::sendFileThroughService(serverHostName, stoi(serverHostDataPort), fileName, filePath);
            }
        }
    }
    int count = 0;
    while (true) {
        if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::FILE_RECV_CHK)) {
            *loop_exit_p = true;
            return;
        }
        instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::FILE_RECV_CHK);

        instance_logger.info("Checking if file is received");
        line = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
        if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_WAIT) == 0) {
            instance_logger.info("Received : " + JasmineGraphInstanceProtocol::FILE_RECV_WAIT);
            instance_logger.info("Checking file status : " + to_string(count));
            count++;
            sleep(1);
            continue;
        } else if (line.compare(JasmineGraphInstanceProtocol::FILE_ACK) == 0) {
            instance_logger.info("Received : " + JasmineGraphInstanceProtocol::FILE_ACK);
            instance_logger.info("File transfer completed");
            break;
        }
    }
    while (true) {
        if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK)) {
            *loop_exit_p = true;
            return;
        }
        instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK);

        line = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
        if (line.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT) == 0) {
            instance_logger.info("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT);
            sleep(1);
            continue;
        } else if (line.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK) == 0) {
            instance_logger.info("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK);
            instance_logger.info("Trained Model Batch upload completed");
            break;
        }
    }
    *loop_exit_p = true;
}

static void initiate_fragment_resolution_command(int connFd, bool *loop_exit_p) {
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    char data[DATA_BUFFER_SIZE];
    std::stringstream ss;
    while (true) {
        string response = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
        if (response.compare(JasmineGraphInstanceProtocol::FRAGMENT_RESOLUTION_DONE) == 0) {
            break;
        } else {
            instance_logger.info("Received ===>: " + response);
            ss << response;
        }

        if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::FRAGMENT_RESOLUTION_CHK)) {
            *loop_exit_p = true;
            return;
        }
        instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::FRAGMENT_RESOLUTION_CHK);
    }
    std::vector<std::string> partitions = Utils::split(ss.str(), ',');
    std::vector<std::string> graphIDs;
    for (std::vector<string>::iterator x = partitions.begin(); x != partitions.end(); ++x) {
        string graphID = x->substr(0, x->find_first_of("_"));
        graphIDs.push_back(graphID);
    }

    string dataFolder = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    std::vector<string> listOfFiles = Utils::getListOfFilesInDirectory(dataFolder);

    std::vector<std::string> graphIDsFromFileSystem;
    for (std::vector<string>::iterator x = listOfFiles.begin(); x != listOfFiles.end(); ++x) {
        string graphID = x->substr(0, x->find_first_of("_"));
        graphIDsFromFileSystem.push_back(graphID);
    }

    std::vector<string> notInGraphIDList;

    for (std::vector<std::string>::iterator it = graphIDsFromFileSystem.begin(); it != graphIDsFromFileSystem.end();
         it++) {
        bool found = false;
        for (std::vector<std::string>::iterator itRemoteID = graphIDs.begin(); itRemoteID != graphIDs.end();
             itRemoteID++) {
            if (it->compare(itRemoteID->c_str()) == 0) {
                found = true;
                break;
            }
        }
        if (!found) {
            notInGraphIDList.push_back(it->c_str());
        }
    }

    string notInItemsString = "";
    std::vector<int> notInItemsList;
    for (std::vector<string>::iterator it = notInGraphIDList.begin(); it != notInGraphIDList.end(); it++) {
        if (isdigit(it->c_str()[0])) {
            bool found = false;
            for (std::vector<int>::iterator it2 = notInItemsList.begin(); it2 != notInItemsList.end(); it2++) {
                if (atoi(it->c_str()) == *it2) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                notInItemsList.push_back(stoi(it->c_str()));
            }
        }
    }

    bool firstFlag = true;
    for (std::vector<int>::iterator it = notInItemsList.begin(); it != notInItemsList.end(); it++) {
        int x = *it;
        if (firstFlag) {
            notInItemsString = std::to_string(x);
            firstFlag = false;
        } else {
            notInItemsString = notInItemsString + "," + std::to_string(x);
        };
    }

    string graphIDList = notInItemsString;
    if (!Utils::send_str_wrapper(connFd, graphIDList)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + graphIDList);
}

static void check_file_accessible_command(int connFd, bool *loop_exit_p) {
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::SEND_FILE_TYPE)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_TYPE);
    string fullFilePath;
    string result = "false";

    char data[DATA_BUFFER_SIZE];
    string fileType = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    if (fileType.compare(JasmineGraphInstanceProtocol::FILE_TYPE_CENTRALSTORE_AGGREGATE) == 0) {
        if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
            *loop_exit_p = true;
            return;
        }
        instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

        string graphId = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
        instance_logger.info("Received Graph ID: " + graphId);

        if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
            *loop_exit_p = true;
            return;
        }

        string partitionId = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
        instance_logger.info("Received Partition ID: " + partitionId);

        string aggregateLocation = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.aggregatefolder");
        string fileName = graphId + "_centralstore_" + partitionId;
        fullFilePath = aggregateLocation + "/" + fileName;
    } else if (fileType.compare(JasmineGraphInstanceProtocol::FILE_TYPE_CENTRALSTORE_COMPOSITE) == 0) {
        if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
            *loop_exit_p = true;
            return;
        }
        instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

        string fileName = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
        instance_logger.info("Received File name: " + fileName);

        string aggregateLocation = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.aggregatefolder");
        fullFilePath = aggregateLocation + "/" + fileName;
    } else if (fileType.compare(JasmineGraphInstanceProtocol::FILE_TYPE_DATA) == 0) {
        if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
            *loop_exit_p = true;
            return;
        }
        instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);
        string fileName = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
        string dataDir = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
        fullFilePath = dataDir + "/" + fileName;
    }
    instance_logger.info("Checking existance of: " + fullFilePath);
    if (Utils::fileExists(fullFilePath)) {
        result = "true";
    }
    if (!Utils::send_str_wrapper(connFd, result)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + result);
}

static void graph_stream_start_command(int connFd, InstanceStreamHandler &instanceStreamHandler, bool *loop_exit_p) {
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::GRAPH_STREAM_START_ACK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.debug("Sent : " + JasmineGraphInstanceProtocol::GRAPH_STREAM_START_ACK);

    int content_length;
    instance_logger.debug("Waiting for edge content length");
    ssize_t return_status = recv(connFd, &content_length, sizeof(int), 0);
    if (return_status > 0) {
        content_length = ntohl(content_length);
        instance_logger.debug("Received content_length = " + std::to_string(content_length));
    } else {
        instance_logger.error("Error while reading content length");
        *loop_exit_p = true;
        return;
    }

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.debug("Acked for content length");

    instance_logger.debug("Waiting for edge data");
    std::string nodeString(content_length, 0);
    return_status = recv(connFd, &nodeString[0], content_length, 0);
    if (return_status > 0) {
        instance_logger.debug("Received edge data.");
    } else {
        instance_logger.error("Error while reading content length");
        *loop_exit_p = true;
        return;
    }
    instanceStreamHandler.handleRequest(nodeString);
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::GRAPH_STREAM_END_OF_EDGE)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.debug("Sent CRLF string to mark the end");
}

static void send_priority_command(int connFd, bool *loop_exit_p) {
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    char data[DATA_BUFFER_SIZE];
    string priority = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received Priority: " + priority);
    int retrievedPriority = stoi(priority);
    highestPriority = retrievedPriority;
}

static void push_partition_command(int connFd, bool *loop_exit_p) {
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    char data[DATA_BUFFER_SIZE];
    string hostDataPort = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received host:dataPort: " + hostDataPort);
    std::vector<std::string> hostPortList = Utils::split(hostDataPort, ':');
    std::string &host = hostPortList[0];
    int port = std::stoi(hostPortList[1]);
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::OK);

    string graphIDPartitionID = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
    instance_logger.info("Received graphID,partionID: " + graphIDPartitionID);
    std::vector<std::string> graphPartitionList = Utils::split(graphIDPartitionID, ',');
    int graphID = std::stoi(graphPartitionList[0]);
    int partitionID = std::stoi(graphPartitionList[1]);

    std::vector<std::string> fileList = {to_string(graphID) + "_" + to_string(partitionID),
                                         to_string(graphID) + "_centralstore_" + to_string(partitionID),
                                         to_string(graphID) + "_centralstore_dp_" + to_string(partitionID)};

    for (auto it = fileList.begin(); it != fileList.end(); it++) {
        std::string fileName = *it;
        std::string path =
            Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + fileName;

        if (!Utils::sendFileThroughService(host, port, fileName, path)) {
            instance_logger.error("Sending failed");
            if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::ERROR)) {
                *loop_exit_p = true;
            }
            return;
        }
    }
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent: " + JasmineGraphInstanceProtocol::OK);
}

string JasmineGraphInstanceService::aggregateStreamingCentralStoreTriangles(
    std::string graphId, std::string partitionId, std::string partitionIdString, std::string centralCountString,
    int threadPriority, std::map<std::string, JasmineGraphIncrementalLocalStore *> &incrementalLocalStores,
    std::string mode) {
    instance_logger.info("###INSTANCE### Started Aggregating Central Store Triangles");
    std::vector<JasmineGraphIncrementalLocalStore *> incrementalLocalStoreInstances;
    std::vector<std::string> centralCountList = Utils::split(centralCountString, ',');
    std::vector<std::string> partitionIdList = Utils::split(partitionIdString, ',');
    partitionIdList.push_back(partitionId);

    std::string triangles;
    if (mode == "0") {
        triangles = StreamingTriangles::countCentralStoreStreamingTriangles(graphId, partitionIdList);
    } else {
        triangles = StreamingTriangles::countDynamicCentralTriangles(
                graphId, partitionIdList, centralCountList);
    }

    instance_logger.info("###INSTANCE### Central Store Aggregation : Completed");

    return triangles;
}

static void query_start_command(int connFd, InstanceHandler &instanceHandler, std::map<std::string,
                                JasmineGraphIncrementalLocalStore *> &incrementalLocalStoreMap, bool *loop_exit_p) {
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::QUERY_START_ACK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Sent : " + JasmineGraphInstanceProtocol::QUERY_START_ACK);

    int content_length;
    instance_logger.info("Waiting for content length");
    ssize_t return_status = recv(connFd, &content_length, sizeof(int), 0);
    if (return_status > 0) {
        content_length = ntohl(content_length);
        instance_logger.info("Received content_length = " + std::to_string(content_length));
    } else {
        instance_logger.info("Error while reading content length");
        *loop_exit_p = true;
        return;
    }

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK)) {
        *loop_exit_p = true;
        return;
    }

    std::string graphId(content_length, 0);
    return_status = recv(connFd, &graphId[0], content_length, 0);
    if (return_status > 0) {
        instance_logger.info("Received graph id: "+graphId);
    } else {
        instance_logger.info("Error while reading content length");
        *loop_exit_p = true;
        return;
    }

    content_length = 0;
    instance_logger.info("Waiting for content length");
    return_status = recv(connFd, &content_length, sizeof(int), 0);
    if (return_status > 0) {
        content_length = ntohl(content_length);
        instance_logger.info("Received content_length = " + std::to_string(content_length));
    } else {
        instance_logger.info("Error while reading content length");
        *loop_exit_p = true;
        return;
    }

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK)) {
        *loop_exit_p = true;
        return;
    }
    std::string partition(content_length, 0);
    return_status = recv(connFd, &partition[0], content_length, 0);
    if (return_status > 0) {
        instance_logger.info("Received partition id: "+partition);
    } else {
        instance_logger.info("Error while reading content length");
        *loop_exit_p = true;
        return;
    }

    std::thread perfThread = std::thread(&PerformanceUtil::collectPerformanceStatistics);
    perfThread.detach();

    JasmineGraphIncrementalLocalStore * incrementalLocalStoreInstance;
    string graphIdentifier = "g"+graphId+"_p"+partition;
    if (incrementalLocalStoreMap.find(graphIdentifier) == incrementalLocalStoreMap.end()) {
        incrementalLocalStoreInstance =
                JasmineGraphInstanceService::loadStreamingStore(graphId, partition, incrementalLocalStoreMap, "app", false);
    } else {
        incrementalLocalStoreInstance = incrementalLocalStoreMap[graphIdentifier];
    }

    content_length = 0;
    instance_logger.info("Waiting for content length");
    return_status = recv(connFd, &content_length, sizeof(int), 0);
    if (return_status > 0) {
        content_length = ntohl(content_length);
        instance_logger.info("Received content_length = " + std::to_string(content_length));
    } else {
        instance_logger.info("Error while reading content length");
        *loop_exit_p = true;
        return;
    }

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK)) {
        *loop_exit_p = true;
        return;
    }

    std::string message(content_length, 0);
    return_status = recv(connFd, &message[0], content_length, 0);
    if (return_status > 0) {
        instance_logger.info("Received query.");
    } else {
        instance_logger.info("Error while reading content length");
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Received full query: " + message);
    instance_logger.info("connect partition id: " + partition + " with connection id: " + std::to_string(connFd));

    instanceHandler.handleRequest(connFd, loop_exit_p, incrementalLocalStoreInstance->gc, masterIP, message);
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::GRAPH_STREAM_END_OF_EDGE)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.debug("Sent CRLF string to mark the end");
}

static void semantic_beam_search(
    int connFd, InstanceHandler &instanceHandler,
    std::map<std::string, JasmineGraphIncrementalLocalStore *>
        &incrementalLocalStoreMap,
    bool *loop_exit_p) {
  if (!Utils::send_str_wrapper(
          connFd, JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK)) {
    *loop_exit_p = true;
    return;
  }
  instance_logger.info("Sent : " +
                       JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK);

  int content_length;
  instance_logger.info("Waiting for content length");
  ssize_t return_status = recv(connFd, &content_length, sizeof(int), 0);
  if (return_status > 0) {
    content_length = ntohl(content_length);
    instance_logger.info("Received content_length = " +
                         std::to_string(content_length));
  } else {
    instance_logger.info("Error while reading content length");
    *loop_exit_p = true;
    return;
  }

  if (!Utils::send_str_wrapper(
          connFd, JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK)) {
    *loop_exit_p = true;
    return;
  }

  std::string graphId(content_length, 0);
  return_status = recv(connFd, &graphId[0], content_length, 0);
  if (return_status > 0) {
    instance_logger.info("Received graph id: " + graphId);
  } else {
    instance_logger.info("Error while reading content length");
    *loop_exit_p = true;
    return;
  }

  content_length = 0;
  instance_logger.info("Waiting for content length");
  return_status = recv(connFd, &content_length, sizeof(int), 0);
  if (return_status > 0) {
    content_length = ntohl(content_length);
    instance_logger.info("Received content_length = " +
                         std::to_string(content_length));
  } else {
    instance_logger.info("Error while reading content length");
    *loop_exit_p = true;
    return;
  }

  if (!Utils::send_str_wrapper(
          connFd, JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK)) {
    *loop_exit_p = true;
    return;
  }
  std::string partition(content_length, 0);
  return_status = recv(connFd, &partition[0], content_length, 0);
  if (return_status > 0) {
    instance_logger.info("Received partition id: " + partition);
  } else {
    instance_logger.info("Error while reading content length");
    *loop_exit_p = true;
    return;
  }

  std::thread perfThread =
      std::thread(&PerformanceUtil::collectPerformanceStatistics);
  perfThread.detach();

  JasmineGraphIncrementalLocalStore *incrementalLocalStoreInstance;
  string graphIdentifier = "g" + graphId + "_p" + partition;
  if (incrementalLocalStoreMap.find(graphIdentifier) ==
      incrementalLocalStoreMap.end()) {
    incrementalLocalStoreInstance =
        JasmineGraphInstanceService::loadStreamingStore(
            graphId, partition, incrementalLocalStoreMap, "app", true);
  } else {
    incrementalLocalStoreInstance = incrementalLocalStoreMap[graphIdentifier];
  }

  content_length = 0;
  instance_logger.info("Waiting for content length");
  return_status = recv(connFd, &content_length, sizeof(int), 0);
  if (return_status > 0) {
    content_length = ntohl(content_length);
    instance_logger.info("Received content_length = " +
                         std::to_string(content_length));
  } else {
    instance_logger.info("Error while reading content length");
    *loop_exit_p = true;
    return;
  }

  if (!Utils::send_str_wrapper(
          connFd, JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK)) {
    *loop_exit_p = true;
    return;
  }

  std::string message(content_length, 0);
  return_status = recv(connFd, &message[0], content_length, 0);
  if (return_status > 0) {
    instance_logger.info("Received query.");
    if (!Utils::send_str_wrapper(
            connFd, JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK)) {
      *loop_exit_p = true;
      return;
    }
  } else {
    instance_logger.info("Error while reading content length");
    *loop_exit_p = true;
    return;
  }
  instance_logger.info("Received full query: " + message);
  instance_logger.info("connect partition id: " + partition +
                       " with connection id: " + std::to_string(connFd));
  char data[DATA_BUFFER_SIZE];

  content_length = 0;
  instance_logger.info("Waiting for content length");
  return_status = recv(connFd, &content_length, sizeof(int), 0);
  if (return_status > 0) {
    content_length = ntohl(content_length);
    instance_logger.info("Received content_length = " +
                         std::to_string(content_length));
  } else {
    instance_logger.info("Error while reading content length");
    *loop_exit_p = true;
    return;
  }

  if (!Utils::send_str_wrapper(
          connFd, JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK)) {
    *loop_exit_p = true;
    return;
  }
  // read workerIP:port in comma separated format
  string workersIP =
      Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
  instance_logger.info("Received Worker IP: " + workersIP);
  if (!Utils::send_str_wrapper(connFd, "HI")) {
    *loop_exit_p = true;
    return;
  }
  std::vector<string> workerSockets;
  stringstream wl(workersIP);
  string intermediate;
  while (getline(wl, intermediate, ',')) {
    workerSockets.push_back(intermediate);
  }
  std::vector<JasmineGraphServer::worker> workers;
  for (const auto &workerSocket : workerSockets) {
    JasmineGraphServer::worker worker;
    size_t pos = workerSocket.find(":");
    if (pos != string::npos) {
      worker.hostname = workerSocket.substr(0, pos);
      worker.port = stoi(workerSocket.substr(pos + 1));
      worker.dataPort =
          worker.port +
          1;  // Assuming data port is one more than the worker port
    } else {
      instance_logger.error("Invalid worker socket format: " + workerSocket);
      *loop_exit_p = true;
      return;
    }
    workers.push_back(worker);
  }

  // instanceHandler.handleRequest(connFd, loop_exit_p,
  // incrementalLocalStoreInstance->gc, masterIP, message);
  unsigned long maxLabel = std::stol(Utils::getJasmineGraphProperty(
      "org.jasminegraph.nativestore.max.label.size"));
  GraphConfig gc{maxLabel, static_cast<unsigned int>(std::stoi(graphId)),
                 static_cast<unsigned int>(std::stoi(partition)), "app"};

  std::string instanceDataFolderLocation = Utils::getJasmineGraphProperty(
      "org.jasminegraph.server.instance.datafolder");
  std::string graphPrefix = instanceDataFolderLocation + "/g" + graphId;
  string dbPrefix = graphPrefix + "_p" + partition;
  FaissIndex *faissStore =
      FaissIndex::getInstance(std::stoi(Utils::getJasmineGraphProperty(
                                  "org.jasminegraph.vectorstore.dimension")),
                              dbPrefix + "_faiss.index");

  TextEmbedder *textEmbedder = new TextEmbedder(
      Utils::getJasmineGraphProperty("org.jasminegraph.vectorstore.embedding."
                                     "ollama.endpoint"),  // Ollama endpoint
      Utils::getJasmineGraphProperty(
          "org.jasminegraph.vectorstore.embedding.model")  // model name
  );

  // check workerlist
  for (const auto &worker : workers) {
    instance_logger.info("Worker Hostname: " + worker.hostname +
                         ", Port: " + std::to_string(worker.port) +
                         ", Data Port: " + std::to_string(worker.dataPort));
  }
  SemanticBeamSearch *semanticBeamSearch = new SemanticBeamSearch(
      faissStore, textEmbedder, textEmbedder->embed(message), 7, gc, workers);
  semanticBeamSearch->getSeedNodes();
  SharedBuffer shared(50);
  semanticBeamSearch->semanticMultiHopBeamSearch(shared, 3, 10);
  auto startTime = std::chrono::high_resolution_clock::now();
  int time = 0;

  while (true) {
    string raw = shared.get();
    instance_logger.debug("raw: " + raw);
    if (raw == "-1") {
      instanceHandler.dataPublishToMaster(connFd, loop_exit_p, raw);
      instance_logger.info("Total time taken for query execution: " +
                           std::to_string(time) + " ms");
      // result.join();
      break;
    }
    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        endTime - startTime);
    time += duration.count();
    instanceHandler.dataPublishToMaster(connFd, loop_exit_p, raw);
    startTime = std::chrono::high_resolution_clock::now();
  }
  // if (!Utils::send_str_wrapper(connFd,fF
  // JasmineGrapfhInstanceProtocol::GRAPH_STREAM_END_OF_EDGE)) {
  //     *loop_exit_p = true;
  //     return;
  // }
  instance_logger.debug("Sent CRLF string to mark the end");
  *loop_exit_p = true;

  close(connFd);
}

static void semantic_search_expand_node_remote_batch(
    int connFd,
    std::map<std::string, JasmineGraphIncrementalLocalStore *>
        &incrementalLocalStoreMap,
    bool *loop_exit_p) {
  char data[DATA_BUFFER_SIZE];
  instance_logger.info("Handling EXPAND_NODE_BATCH request");

  // 1. ACK init
  Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK);

  // 2. Expect graphID
  std::string graphID =
      Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
  Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::OK);

  // 3. Receive request JSON (nodeIds + fromPartition)
  int content_length;
  recv(connFd, &content_length, sizeof(int), 0);
  content_length = ntohl(content_length);

  std::string requestStr(content_length, 0);
  size_t received = 0;
  while (received < content_length) {
    ssize_t ret =
        recv(connFd, &requestStr[received], content_length - received, 0);
    if (ret <= 0) {
      instance_logger.error("Error receiving request string");
      break;
    }
    received += ret;
  }
  instance_logger.info("Received request string of length: " +
                       std::to_string(received));
  instance_logger.info("Received request string of length: " +
                       std::to_string(requestStr.size()));
  instance_logger.info("Received request: " + requestStr);

  json request = json::parse(requestStr);
  std::vector<json> currentPaths =
      request["currentPaths"].get<std::vector<json>>();
  std::string fromPartition = request["fromPartition"];
  unsigned int partitionId = request["toPartition"].get<unsigned int>();
  vector<float> queryEmbedding =
      request["queryEmbedding"].get<std::vector<float>>();

  // recieve embedding size
  // recv( connFd, &content_length, sizeof(int), 0);
  // content_length = ntohl(content_length);
  // std::vector<float> queryEmbedding(content_length);
  //
  // recv(connFd, queryEmbedding.data(), queryEmbedding.size() * sizeof(float),
  // 0);
  // // request[""]
  // instance_logger.info("Received query embedding of size: " +
  // std::to_string(queryEmbedding.size()));
  // // log query emb
  // instance_logger.debug("Query embedding: ");
  // for (const auto &val : queryEmbedding)
  // {
  //     instance_logger.debug(std::to_string(val));
  // }

  // log emb

  // instance_logger.info("Expanding " + std::to_string(nodeIds.size()) +
  //                      " nodes from partition " + fromPartition);

  // 4. Expand nodes using local store
  json response;
  response["expandedPaths"] = json::array();
  std::vector<ScoredPath> expandedPaths;

  unsigned long maxLabel = std::stol(Utils::getJasmineGraphProperty(
      "org.jasminegraph.nativestore.max.label.size"));
  GraphConfig gc{maxLabel, static_cast<unsigned int>(std::stoi(graphID)),
                 static_cast<unsigned int>(partitionId), "app"};

  NodeManager nodeManager(gc);
  FaissIndex *faissStore =
      FaissIndex::getInstance(std::stoi(Utils::getJasmineGraphProperty(
                                  "org.jasminegraph.vectorstore.dimension")),
                              nodeManager.getDbPrefix() + "_faiss.index");

  for (const auto currentPath : currentPaths) {
    float score = currentPath["score"];
    json newPath;
    json lastNodeJson = currentPath["pathObj"]["pathNodes"].back();

    string lastNodeId = lastNodeJson["id"].get<std::string>();
    instance_logger.debug("Last node ID: " + lastNodeId);
    NodeBlock *nodeBlock = nodeManager.get(lastNodeId);
    if (!nodeBlock) {
      continue;
    }
    std::list<std::pair<NodeBlock *, RelationBlock *>> neighbors =
        nodeBlock->getAllEdgeNodes();
    json expanded;
    expanded["nodeId"] = lastNodeId;
    json neighborsJson = json::array();
    vector<float> emb_ = faissStore->getEmbeddingById(lastNodeId);
    instance_logger.debug(" emb_" + std::to_string(emb_.at(0)));

    for (const auto &neighbor : neighbors) {
      json newPath = currentPath["pathObj"];
      instance_logger.debug("newPath" + newPath.dump());

      json nodeData;
      auto nodeProps = neighbor.first->getAllProperties();
      nodeData["partitionID"] =
          std::string(neighbor.first->getMetaPropertyHead()->value);
      for (auto &[k, v] : nodeProps) nodeData[k] = v;
      // nodeData["id"] = std::to_string(neighbor.first->nodeId);

      vector<float> emb_ =
          faissStore->getEmbeddingById(std::to_string(neighbor.first->nodeId));
      instance_logger.debug("Exapnd Scoring node ID: " +
                            std::to_string(neighbor.first->nodeId));
      // check queryembedding and emb_
      if (emb_.empty()) {
        instance_logger.error("No embedding found for node ID: " +
                              std::to_string(neighbor.first->nodeId));
        continue;
      }
      if (queryEmbedding.empty()) {
        instance_logger.error("No query embedding provided.");
        continue;
      }
      newPath["pathNodes"].push_back(nodeData);

      json relData;
      auto relProps = neighbor.second->getAllProperties();
      for (auto &[k, v] : relProps) relData[k] = v;
      newPath["pathRels"].push_back(relData);
      json expandedPath;
      expandedPath["pathObj"] = newPath;
      expandedPath["score"] =
          score + Utils::cosineSimilarity(queryEmbedding, emb_);
      response["expandedPaths"].push_back(expandedPath);
      instance_logger.info(
          "Expanded node ID: " + std::to_string(neighbor.first->nodeId) +
          " with score: " + std::to_string(score));
      instance_logger.info(
          "Added new path with " + std::to_string(newPath["pathNodes"].size()) +
          " nodes and " + std::to_string(newPath["pathRels"].size()) +
          " relations.");
    }
  }

  // if (!Utils::sendExpectResponse(connFd, data, INSTANCE_DATA_LENGTH,
  // JasmineGraphInstanceProtocol::QUERY_DATA_START,
  // JasmineGraphInstanceProtocol::OK)) {
  //     instance_logger.error("Failed to send QUERY_DATA_START");
  //
  // }

  std::string responseStr = response.dump();
  instance_logger.info("Expanded Response: " + responseStr);
  int respLen = htonl(responseStr.size());
  send(connFd, &respLen, sizeof(int), 0);
  send(connFd, responseStr.c_str(), responseStr.size(), 0);
  *loop_exit_p = true;
  close(connFd);
  // 6. Close connection
  Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::CLOSE);
}



static void sub_query_start_command(int connFd, InstanceHandler &instanceHandler, std::map<std::string,
        JasmineGraphIncrementalLocalStore *> &incrementalLocalStoreMap, bool *loop_exit_p) {
    instance_logger.info("Received sub query start command");
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::SUB_QUERY_START_ACK)) {
        *loop_exit_p = true;
        return;
    }

    int content_length;
    instance_logger.info("Waiting for content length");
    ssize_t return_status = recv(connFd, &content_length, sizeof(int), 0);
    if (return_status > 0) {
        content_length = ntohl(content_length);
        instance_logger.info("Received content_length = " + std::to_string(content_length));
    } else {
        instance_logger.info("Error while reading content length");
        *loop_exit_p = true;
        return;
    }

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK)) {
        *loop_exit_p = true;
        return;
    }

    std::string graphId(content_length, 0);
    return_status = recv(connFd, &graphId[0], content_length, 0);
    if (return_status > 0) {
        instance_logger.info("Received graph id: " + graphId);
    } else {
        instance_logger.info("Error while reading content length");
        *loop_exit_p = true;
        return;
    }

    content_length = 0;
    instance_logger.info("Waiting for content length");
    return_status = recv(connFd, &content_length, sizeof(int), 0);
    if (return_status > 0) {
        content_length = ntohl(content_length);
        instance_logger.info("Received content_length = " + std::to_string(content_length));
    } else {
        instance_logger.info("Error while reading content length");
        *loop_exit_p = true;
        return;
    }

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK)) {
        *loop_exit_p = true;
        return;
    }
    std::string partition(content_length, 0);
    return_status = recv(connFd, &partition[0], content_length, 0);
    if (return_status > 0) {
        instance_logger.info("Received partition id: " + partition);
    } else {
        instance_logger.info("Error while reading content length");
        *loop_exit_p = true;
        return;
    }

    JasmineGraphIncrementalLocalStore * incrementalLocalStoreInstance;
    string graphIdentifier = "g"+graphId+"_p" + partition;
    if (incrementalLocalStoreMap.find(graphIdentifier) == incrementalLocalStoreMap.end()) {
        incrementalLocalStoreInstance =
                JasmineGraphInstanceService::loadStreamingStore(graphId, partition, incrementalLocalStoreMap, "app", false);
    } else {
        incrementalLocalStoreInstance = incrementalLocalStoreMap[graphIdentifier];
    }

    content_length = 0;
    instance_logger.info("Waiting for content length");
    return_status = recv(connFd, &content_length, sizeof(int), 0);
    if (return_status > 0) {
        content_length = ntohl(content_length);
        instance_logger.info("Received content_length = " + std::to_string(content_length));
    } else {
        instance_logger.info("Error while reading content length");
        *loop_exit_p = true;
        return;
    }

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK)) {
        *loop_exit_p = true;
        return;
    }

    std::string message(content_length, 0);
    return_status = recv(connFd, &message[0], content_length, 0);
    if (return_status > 0) {
        instance_logger.info("Received sub query.");
    } else {
        instance_logger.info("Error while reading content length");
        *loop_exit_p = true;
        return;
    }
    instance_logger.info("Received full sub query: " + message);
    instanceHandler.handleRequest(connFd, loop_exit_p, incrementalLocalStoreInstance->gc, masterIP, message);
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::GRAPH_STREAM_END_OF_EDGE)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.debug("Sent CRLF string to mark the end");
}
static void hdfs_start_stream_command(int connFd, bool *loop_exit_p, bool isLocalStream,
                                      InstanceStreamHandler &instanceStreamHandler) {
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::HDFS_STREAM_START_ACK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.debug("Sent : " + JasmineGraphInstanceProtocol::HDFS_STREAM_START_ACK);

    char data[DATA_BUFFER_SIZE];
    string isEmbedGraph = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
    instance_logger.debug("Received isEmbedGraph : " + isEmbedGraph);


    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::HDFS_STREAM_IS_EMBED_ACK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.debug("Acked for isEmbedGraph ");

    string fileName = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
    instance_logger.debug("Received File name: " + fileName);


    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::HDFS_STREAM_FILE_NAME_ACK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.debug("Acked for file name");

    string size = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
    instance_logger.debug("Received file size in bytes: " + size);

    int fileSize = stoi(size);
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::HDFS_STREAM_FILE_SIZE_ACK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.debug("Acked for file size");

    string line;
    string fullFilePath =
            Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + fileName;

    while (!Utils::fileExists(fullFilePath)) {
        instance_logger.error("Instance data file " + fullFilePath + " does not exist");
        sleep(1);
    }
    instance_logger.info("Instance data file " + fullFilePath + " exist");

    while (Utils::getFileSize(fullFilePath) < fileSize) {
        line = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
        if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) != 0) {
            instance_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::FILE_RECV_CHK +
                                  " ; Received: " + line);
            close(connFd);
            return;
        }
        if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::FILE_RECV_WAIT)) {
            *loop_exit_p = true;
            return;
        }
    }

    line = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
    if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) != 0) {
        instance_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::FILE_RECV_CHK +
                              " ; Received: " + line);
        close(connFd);
        return;
    }
    instance_logger.debug("Received : " + line);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::FILE_ACK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.debug("Sent : " + JasmineGraphInstanceProtocol::FILE_ACK);

    while (!Utils::fileExists(fullFilePath)) {
        line = Utils::read_str_trim_wrapper(connFd, data, INSTANCE_DATA_LENGTH);
        if (line.compare(JasmineGraphInstanceProtocol::HDFS_STREAM_END_CHK) != 0) {
            instance_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::HDFS_STREAM_END_CHK +
                                  " ; Received: " + line);
            close(connFd);
            return;
        }
        instance_logger.debug("Received : " + line);
        if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::HDFS_STREAM_END_WAIT)) {
            *loop_exit_p = true;
            return;
        }
        instance_logger.debug("Sent : " + JasmineGraphInstanceProtocol::HDFS_STREAM_END_WAIT);
    }

    line = Utils::read_str_wrapper(connFd, data, INSTANCE_DATA_LENGTH, false);
    if (line.compare(JasmineGraphInstanceProtocol::HDFS_STREAM_END_CHK) != 0) {
        instance_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::HDFS_STREAM_END_CHK +
                              " ; Received: " + line);
        close(connFd);
        return;
    }
    instance_logger.debug("Received : " + line);
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::HDFS_STREAM_END_ACK)) {
        *loop_exit_p = true;
        return;
    }
    instance_logger.debug("Sent : " + JasmineGraphInstanceProtocol::HDFS_STREAM_END_ACK);

processFile(fileName, isLocalStream, instanceStreamHandler, isEmbedGraph == "true");
    // delete file chunk after adding to the store
    Utils::deleteFile(fullFilePath);
}

static void processFile(string fileName, bool isLocal,
                                              InstanceStreamHandler &handler , bool isEmbedGraph) {
    std::string fileDirectory = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/";
    std::string filePath = fileDirectory + fileName;

    std::regex fileNamePattern;
    if (isLocal) {
        fileNamePattern = std::regex(R"((\d+)_(\d+)_localstore_(\d+))");
    } else  {
        fileNamePattern = std::regex(R"((\d+)_(\d+)_centralstore_(\d+))");
    }

    std::smatch match;
    int graphId, partitionIndex;
    if (std::regex_match(fileName, match, fileNamePattern)) {
        graphId = std::stoi(match[1].str());         // Extract graphId
        partitionIndex = std::stoi(match[2].str());  // Extract partitionIndex

        instance_logger.debug("Extracted graphId: " + std::to_string(graphId) +
                             ", partitionIndex: " + std::to_string(partitionIndex));
    } else {
        instance_logger.error("File name format is incorrect: " + fileName);
        return;
    }

    std::ifstream file(filePath);
    if (!file.is_open()) {
        instance_logger.error("Error opening file: " + filePath);
        return;
    }

    instance_logger.debug("Processing file: " + filePath);
    instance_logger.debug("isEmbed enabled: " + to_string(isEmbedGraph));


    // check file contents
    if (file.peek() == std::ifstream::traits_type::eof()) {
        instance_logger.error("File is empty: " + filePath);
        file.close();
        return;
    }

    std::string line;
    while (std::getline(file, line)) {
        instance_logger.debug("currentLine " + line);
        if (isLocal) {
            handler.handleLocalEdge(
                    line,
                    std::to_string(graphId),
                    std::to_string(partitionIndex),
                    std::to_string(graphId) + "_" + std::to_string(partitionIndex) , isEmbedGraph);
        } else {
            handler.handleCentralEdge(
                    line,
                    std::to_string(graphId),
                    std::to_string(partitionIndex),
                    std::to_string(graphId) + "_" + std::to_string(partitionIndex), isEmbedGraph);
        }
    }

    if (isEmbedGraph)
    {

        JasmineGraphIncrementalLocalStore* localStore = handler.incrementalLocalStoreMap[std::to_string(graphId) + "_" + std::to_string(partitionIndex)];

        localStore->getAndStoreEmbeddings();
    }
    // std::string instanceDataFolderLocation =
    //   Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    // std::string graphPrefix = instanceDataFolderLocation + "/g" + std::to_string(graphId);
    // string dbPrefix = graphPrefix + "_p" + std::to_string(partitionIndex);
    // FaissIndex::getInstance(std::stoi(Utils::getJasmineGraphProperty("org.jasminegraph.vectorstore.dimension")),
    //
    //
    //     dbPrefix+ "_faiss.index")->save(dbPrefix+ "_faiss.index");

    file.close();

    instance_logger.info("Finished processing file: " + filePath);
}


