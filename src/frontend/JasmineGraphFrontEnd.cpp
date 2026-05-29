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

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstdlib>
#include <cstdio>
#include <chrono>
#include <ctime>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <future>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <nlohmann/json.hpp>
#include <sstream>
#include <set>
#include <thread>
#include <unordered_map>
#include <sys/stat.h>

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
#include "../query/algorithms/triangles/HistoryTriangles.h"
#include "../query/algorithms/pagerank/HistoryPageRank.h"
#include "../query/algorithms/bfs/HistoryBFS.h"
#include "../temporalstore/TemporalQueryExecutor.h"
#include "../temporalstore/TemporalStorePersistence.h"
#include "../util/Conts.h"
#include "../util/hdfs/HDFSConnector.h"
#include "../util/hdfs/HDFSStreamHandler.h"
#include "../util/kafka/KafkaCC.h"
#include "../util/kafka/StreamHandler.h"
#include "../util/kafka/StreamRegistry.h"
#include "../util/logger/Logger.h"
#include "/home/ubuntu/software/antlr/CypherLexer.h"
#include "/home/ubuntu/software/antlr/CypherParser.h"
#include "JasmineGraphFrontEndProtocol.h"
#include "KafkaTopicUtils.h"
#include "antlr4-runtime.h"
#include "core/CoreConstants.h"
#include "core/common/JasmineGraphFrontendCommon.h"
#include "core/scheduler/JobScheduler.h"

#define MAX_PENDING_CONNECTIONS 10
#define DATA_BUFFER_SIZE (FRONTEND_DATA_LENGTH + 1)

using json = nlohmann::json;
using namespace std;
using namespace std::chrono;

Logger frontend_logger;

static void parseHdfsConfigFile(const std::string &filePath, std::string &hdfsServerIp, std::string &hdfsPort) {
    std::vector<std::string> vec = Utils::getFileContent(filePath);
    for (const auto &item : vec) {
        if (item.empty() || item.rfind("#", 0) == 0) {
            continue;
        }

        std::vector<std::string> parts = Utils::split(item, '=');
        if (parts.size() != 2) {
            frontend_logger.error("Invalid line in configuration file: " + item);
            continue;
        }

        if (parts[0].compare("hdfs.host") == 0) {
            hdfsServerIp = parts[1];
        } else if (parts[0].compare("hdfs.port") == 0) {
            hdfsPort = parts[1];
        }
    }
}

std::atomic<int> highPriorityTaskCount;
static int connFd;
static std::atomic<int> currentFESession;
static bool canCalibrate = true;
std::set<ProcessInfo> processData;
std::string stream_topic_name;
bool JasmineGraphFrontEnd::strian_exit;
std::map<int, std::thread::id> activeStreamThreads;           // map graphID → thread ID
std::map<int, std::shared_ptr<std::atomic<bool>>> stopFlags;  // map graphID → stop flag

std::mutex threadMapMutex;
static void writeSocketResultOrEmpty(int connectionFd, const std::string &result, bool *loop_exit_p);
std::mutex historyResultFileMutex;
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
static void temporal_query_command(int connFd, SQLiteDBInterface *, bool *);
static void temporal_snapshot_command(int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p);
static void temporal_range_command(int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p);
static void add_stream_kafka_command(int connFd, std::string &kafka_server_IP, cppkafka::Configuration &configs,
                                     KafkaConnector *&kstream, thread &input_stream_handler_thread,
                                     vector<DataPublisher *> &workerClients, int numberOfPartitions,
                                     SQLiteDBInterface *sqlite, bool *loop_exit_p,
                                     bool isCsvMode = false);
static void addStreamHDFSCommand(std::string masterIP, int connFd, std::string &hdfsServerIp,
                                 std::thread &inputStreamHandlerThread, int numberOfPartitions,
                                 SQLiteDBInterface *sqlite, bool *loop_exit_p);
static void send_graph_hdfs_command(const std::string &masterIP, int connectionFd, SQLiteDBInterface *sqlite,
                                    bool *loop_exit_p);
static void stop_stream_kafka_command(int connFd, const std::string &topicName, bool *loop_exit_p);
static void kafka_topics_command(int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p);
static void process_dataset_command(int connFd, bool *loop_exit_p);
static void triangles_command(std::string masterIP, int connFd, SQLiteDBInterface *sqlite,
                              PerformanceSQLiteDBInterface *perfSqlite, JobScheduler *jobScheduler, bool *loop_exit_p);
static void streaming_triangles_command(std::string masterIP, int connFd, JobScheduler *jobScheduler, bool *loop_exit_p,
                                        int numberOfPartitions, bool *strian_exit);
static void history_triangle_command(int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p,
                                     const std::string& masterIP);
static void history_triangle_timestamp_command(int connFd, SQLiteDBInterface *, bool *,
                                               const std::string& masterIP);
static void history_pagerank_command(int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p,
                                     const std::string& masterIP);
static void history_pagerank_timestamp_command(int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p,
                                               const std::string& masterIP);
static void history_bfs_command(int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p);
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
static std::string read_socket_value(int connFd, size_t length);
static std::string read_frontend_socket_value(int connFd);
static std::string format_local_timestamp(std::time_t timePoint);
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

static std::string getTemporalSnapshotDir() {
    std::string configuredPath =
        Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.temporalsnapshotfolder");
    if (!configuredPath.empty()) {
        struct stat configuredStats;
        if (stat(configuredPath.c_str(), &configuredStats) == 0 && S_ISDIR(configuredStats.st_mode)) {
            return configuredPath;
        }

        // Configuration drift can happen when master/worker containers are built from
        // slightly different property files. Fallback to datafolder-based path.
        frontend_logger.warn("Configured temporal snapshot directory not accessible: " + configuredPath +
                             ". Falling back to datafolder/temporal_snapshots");
    }

    return Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") +
           "/temporal_snapshots";
}

struct TemporalSnapshotSummary {
    uint64_t totalEdges;
    uint64_t timestamp;
};

using SnapshotPartitionTotals = std::map<uint32_t, std::map<uint32_t, uint64_t>>;

static bool tryParsePartitionIdFromSnapmetaFileName(const std::string& fileName, int graphId,
                                                    uint32_t& partitionId) {
    std::string prefix = "graph" + std::to_string(graphId) + "_part";
    std::string suffix = "_snapmeta.bin";

    if (fileName.rfind(prefix, 0) != 0) {
        return false;
    }
    if (fileName.size() <= prefix.size() + suffix.size()) {
        return false;
    }
    if (fileName.compare(fileName.size() - suffix.size(), suffix.size(), suffix) != 0) {
        return false;
    }

    std::string partitionText =
        fileName.substr(prefix.size(), fileName.size() - prefix.size() - suffix.size());
    if (partitionText.empty()) {
        return false;
    }
    if (!std::all_of(partitionText.begin(), partitionText.end(),
                     [](unsigned char ch) { return std::isdigit(ch); })) {
        return false;
    }

    partitionId = static_cast<uint32_t>(std::stoul(partitionText));
    return true;
}

static void mergeSnapshotMetaRecord(const TemporalStorePersistence::SnapshotMetaRecord& rec,
                                    uint32_t partitionId,
                                    std::map<uint32_t, TemporalSnapshotSummary>& snapMap,
                                    SnapshotPartitionTotals& partitionTotals) {
    auto& summary = snapMap[rec.snapshotId];
    if (summary.timestamp < rec.timestamp) {
        summary.timestamp = rec.timestamp;
    }

    auto& perPartition = partitionTotals[rec.snapshotId];
    auto existing = perPartition.find(partitionId);
    if (existing == perPartition.end()) {
        perPartition[partitionId] = rec.totalEdges;
        summary.totalEdges += rec.totalEdges;
        return;
    }

    // If duplicate metadata appears for the same partition (e.g. mirrored files on
    // multiple worker hosts), keep the highest total and update aggregate sum.
    if (rec.totalEdges > existing->second) {
        summary.totalEdges += (rec.totalEdges - existing->second);
        existing->second = rec.totalEdges;
    }
}

static std::string shellQuote(const std::string& value) {
    std::string quoted = "'";
    for (char ch : value) {
        if (ch == '\'') {
            quoted += "'\\''";
        } else {
            quoted += ch;
        }
    }
    quoted += "'";
    return quoted;
}

static std::string getHistoryResultLogFilePath() {
    return Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") +
           "/history_query_results.log";
}

static void appendHistoryQueryResultToFile(const std::string& queryName,
                                           int graphId,
                                           const std::string& requestDetails,
                                           const std::string& responseText) {
    std::lock_guard<std::mutex> lock(historyResultFileMutex);

    std::string resultFilePath = getHistoryResultLogFilePath();
    std::ofstream out(resultFilePath, std::ios::out | std::ios::app);
    if (!out.is_open()) {
        frontend_logger.warn("Unable to open history results file: " + resultFilePath);
        return;
    }

    std::time_t now = std::time(nullptr);
    out << "[" << format_local_timestamp(now) << "] " << queryName
        << " graph=" << graphId;
    if (!requestDetails.empty()) {
        out << " " << requestDetails;
    }
    out << "\n";
    out << responseText;
    if (responseText.empty() || responseText.back() != '\n') {
        out << "\n";
    }
    out << "----\n";
}

static std::string sanitizeForFileName(const std::string& value) {
    std::string result = value;
    for (char& ch : result) {
        if (!(std::isalnum(static_cast<unsigned char>(ch)) || ch == '_' || ch == '-' || ch == '.')) {
            ch = '_';
        }
    }
    return result;
}

static std::string captureCommandOutput(const std::string& command) {
    std::string output;
    FILE* pipe = popen(command.c_str(), "r");
    if (pipe == nullptr) {
        return output;
    }

    char buffer[4096];
    while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
        output.append(buffer);
    }

    pclose(pipe);
    return output;
}

static bool copyCommandOutputToFile(const std::string& command, const std::string& destinationPath) {
    FILE* pipe = popen(command.c_str(), "r");
    if (pipe == nullptr) {
        return false;
    }

    std::ofstream out(destinationPath, std::ios::binary | std::ios::trunc);
    if (!out.is_open()) {
        pclose(pipe);
        return false;
    }

    char buffer[8192];
    size_t bytesRead = 0;
    while ((bytesRead = fread(buffer, 1, sizeof(buffer), pipe)) > 0) {
        out.write(buffer, static_cast<std::streamsize>(bytesRead));
        if (!out.good()) {
            break;
        }
    }

    int status = pclose(pipe);
    out.close();
    return out.good() && status == 0;
}

static std::string buildWorkerTarget(const Utils::worker& worker) {
    // Some deployments persist user@host inside hostname already.
    // Avoid invalid targets like user@user@host.
    if (worker.hostname.find('@') != std::string::npos) {
        return worker.hostname;
    }
    if (!worker.username.empty()) {
        return worker.username + "@" + worker.hostname;
    }
    return worker.hostname;
}

static std::vector<std::string> buildWorkerTargetCandidates(const Utils::worker& worker) {
    std::vector<std::string> candidates;

    auto pushUnique = [&](const std::string& value) {
        if (value.empty()) {
            return;
        }
        if (std::find(candidates.begin(), candidates.end(), value) == candidates.end()) {
            candidates.push_back(value);
        }
    };

    // Preferred canonical target first.
    pushUnique(buildWorkerTarget(worker));

    // Fallbacks for mixed worker table data (ip may already include user, etc.).
    pushUnique(worker.hostname);
    if (!worker.username.empty() && worker.hostname.find('@') == std::string::npos) {
        pushUnique(worker.username + "@" + worker.hostname);
    }

    return candidates;
}

static void collectTemporalSnapshotMetadataFromDirectory(
    const std::string& snapshotDir, int graphId,
    std::map<uint32_t, TemporalSnapshotSummary>& snapMap,
    SnapshotPartitionTotals& partitionTotals) {
    std::vector<std::string> files = Utils::getListOfFilesInDirectory(snapshotDir);
    std::string graphPrefix = "graph" + std::to_string(graphId) + "_part";

    for (const auto& file : files) {
        if (file.find(graphPrefix) == std::string::npos) continue;
        if (file.find("_snapmeta.bin") == std::string::npos) continue;

        uint32_t partitionId = 0;
        if (!tryParsePartitionIdFromSnapmetaFileName(file, graphId, partitionId)) {
            continue;
        }

        std::string metaPath = snapshotDir + "/" + file;
        auto records = TemporalStorePersistence::readAllSnapmeta(metaPath);
        for (const auto& rec : records) {
            mergeSnapshotMetaRecord(rec, partitionId, snapMap, partitionTotals);
        }
    }
}

static void collectTemporalSnapshotMetadataFromRemoteHost(
    const Utils::worker& worker, int graphId, const std::string& snapshotDir,
    std::map<uint32_t, TemporalSnapshotSummary>& snapMap,
    SnapshotPartitionTotals& partitionTotals) {
    std::string hostTarget = buildWorkerTarget(worker);
    if (hostTarget.empty() || hostTarget == "localhost" || hostTarget == "127.0.0.1") {
        return;
    }

    std::string graphPrefix = "graph" + std::to_string(graphId) + "_part";
    std::string findCommand = "ssh -o BatchMode=yes -o ConnectTimeout=5 " + shellQuote(hostTarget) +
                              " find " + shellQuote(snapshotDir) +
                              " -maxdepth 1 -type f -name " + shellQuote(graphPrefix + "*_snapmeta.bin") +
                              " 2>/dev/null";

    std::string remoteFiles = captureCommandOutput(findCommand);
    std::stringstream filesStream(remoteFiles);
    std::string remoteFile;
    while (std::getline(filesStream, remoteFile)) {
        remoteFile = Utils::trim_copy(remoteFile);
        if (remoteFile.empty()) {
            continue;
        }

        std::string tempFile = "/tmp/jg_tmpsn_" + sanitizeForFileName(hostTarget) + "_" +
                               sanitizeForFileName(Utils::getFileName(remoteFile));
        std::string catCommand = "ssh -o BatchMode=yes -o ConnectTimeout=5 " + shellQuote(hostTarget) +
                                 " cat " + shellQuote(remoteFile);

        if (!copyCommandOutputToFile(catCommand, tempFile)) {
            frontend_logger.error("Failed to fetch snapshot metadata from host " + hostTarget + ": " + remoteFile);
            continue;
        }

        uint32_t partitionId = 0;
        std::string baseFileName = Utils::getFileName(remoteFile);
        if (!tryParsePartitionIdFromSnapmetaFileName(baseFileName, graphId, partitionId)) {
            remove(tempFile.c_str());
            continue;
        }

        auto records = TemporalStorePersistence::readAllSnapmeta(tempFile);
        for (const auto& rec : records) {
            mergeSnapshotMetaRecord(rec, partitionId, snapMap, partitionTotals);
        }

        remove(tempFile.c_str());
    }
}

static std::map<uint32_t, TemporalSnapshotSummary> loadTemporalSnapshotSummariesForGraph(SQLiteDBInterface* sqlite,
                                                                                         int graphId) {
    std::map<uint32_t, TemporalSnapshotSummary> snapMap;
    SnapshotPartitionTotals partitionTotals;
    std::string snapshotDir = getTemporalSnapshotDir();

    collectTemporalSnapshotMetadataFromDirectory(snapshotDir, graphId, snapMap, partitionTotals);

    const std::vector<Utils::worker>& workerList = Utils::getWorkerList(sqlite);
    std::set<std::string> visitedHosts;
    for (const auto& worker : workerList) {
        std::string hostTarget = buildWorkerTarget(worker);
        if (hostTarget.empty()) {
            continue;
        }
        if (visitedHosts.insert(hostTarget).second) {
            collectTemporalSnapshotMetadataFromRemoteHost(worker, graphId, snapshotDir, snapMap, partitionTotals);
        }
    }

    return snapMap;
}

static bool copyLocalFileBinary(const std::string& sourcePath, const std::string& destinationPath) {
    std::ifstream in(sourcePath, std::ios::binary);
    if (!in.is_open()) {
        return false;
    }

    std::ofstream out(destinationPath, std::ios::binary | std::ios::trunc);
    if (!out.is_open()) {
        return false;
    }

    out << in.rdbuf();
    return !in.bad() && out.good();
}

static int collectTemporalBitmapIndexesFromDirectory(const std::string& snapshotDir,
                                                     int graphId,
                                                     const std::string& destinationDir) {
    std::vector<std::string> files = Utils::getListOfFilesInDirectory(snapshotDir);
    std::string graphPrefix = "graph" + std::to_string(graphId) + "_part";
    int copied = 0;

    for (const auto& file : files) {
        if (file.find(graphPrefix) == std::string::npos) continue;
        bool isLegacyBitmap = file.find("_bitmaps.ebm") != std::string::npos;
        bool isDeltaFile = file.find(".delta") != std::string::npos &&
                           file.find("_snap") != std::string::npos;
        if (!isLegacyBitmap && !isDeltaFile) continue;

        std::string sourcePath = snapshotDir + "/" + file;
        std::string destinationPath = destinationDir + "/" + file;
        if (copyLocalFileBinary(sourcePath, destinationPath)) {
            copied++;
        }
    }

    return copied;
}

static int collectTemporalBitmapIndexesFromRemoteTarget(const std::string& hostTarget,
                                                        int graphId,
                                                        const std::string& snapshotDir,
                                                        const std::string& destinationDir) {
    if (hostTarget.empty() || hostTarget == "localhost" || hostTarget == "127.0.0.1") {
        return 0;
    }

    std::string graphPrefix = "graph" + std::to_string(graphId) + "_part";

    auto buildFindCommand = [&](const std::string& rootDir) {
         std::string remoteCommand = "find " + shellQuote(rootDir) +
                         " -maxdepth 1 -type f \\( -name " +
                         shellQuote(graphPrefix + "*_bitmaps.ebm") +
                         " -o -name " + shellQuote(graphPrefix + "*_snap*.delta") +
                         " \\) 2>/dev/null";
         return "timeout 10 ssh -o BatchMode=yes -o ConnectTimeout=5 " + shellQuote(hostTarget) +
             " " + shellQuote(remoteCommand);
    };

    int copied = 0;
    std::string remoteFiles = captureCommandOutput(buildFindCommand(snapshotDir));
    if (remoteFiles.empty()) {
        frontend_logger.warn("No remote temporal bitmap files found for graph " +
                             std::to_string(graphId) + " on target " + hostTarget +
                             " in " + snapshotDir);

        std::string fallbackDir = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") +
                                  "/temporal_snapshots";
        if (fallbackDir != snapshotDir) {
            remoteFiles = captureCommandOutput(buildFindCommand(fallbackDir));
            if (!remoteFiles.empty()) {
                frontend_logger.info("Remote temporal bitmap files found for graph " +
                                     std::to_string(graphId) + " on target " + hostTarget +
                                     " in fallback dir " + fallbackDir);
            }
        }
    }
    std::stringstream filesStream(remoteFiles);
    std::string remoteFile;
    while (std::getline(filesStream, remoteFile)) {
        remoteFile = Utils::trim_copy(remoteFile);
        if (remoteFile.empty()) {
            continue;
        }

        std::string destinationPath = destinationDir + "/" + Utils::getFileName(remoteFile);
        std::string catCommand = "timeout 10 ssh -o BatchMode=yes -o ConnectTimeout=5 " + shellQuote(hostTarget) +
                                 " cat " + shellQuote(remoteFile);

        if (copyCommandOutputToFile(catCommand, destinationPath)) {
            copied++;
        } else {
            frontend_logger.error("Failed to fetch bitmap index from target " + hostTarget + ": " + remoteFile);
        }
    }

    return copied;
}

static int collectTemporalBitmapIndexesFromRemoteHost(const Utils::worker& worker,
                                                      int graphId,
                                                      const std::string& snapshotDir,
                                                      const std::string& destinationDir) {
    std::vector<std::string> targets = buildWorkerTargetCandidates(worker);
    int totalCopied = 0;

    for (const auto& target : targets) {
        int copied = collectTemporalBitmapIndexesFromRemoteTarget(target, graphId, snapshotDir, destinationDir);
        if (copied > 0) {
            frontend_logger.info("Fetched " + std::to_string(copied) +
                                 " temporal bitmap files for graph " + std::to_string(graphId) +
                                 " from target " + target);
            totalCopied += copied;
            break;
        }
    }

    if (totalCopied == 0 && !targets.empty()) {
        frontend_logger.warn("No temporal bitmap files staged from any target for graph " +
                             std::to_string(graphId) + "; checked targets: " +
                             [&]() {
                                 std::string joined;
                                 for (const auto& target : targets) {
                                     if (!joined.empty()) {
                                         joined += ",";
                                     }
                                     joined += target;
                                 }
                                 return joined;
                             }());
    }

    return totalCopied;
}

static std::string stageTemporalBitmapIndexesForGraph(SQLiteDBInterface* sqlite,
                                                      int graphId,
                                                      const std::string& snapshotDir) {
    char tempDirTemplate[] = "/tmp/jg_histrian_XXXXXX";
    char* createdDir = mkdtemp(tempDirTemplate);
    if (createdDir == nullptr) {
        return "";
    }

    std::string stagingDir(createdDir);
    int copied = 0;
    copied += collectTemporalBitmapIndexesFromDirectory(snapshotDir, graphId, stagingDir);

    const std::vector<Utils::worker>& workerList = Utils::getWorkerList(sqlite);
    std::set<std::string> visitedHosts;
    for (const auto& worker : workerList) {
        std::string hostTarget = buildWorkerTarget(worker);
        if (hostTarget.empty()) {
            continue;
        }
        if (visitedHosts.insert(hostTarget).second) {
            copied += collectTemporalBitmapIndexesFromRemoteHost(worker, graphId, snapshotDir, stagingDir);
        }
    }

    if (copied == 0) {
        rmdir(stagingDir.c_str());
        return "";
    }

    return stagingDir;
}

static void cleanupStagedTemporalBitmapIndexes(const std::string& stagingDir) {
    if (stagingDir.empty()) {
        return;
    }

    std::vector<std::string> files = Utils::getListOfFilesInDirectory(stagingDir);
    for (const auto& file : files) {
        std::string fullPath = stagingDir + "/" + file;
        remove(fullPath.c_str());
    }
    rmdir(stagingDir.c_str());
}

static bool tryParsePartitionIdFromTemporalFileName(const std::string& fileName,
                                                    int graphId,
                                                    int& partitionId) {
    std::string prefix = "graph" + std::to_string(graphId) + "_part";
    if (fileName.rfind(prefix, 0) != 0) {
        return false;
    }

    size_t partitionStart = prefix.size();
    size_t partitionEnd = partitionStart;
    while (partitionEnd < fileName.size() &&
           std::isdigit(static_cast<unsigned char>(fileName[partitionEnd]))) {
        ++partitionEnd;
    }

    if (partitionEnd == partitionStart) {
        return false;
    }

    partitionId = std::stoi(fileName.substr(partitionStart, partitionEnd - partitionStart));
    return true;
}

static std::set<int> discoverTemporalPartitionsOnRemoteTarget(const std::string& hostTarget,
                                                              int graphId,
                                                              const std::string& snapshotDir) {
    std::set<int> partitionIds;
    if (hostTarget.empty() || hostTarget == "localhost" || hostTarget == "127.0.0.1") {
        return partitionIds;
    }

    auto listFilesForRoot = [&](const std::string& rootDir) {
        std::string graphPrefix = "graph" + std::to_string(graphId) + "_part";
        std::string remoteCommand =
            "find " + shellQuote(rootDir) +
            " -maxdepth 1 -type f \\( -name " + shellQuote(graphPrefix + "*_snapmeta.bin") +
            " -o -name " + shellQuote(graphPrefix + "*_bitmaps.ebm") +
            " -o -name " + shellQuote(graphPrefix + "*_snap*.delta") +
            " \\) 2>/dev/null";
        std::string command = "ssh -o BatchMode=yes -o ConnectTimeout=5 " +
                              shellQuote(hostTarget) + " " + shellQuote(remoteCommand);
        return captureCommandOutput(command);
    };

    std::string files = listFilesForRoot(snapshotDir);
    if (files.empty()) {
        std::string fallbackDir =
            Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") +
            "/temporal_snapshots";
        if (fallbackDir != snapshotDir) {
            files = listFilesForRoot(fallbackDir);
        }
    }

    std::stringstream fileStream(files);
    std::string remoteFile;
    while (std::getline(fileStream, remoteFile)) {
        std::string trimmed = Utils::trim_copy(remoteFile);
        if (trimmed.empty()) {
            continue;
        }

        int partitionId = -1;
        std::string baseName = Utils::getFileName(trimmed);
        if (tryParsePartitionIdFromTemporalFileName(baseName, graphId, partitionId)) {
            partitionIds.insert(partitionId);
        }
    }

    return partitionIds;
}

static std::set<int> discoverTemporalPartitionsForWorker(const Utils::worker& worker,
                                                         int graphId,
                                                         const std::string& snapshotDir) {
    std::set<int> partitionIds;
    std::vector<std::string> targets = buildWorkerTargetCandidates(worker);

    for (const auto& target : targets) {
        std::set<int> found = discoverTemporalPartitionsOnRemoteTarget(target, graphId, snapshotDir);
        if (!found.empty()) {
            partitionIds.insert(found.begin(), found.end());
            break;
        }
    }

    return partitionIds;
}

static bool parsePartitionIdFromBitmapFileName(const std::string& fileName,
                                                int graphId,
                                                uint32_t& partitionId) {
    const std::string prefix = "graph" + std::to_string(graphId) + "_part";
    const std::string suffix = "_bitmaps.ebm";

    if (fileName.rfind(prefix, 0) != 0) {
        return false;
    }
    if (fileName.size() <= prefix.size() + suffix.size()) {
        return false;
    }
    if (fileName.compare(fileName.size() - suffix.size(), suffix.size(), suffix) != 0) {
        return false;
    }

    std::string partitionText =
        fileName.substr(prefix.size(), fileName.size() - prefix.size() - suffix.size());
    if (partitionText.empty()) {
        return false;
    }
    if (!std::all_of(partitionText.begin(), partitionText.end(),
                     [](unsigned char ch) { return std::isdigit(ch); })) {
        return false;
    }

    partitionId = static_cast<uint32_t>(std::stoul(partitionText));
    return true;
}

static bool parsePartitionIdFromDeltaFileName(const std::string& fileName,
                                              int graphId,
                                              uint32_t& partitionId) {
    const std::string prefix = "graph" + std::to_string(graphId) + "_part";
    const std::string splitMarker = "_snap";
    const std::string suffix = ".delta";

    if (fileName.rfind(prefix, 0) != 0) {
        return false;
    }
    if (fileName.size() <= prefix.size() + splitMarker.size() + suffix.size()) {
        return false;
    }
    if (fileName.compare(fileName.size() - suffix.size(), suffix.size(), suffix) != 0) {
        return false;
    }

    size_t snapPos = fileName.find(splitMarker, prefix.size());
    if (snapPos == std::string::npos || snapPos <= prefix.size()) {
        return false;
    }

    std::string partitionText = fileName.substr(prefix.size(), snapPos - prefix.size());
    if (partitionText.empty()) {
        return false;
    }
    if (!std::all_of(partitionText.begin(), partitionText.end(),
                     [](unsigned char ch) { return std::isdigit(ch); })) {
        return false;
    }

    partitionId = static_cast<uint32_t>(std::stoul(partitionText));
    return true;
}

static bool hasTemporalBitmapIndexesForGraphInDirectory(const std::string& directory,
                                                        int graphId) {
    if (directory.empty()) {
        return false;
    }

    std::vector<std::string> files = Utils::getListOfFilesInDirectory(directory);
    for (const auto& file : files) {
        uint32_t partitionId = 0;
        if (parsePartitionIdFromBitmapFileName(file, graphId, partitionId) ||
            parsePartitionIdFromDeltaFileName(file, graphId, partitionId)) {
            return true;
        }
    }

    return false;
}

static std::vector<std::string> getLocalTemporalSnapshotCandidateDirs() {
    std::vector<std::string> candidates;

    std::string configuredDir = getTemporalSnapshotDir();
    if (!configuredDir.empty()) {
        candidates.push_back(configuredDir);
    }

    std::string dataFolderDir =
        Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") +
        "/temporal_snapshots";
    if (!dataFolderDir.empty() &&
        std::find(candidates.begin(), candidates.end(), dataFolderDir) == candidates.end()) {
        candidates.push_back(dataFolderDir);
    }

    std::string homeFallbackDir = Utils::getJasmineGraphHome() + "/env/data/temporal_snapshots";
    if (!homeFallbackDir.empty() &&
        std::find(candidates.begin(), candidates.end(), homeFallbackDir) == candidates.end()) {
        candidates.push_back(homeFallbackDir);
    }

    return candidates;
}

static bool countHistoryTrianglesFromStagedBitmaps(SQLiteDBInterface* sqlite,
                                                   int graphId,
                                                   uint32_t snapshotId,
                                                   TemporalTriangleResult& result,
                                                   std::string& errorMessage,
                                                   std::string& dataSourceInfo) {
    std::string snapshotDir = getTemporalSnapshotDir();
    std::string localDirectFailureReason;
    std::vector<std::string> localDirs = getLocalTemporalSnapshotCandidateDirs();

    // Fast path: execute directly from local snapshot directories that actually contain
    // bitmap files for this graph. This avoids expensive per-query staging.
    for (const auto& localDir : localDirs) {
        if (!hasTemporalBitmapIndexesForGraphInDirectory(localDir, graphId)) {
            continue;
        }

        try {
            TemporalTriangleResult directResult =
                HistoryTriangles::countTrianglesAtSnapshot(graphId, snapshotId, localDir);
            if (directResult.partitionsProcessed > 0) {
                directResult.stagingMs = 0;
                result = std::move(directResult);
                dataSourceInfo = "Data source: local-direct (path=" + localDir + ")";
                frontend_logger.info("histrian local-direct path used: " + localDir);
                return true;
            }
            localDirectFailureReason = "local-direct found files in " + localDir +
                                       " but processed zero partitions";
        } catch (const std::exception&) {
            localDirectFailureReason = "local-direct read/count failed in " + localDir;
        }
    }

    if (localDirectFailureReason.empty()) {
        std::stringstream reason;
        reason << "no graph bitmap files found in local candidates";
        if (!localDirs.empty()) {
            reason << " [";
            for (size_t i = 0; i < localDirs.size(); ++i) {
                if (i > 0) {
                    reason << ",";
                }
                reason << localDirs[i];
            }
            reason << "]";
        }
        localDirectFailureReason = reason.str();
    }

    auto stagedStart = std::chrono::high_resolution_clock::now();
    std::string stagedSnapshotDir = stageTemporalBitmapIndexesForGraph(sqlite, graphId, snapshotDir);
    if (stagedSnapshotDir.empty()) {
        errorMessage = "No temporal bitmap index files available for graph " + std::to_string(graphId);
        return false;
    }

    auto stagedEnd = std::chrono::high_resolution_clock::now();
    long stagingMs = std::chrono::duration_cast<std::chrono::milliseconds>(stagedEnd - stagedStart).count();

    try {
        result = HistoryTriangles::countTrianglesAtSnapshot(graphId, snapshotId, stagedSnapshotDir);
        result.stagingMs = stagingMs;
        dataSourceInfo = "Data source: staged-fallback (reason=" + localDirectFailureReason + ")";
        cleanupStagedTemporalBitmapIndexes(stagedSnapshotDir);
    } catch (const std::exception& e) {
        cleanupStagedTemporalBitmapIndexes(stagedSnapshotDir);
        errorMessage = e.what();
        return false;
    }

    if (result.partitionsProcessed == 0) {
        errorMessage = "No snapshot partitions were processed for graph " + std::to_string(graphId);
        return false;
    }

    return true;
}

namespace {

constexpr size_t HISTORY_TRIANGLE_SHARD_COUNT = 256;
constexpr size_t HISTORY_TRIANGLE_NODE_BUCKET_COUNT = 256;
constexpr size_t HISTORY_TRIANGLE_BATCH_SIZE = 1024;

bool recvAll(int sockfd, void* data, size_t length) {
    char* cursor = static_cast<char*>(data);
    size_t received = 0;
    while (received < length) {
        ssize_t readCount = recv(sockfd, cursor + received, length - received, 0);
        if (readCount <= 0) {
            return false;
        }
        received += static_cast<size_t>(readCount);
    }
    return true;
}

uint32_t fromNetwork32(uint32_t value) {
    return ntohl(value);
}

bool readUint32(int sockfd, uint32_t& value) {
    uint32_t networkValue = 0;
    if (!recvAll(sockfd, &networkValue, sizeof(networkValue))) {
        return false;
    }
    value = fromNetwork32(networkValue);
    return true;
}

bool readUint64(int sockfd, uint64_t& value) {
    uint32_t high = 0;
    uint32_t low = 0;

    if (!readUint32(sockfd, high) || !readUint32(sockfd, low)) {
        return false;
    }

    value = (static_cast<uint64_t>(high) << 32) | static_cast<uint64_t>(low);
    return true;
}

bool readDouble(int sockfd, double& value) {
    // Read double as 8 bytes
    if (!recvAll(sockfd, &value, sizeof(double))) {
        return false;
    }
    return true;
}

std::string createEdgeShardTempDir() {
    char tempDirTemplate[] = "/tmp/jg_histrian_frontend_XXXXXX";
    char* createdDir = mkdtemp(tempDirTemplate);
    if (createdDir == nullptr) {
        return "";
    }
    return std::string(createdDir);
}

uint64_t encodeUndirectedEdge(uint32_t sourceIndex, uint32_t destIndex) {
    if (sourceIndex > destIndex) {
        std::swap(sourceIndex, destIndex);
    }
    return (static_cast<uint64_t>(sourceIndex) << 32) | static_cast<uint64_t>(destIndex);
}

uint32_t decodeSourceIndex(uint64_t encoded) {
    return static_cast<uint32_t>(encoded >> 32);
}

uint32_t decodeDestIndex(uint64_t encoded) {
    return static_cast<uint32_t>(encoded & 0xffffffffULL);
}

size_t countCommonSortedValues(const uint32_t* left,
                               size_t leftSize,
                               const uint32_t* right,
                               size_t rightSize) {
    size_t count = 0;
    size_t leftIndex = 0;
    size_t rightIndex = 0;

    while (leftIndex < leftSize && rightIndex < rightSize) {
        uint32_t leftValue = left[leftIndex];
        uint32_t rightValue = right[rightIndex];
        if (leftValue == rightValue) {
            ++count;
            ++leftIndex;
            ++rightIndex;
        } else if (leftValue < rightValue) {
            ++leftIndex;
        } else {
            ++rightIndex;
        }
    }

    return count;
}

uint64_t countTrianglesOnCSRGraph(const std::vector<uint64_t>& csrOffsets,
                                  const std::vector<uint32_t>& csrNeighbors) {
    if (csrOffsets.size() < 2 || csrNeighbors.empty()) {
        return 0;
    }
    const uint32_t nodeCount = static_cast<uint32_t>(csrOffsets.size() - 1);
    const uint32_t* neighborsData = csrNeighbors.data();
    uint64_t triangleCount = 0;
#ifdef _OPENMP
    if (nodeCount >= 2048) {
#pragma omp parallel for schedule(dynamic, 64) reduction(+:triangleCount)
        for (int64_t i = 0; i < static_cast<int64_t>(nodeCount); ++i) {
            uint64_t srcBegin = csrOffsets[static_cast<size_t>(i)];
            uint64_t srcEnd = csrOffsets[static_cast<size_t>(i) + 1];
            const uint32_t* srcNbrs = neighborsData + srcBegin;
            size_t srcSize = static_cast<size_t>(srcEnd - srcBegin);
            for (size_t offset = 0; offset < srcSize; ++offset) {
                uint32_t mid = srcNbrs[offset];
                uint64_t midBegin = csrOffsets[mid];
                uint64_t midEnd = csrOffsets[mid + 1];
                const uint32_t* midNbrs = neighborsData + midBegin;
                size_t midSize = static_cast<size_t>(midEnd - midBegin);
                triangleCount += (srcSize < midSize)
                    ? countCommonSortedValues(srcNbrs, srcSize, midNbrs, midSize)
                    : countCommonSortedValues(midNbrs, midSize, srcNbrs, srcSize);
            }
        }
        return triangleCount;
    }
#endif
    for (uint32_t i = 0; i < nodeCount; ++i) {
        uint64_t srcBegin = csrOffsets[i];
        uint64_t srcEnd = csrOffsets[i + 1];
        const uint32_t* srcNbrs = neighborsData + srcBegin;
        size_t srcSize = static_cast<size_t>(srcEnd - srcBegin);
        for (size_t offset = 0; offset < srcSize; ++offset) {
            uint32_t mid = srcNbrs[offset];
            uint64_t midBegin = csrOffsets[mid];
            uint64_t midEnd = csrOffsets[mid + 1];
            const uint32_t* midNbrs = neighborsData + midBegin;
            size_t midSize = static_cast<size_t>(midEnd - midBegin);
            triangleCount += (srcSize < midSize)
                ? countCommonSortedValues(srcNbrs, srcSize, midNbrs, midSize)
                : countCommonSortedValues(midNbrs, midSize, srcNbrs, srcSize);
        }
    }
    return triangleCount;
}

bool rewriteUniqueEncodedEdgesToBinaryFile(const std::string& filePath,
                                           const std::vector<uint64_t>& edges) {
    std::ofstream out(filePath, std::ios::binary | std::ios::trunc);
    if (!out.is_open()) {
        return false;
    }
    if (!edges.empty()) {
        out.write(reinterpret_cast<const char*>(edges.data()),
                  static_cast<std::streamsize>(edges.size() * sizeof(uint64_t)));
    }
    return static_cast<bool>(out);
}

class HistoryTriangleAggregation {
 public:
    HistoryTriangleAggregation() : tempDir(createEdgeShardTempDir()) {
        if (!tempDir.empty()) {
            for (size_t shardId = 0; shardId < HISTORY_TRIANGLE_SHARD_COUNT; ++shardId) {
                shardPaths[shardId] = tempDir + "/edges_" + std::to_string(shardId) + ".bin";
                shardStreams[shardId].open(shardPaths[shardId], std::ios::binary | std::ios::trunc);
            }
        }
        // Pre-allocate maps to avoid frequent rehashes during ingestion.
        // For a graph with 200M nodes, each of 256 buckets will hold ~800k entries.
        for (auto& bucket : nodeBuckets) {
            bucket.reserve(500000);
        }
    }

    ~HistoryTriangleAggregation() {
        for (auto& stream : shardStreams) {
            if (stream.is_open()) {
                stream.close();
            }
        }
        if (!tempDir.empty()) {
            Utils::deleteDirectory(tempDir);
        }
    }

    const std::string& tempDirPath() const {
        return tempDir;
    }

    bool ready() const {
        if (tempDir.empty()) {
            return false;
        }
        for (const auto& stream : shardStreams) {
            if (!stream.is_open()) {
                return false;
            }
        }
        return true;
    }

    uint32_t getOrAddNode(const std::string& node) {
        size_t bucketId = std::hash<std::string>{}(node) % HISTORY_TRIANGLE_NODE_BUCKET_COUNT;
        std::lock_guard<std::mutex> lock(nodeBucketLocks[bucketId]);
        auto& bucket = nodeBuckets[bucketId];
        auto it = bucket.find(node);
        if (it != bucket.end()) {
            return it->second;
        }
        uint32_t index = nextNodeIndex.fetch_add(1, std::memory_order_relaxed);
        bucket.emplace(node, index);
        return index;
    }

    void appendEdge(const std::string& sourceId, const std::string& destId) {
        uint32_t sourceIndex = getOrAddNode(sourceId);
        uint32_t destIndex = getOrAddNode(destId);
        uint64_t encoded = encodeUndirectedEdge(sourceIndex, destIndex);
        size_t shardId = static_cast<size_t>(encoded & (HISTORY_TRIANGLE_SHARD_COUNT - 1));
        std::lock_guard<std::mutex> lock(shardLocks[shardId]);
        shardStreams[shardId].write(reinterpret_cast<const char*>(&encoded), sizeof(encoded));
        rawEdgeCount.fetch_add(1, std::memory_order_relaxed);
    }

    uint64_t getRawEdgeCount() const {
        return rawEdgeCount.load(std::memory_order_relaxed);
    }

    uint32_t getNodeCount() const {
        return nextNodeIndex.load(std::memory_order_relaxed);
    }

    const std::array<std::string, HISTORY_TRIANGLE_SHARD_COUNT>& getShardPaths() const {
        return shardPaths;
    }

    bool finalizeShardStreams() {
        for (auto& stream : shardStreams) {
            stream.flush();
            if (!stream.good()) {
                return false;
            }
        }
        return true;
    }

 private:
    std::string tempDir;
    std::array<std::string, HISTORY_TRIANGLE_SHARD_COUNT> shardPaths{};
    std::array<std::ofstream, HISTORY_TRIANGLE_SHARD_COUNT> shardStreams;
    std::array<std::mutex, HISTORY_TRIANGLE_SHARD_COUNT> shardLocks;
    std::array<std::unordered_map<std::string, uint32_t>, HISTORY_TRIANGLE_NODE_BUCKET_COUNT> nodeBuckets;
    std::array<std::mutex, HISTORY_TRIANGLE_NODE_BUCKET_COUNT> nodeBucketLocks;
    std::atomic<uint32_t> nextNodeIndex{0};
    std::atomic<uint64_t> rawEdgeCount{0};
};

// Returns localTriangleCount on success, or -1 on failure.
// Boundary edges from the worker are inserted into `aggregation`.
bool collectHistoryTriangleEdgesFromWorker(int graphId,
                                           uint32_t snapshotId,
                                           const Utils::worker& worker,
                                           int partitionId,
                                           const std::string& masterIP,
                                           int threadPriority,
                                           HistoryTriangleAggregation& aggregation,
                                           long& workerDurationMs,
                                           uint64_t& workerRawEdges,
                                           uint64_t& workerLocalTriangles) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        frontend_logger.error("Cannot create socket for distributed history triangle count");
        return false;
    }

    std::string host = worker.hostname;
    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }

    struct hostent* server = gethostbyname(host.c_str());
    if (server == nullptr) {
        frontend_logger.error("Failed to resolve worker host " + host);
        close(sockfd);
        return false;
    }

    struct sockaddr_in serv_addr;
    bzero((char*)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char*)server->h_addr, (char*)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(std::stoi(worker.port));

    if (Utils::connect_wrapper(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        frontend_logger.error("Failed to connect to worker " + host + ":" + worker.port);
        close(sockfd);
        return false;
    }

    std::string data(INSTANCE_DATA_LENGTH + 1, '\0');
    auto closeAndFail = [&]() {
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return false;
    };

    if (!Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE) ||
        Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH) !=
            JasmineGraphInstanceProtocol::HANDSHAKE_OK) {
        return closeAndFail();
    }

    if (!Utils::send_str_wrapper(sockfd, masterIP) ||
        Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH) !=
            JasmineGraphInstanceProtocol::HOST_OK) {
        return closeAndFail();
    }

    if (!Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::HISTORY_TRIANGLES) ||
        Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH) !=
            JasmineGraphInstanceProtocol::OK) {
        return closeAndFail();
    }

    if (!Utils::send_str_wrapper(sockfd, std::to_string(graphId)) ||
        Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH) !=
            JasmineGraphInstanceProtocol::OK) {
        return closeAndFail();
    }

    if (!Utils::send_str_wrapper(sockfd, std::to_string(partitionId)) ||
        Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH) !=
            JasmineGraphInstanceProtocol::OK) {
        return closeAndFail();
    }

    if (!Utils::send_str_wrapper(sockfd, std::to_string(snapshotId)) ||
        Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH) !=
            JasmineGraphInstanceProtocol::OK) {
        return closeAndFail();
    }

    if (!Utils::send_str_wrapper(sockfd, std::to_string(threadPriority)) ||
        Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH) !=
            JasmineGraphInstanceProtocol::OK) {
        return closeAndFail();
    }

    if (!Utils::send_str_wrapper(sockfd, "trace-disabled")) {
        return closeAndFail();
    }

    // New protocol: worker sends localTriangleCount FIRST, then boundary-edge batches
    uint64_t localTriCount = 0;
    if (!readUint64(sockfd, localTriCount)) {
        return closeAndFail();
    }
    workerLocalTriangles = localTriCount;

    uint32_t batchCount = 0;
    while (true) {
        if (!readUint32(sockfd, batchCount)) {
            return closeAndFail();
        }
        if (batchCount == 0) {
            break;
        }

        for (uint32_t i = 0; i < batchCount; ++i) {
            uint32_t sourceLength = 0;
            uint32_t destLength = 0;
            if (!readUint32(sockfd, sourceLength)) {
                return closeAndFail();
            }
            std::string sourceId(sourceLength, '\0');
            if (!recvAll(sockfd, sourceId.data(), sourceLength)) {
                return closeAndFail();
            }

            if (!readUint32(sockfd, destLength)) {
                return closeAndFail();
            }
            std::string destId(destLength, '\0');
            if (!recvAll(sockfd, destId.data(), destLength)) {
                return closeAndFail();
            }

            if (sourceId != destId) {
                aggregation.appendEdge(sourceId, destId);
            }
        }
    }

    uint64_t rawEdgesFromWorker = 0;
    uint64_t durationFromWorker = 0;
    if (!readUint64(sockfd, rawEdgesFromWorker) || !readUint64(sockfd, durationFromWorker)) {
        return closeAndFail();
    }

    workerRawEdges    = rawEdgesFromWorker;
    workerDurationMs  = static_cast<long>(durationFromWorker);

    Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
    close(sockfd);
    return true;
}


std::pair<uint64_t, uint64_t> countTrianglesFromEncodedShards(
    const std::array<std::string, HISTORY_TRIANGLE_SHARD_COUNT>& shardPaths,
    uint32_t nodeCount) {

    if (nodeCount == 0) {
        return {0, 0};
    }

    std::vector<uint32_t> degree(nodeCount, 0);
    uint64_t uniqueEdges = 0;

    for (const auto& shardPath : shardPaths) {
        std::ifstream in(shardPath, std::ios::binary);
        if (!in.is_open()) {
            continue;
        }

        in.seekg(0, std::ios::end);
        std::streamoff fileSize = in.tellg();
        in.seekg(0, std::ios::beg);

        std::vector<uint64_t> shardEdges;
        if (fileSize > 0) {
            shardEdges.reserve(static_cast<size_t>(fileSize / static_cast<std::streamoff>(sizeof(uint64_t))));
        }

        uint64_t encoded = 0;
        while (in.read(reinterpret_cast<char*>(&encoded), sizeof(encoded))) {
            shardEdges.push_back(encoded);
        }

        if (shardEdges.empty()) {
            continue;
        }

        std::sort(shardEdges.begin(), shardEdges.end());
        shardEdges.erase(std::unique(shardEdges.begin(), shardEdges.end()), shardEdges.end());

        uniqueEdges += shardEdges.size();
        for (uint64_t edge : shardEdges) {
            uint32_t sourceIndex = decodeSourceIndex(edge);
            uint32_t destIndex = decodeDestIndex(edge);
            degree[sourceIndex]++;
            degree[destIndex]++;
        }

        if (!rewriteUniqueEncodedEdgesToBinaryFile(shardPath, shardEdges)) {
            return {0, 0};
        }
    }

    std::vector<uint32_t> forwardDegree(nodeCount, 0);
    for (const auto& shardPath : shardPaths) {
        std::ifstream in(shardPath, std::ios::binary);
        if (!in.is_open()) {
            continue;
        }

        uint64_t encoded = 0;
        while (in.read(reinterpret_cast<char*>(&encoded), sizeof(encoded))) {
            uint32_t sourceIndex = decodeSourceIndex(encoded);
            uint32_t destIndex = decodeDestIndex(encoded);

            bool sourceBeforeDest = (degree[sourceIndex] < degree[destIndex]) ||
                                    (degree[sourceIndex] == degree[destIndex] &&
                                     sourceIndex < destIndex);
            if (sourceBeforeDest) {
                forwardDegree[sourceIndex]++;
            } else {
                forwardDegree[destIndex]++;
            }
        }
    }

    std::vector<uint64_t> csrOffsets(nodeCount + 1, 0);
    for (uint32_t nodeIndex = 0; nodeIndex < nodeCount; ++nodeIndex) {
        csrOffsets[nodeIndex + 1] = csrOffsets[nodeIndex] + forwardDegree[nodeIndex];
    }

    std::vector<uint32_t> csrNeighbors(static_cast<size_t>(csrOffsets[nodeCount]));
    std::fill(forwardDegree.begin(), forwardDegree.end(), 0);

    for (const auto& shardPath : shardPaths) {
        std::ifstream in(shardPath, std::ios::binary);
        if (!in.is_open()) {
            continue;
        }

        uint64_t encoded = 0;
        while (in.read(reinterpret_cast<char*>(&encoded), sizeof(encoded))) {
            uint32_t sourceIndex = decodeSourceIndex(encoded);
            uint32_t destIndex = decodeDestIndex(encoded);

            bool sourceBeforeDest = (degree[sourceIndex] < degree[destIndex]) ||
                                    (degree[sourceIndex] == degree[destIndex] &&
                                     sourceIndex < destIndex);
            if (sourceBeforeDest) {
                uint32_t slot = 0;
#ifdef _OPENMP
#pragma omp atomic capture
#endif
                slot = forwardDegree[sourceIndex]++;
                csrNeighbors[static_cast<size_t>(csrOffsets[sourceIndex] + slot)] = destIndex;
            } else {
                uint32_t slot = 0;
#ifdef _OPENMP
#pragma omp atomic capture
#endif
                slot = forwardDegree[destIndex]++;
                csrNeighbors[static_cast<size_t>(csrOffsets[destIndex] + slot)] = sourceIndex;
            }
        }
    }

    std::vector<uint32_t>().swap(degree);
    std::vector<uint32_t>().swap(forwardDegree);

    for (uint32_t nodeIndex = 0; nodeIndex < nodeCount; ++nodeIndex) {
        uint64_t begin = csrOffsets[nodeIndex];
        uint64_t end = csrOffsets[nodeIndex + 1];
        if (end > begin + 1) {
            std::sort(csrNeighbors.begin() + static_cast<size_t>(begin),
                      csrNeighbors.begin() + static_cast<size_t>(end));
        }
    }

    uint64_t triangleCount = countTrianglesOnCSRGraph(csrOffsets, csrNeighbors);
    return {triangleCount, static_cast<uint32_t>(uniqueEdges)};
}

}  // namespace

static std::pair<long, long> getHistoryTriangleCountFromWorker(int graphId,
                                                                uint32_t snapshotId,
                                                                const Utils::worker& worker,
                                                                int partitionId,
                                                                const std::string& masterIP,
                                                                int threadPriority) {
    (void)graphId;
    (void)snapshotId;
    (void)worker;
    (void)partitionId;
    (void)masterIP;
    (void)threadPriority;
    return {-1, 0};
}

static TemporalTriangleResult countHistoryTrianglesDistributed(SQLiteDBInterface* sqlite,
                                                               int graphId,
                                                               uint32_t snapshotId,
                                                               const std::string& masterIP) {
    TemporalTriangleResult result{};
    std::vector<Utils::worker> allWorkers = Utils::getWorkerList(sqlite);

    // Query which workers host which partitions for this graph
    std::string sqlStatement =
        "SELECT DISTINCT worker_idworker, partition_idpartition "
        "FROM worker_has_partition "
        "WHERE partition_graph_idgraph=" + std::to_string(graphId) + ";";
    const auto& rows = sqlite->runSelect(sqlStatement);

    // workerID -> list of partition IDs
    std::map<std::string, std::vector<int>> workerPartitionMap;
    for (const auto& row : rows) {
        workerPartitionMap[row.at(0).second].push_back(std::stoi(row.at(1).second));
    }

    if (workerPartitionMap.empty()) {
        frontend_logger.warn("No worker-partition assignments found in worker_has_partition for graph " +
                             std::to_string(graphId) +
                             ". Falling back to remote temporal file discovery");

        std::string snapshotDir = getTemporalSnapshotDir();
        std::map<int, std::string> partitionOwner;
        for (const auto& worker : allWorkers) {
            std::set<int> discoveredPartitions =
                discoverTemporalPartitionsForWorker(worker, graphId, snapshotDir);

            if (discoveredPartitions.empty()) {
                continue;
            }

            for (int partitionId : discoveredPartitions) {
                auto inserted = partitionOwner.emplace(partitionId, worker.workerID);
                if (!inserted.second) {
                    frontend_logger.warn("Skipping duplicate discovered partition " +
                                         std::to_string(partitionId) +
                                         " for worker " + worker.workerID +
                                         " (already assigned to worker " + inserted.first->second + ")");
                    continue;
                }
                workerPartitionMap[worker.workerID].push_back(partitionId);
            }
        }

        if (workerPartitionMap.empty()) {
            frontend_logger.error("No worker-partition assignments found for graph " +
                                  std::to_string(graphId) +
                                  " and remote temporal partition discovery found nothing");
            return result;
        }

        for (auto& [workerId, partitions] : workerPartitionMap) {
            std::sort(partitions.begin(), partitions.end());
            partitions.erase(std::unique(partitions.begin(), partitions.end()), partitions.end());
        }

        frontend_logger.info("Discovered " + std::to_string(partitionOwner.size()) +
                             " remote temporal partitions for graph " +
                             std::to_string(graphId));
    }

    // Thread-safe sharded accumulator for boundary edges from all workers
    HistoryTriangleAggregation aggregation;
    if (!aggregation.ready()) {
        const std::string tempDirPath = aggregation.tempDirPath().empty()
            ? std::string("<unavailable>")
            : aggregation.tempDirPath();
        frontend_logger.error("Failed to initialise HistoryTriangleAggregation temp dir: " +
                              tempDirPath);
        return result;
    }

    // Per-task state (no shared mutation during async execution)
    struct WorkerTask {
        std::string workerID;
        int partitionId{0};
        uint64_t localTriangles{0};
        uint64_t rawEdges{0};
        long durationMs{0};
        std::future<bool> taskSucceeded;
    };
    std::vector<WorkerTask> tasks;
    tasks.reserve(rows.size());

    // Launch one async task per (worker, partition)
    for (const auto& w : allWorkers) {
        auto it = workerPartitionMap.find(w.workerID);
        if (it == workerPartitionMap.end()) continue;

        for (int pid : it->second) {
            tasks.push_back({w.workerID, pid, 0, 0, 0, {}});
            WorkerTask& task = tasks.back();
            task.taskSucceeded = std::async(
                std::launch::async,
                [&aggregation, graphId, snapshotId, w, pid, &masterIP, &task]() mutable -> bool {
                    long duration = 0;
                    uint64_t rawEdges = 0;
                    uint64_t localTriangles = 0;
                    bool ok = collectHistoryTriangleEdgesFromWorker(
                        graphId, snapshotId, w, pid, masterIP,
                        Conts::DEFAULT_THREAD_PRIORITY,
                        aggregation, duration, rawEdges, localTriangles);
                    task.localTriangles = localTriangles;
                    task.rawEdges       = rawEdges;
                    task.durationMs     = duration;
                    return ok;
                });
        }
    }

    if (tasks.empty()) {
        frontend_logger.error(
            "No runnable worker-partition tasks found for graph " +
            std::to_string(graphId) +
            ". Partition assignments may reference workers that are not currently registered");
        return result;
    }

    // Collect results
    int partitionsProcessed = 0;
    int partitionsFailed = 0;
    uint64_t totalLocalTriangles = 0;
    uint64_t totalRawEdges = 0;
    long maxWorkerDuration = 0;

    for (auto& task : tasks) {
        bool ok = task.taskSucceeded.get();
        if (ok) {
            partitionsProcessed++;
            totalLocalTriangles += task.localTriangles;
            totalRawEdges       += task.rawEdges;
            if (task.durationMs > maxWorkerDuration) maxWorkerDuration = task.durationMs;
            frontend_logger.info(
                "Worker " + task.workerID +
                " partition=" + std::to_string(task.partitionId) +
                " localTriangles=" + std::to_string(task.localTriangles) +
                " rawEdges=" + std::to_string(task.rawEdges) +
                " dur=" + std::to_string(task.durationMs) + "ms");
        } else {
            partitionsFailed++;
            frontend_logger.warn(
                "Worker " + task.workerID +
                " partition=" + std::to_string(task.partitionId) + " failed — skipped");
        }
    }

    if (partitionsProcessed == 0) {
        frontend_logger.error(
            "All distributed history triangle tasks failed for graph " +
            std::to_string(graphId) + " snapshot=" + std::to_string(snapshotId) +
            " (failed_tasks=" + std::to_string(partitionsFailed) + ")");
        return result;
    }

    // Finalize shard streams before reading from disk
    aggregation.finalizeShardStreams();

    // Count exact global triangles from all streamed partition edges (OpenMP used inside)
    auto [globalTriangleCount, uniqueEdges] =
        countTrianglesFromEncodedShards(aggregation.getShardPaths(),
                                        aggregation.getNodeCount());

    result.triangleCount       = globalTriangleCount;
    result.rawEdges            = totalRawEdges;
    result.uniqueEdges         = uniqueEdges;
    result.partitionsProcessed = partitionsProcessed;
    result.durationMs          = maxWorkerDuration;
    result.uniqueNodes         = aggregation.getNodeCount();

    frontend_logger.info(
        "[histrian] local_hint=" + std::to_string(totalLocalTriangles) +
        " global=" + std::to_string(globalTriangleCount) +
        " total=" + std::to_string(result.triangleCount) +
        " partitions=" + std::to_string(partitionsProcessed));

    return result;
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
        return nullptr;
    }

    std::string data(FRONTEND_DATA_LENGTH + 1, '\0');
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
        std::string line = JasmineGraphFrontEndCommon::readAndProcessInput(connFd, data.data(), failCnt);
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
                                     numberOfPartitions, sqlite, &loop_exit, false);
        } else if (line.compare(ADD_STREAM_KAFKA_CSV) == 0) {
            if (!workerClientsInitialized) {
                workerClients = getWorkerClients(sqlite);
                workerClientsInitialized = true;
            }
            add_stream_kafka_command(connFd, kafka_server_IP, configs, kstream, input_stream_handler, workerClients,
                                     numberOfPartitions, sqlite, &loop_exit, true);
        } else if (line.compare(KTOP) == 0) {
            kafka_topics_command(connFd, sqlite, &loop_exit);
        } else if (line.compare(ADD_STREAM_HDFS) == 0) {
            addStreamHDFSCommand(masterIP, connFd, hdfsServerIp, input_stream_handler, numberOfPartitions, sqlite,
                                 &loop_exit);
        } else if (line.compare(SEND_GRAPH_HDFS) == 0) {
            send_graph_hdfs_command(masterIP, connFd, sqlite, &loop_exit);
        } else if (line.compare(CONSTRUCT_KG) == 0) {
            JasmineGraphFrontEnd::constructKGStreamHDFSCommand(masterIP, connFd, numberOfPartitions, sqlite,
                                                               &loop_exit);
        } else if (line.compare(STOP_CONSTRUCT_KG) == 0) {
            JasmineGraphFrontEnd::stop_graph_streaming(connFd, &loop_exit);
        } else if (line.compare(0, STOP_STREAM_KAFKA.length(), STOP_STREAM_KAFKA) == 0 &&
                   (line.length() == STOP_STREAM_KAFKA.length() ||
                    std::isspace(static_cast<unsigned char>(line[STOP_STREAM_KAFKA.length()])))) {
            std::string topicName;

            if (line.length() > STOP_STREAM_KAFKA.length()) {
                topicName = Utils::trim_copy(line.substr(STOP_STREAM_KAFKA.length()));
            }

            if (topicName.empty()) {
                string topicMsg = "Enter Kafka topic to stop streaming: ";
                write(connFd, topicMsg.c_str(), topicMsg.length());
                write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());

                char topicBuffer[FRONTEND_DATA_LENGTH + 1];
                memset(topicBuffer, 0, FRONTEND_DATA_LENGTH + 1);
                read(connFd, topicBuffer, FRONTEND_DATA_LENGTH);
                topicName = Utils::trim_copy(string(topicBuffer));
            }

            if (topicName.empty()) {
                string errorMsg = "Error: Invalid topic name";
                write(connFd, errorMsg.c_str(), errorMsg.length());
                write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
            } else {
                stop_stream_kafka_command(connFd, topicName, &loop_exit);
            }
        } else if (line.compare(RMGR) == 0) {
            remove_graph_command(masterIP, connFd, sqlite, &loop_exit);
        } else if (line.compare(PROCESS_DATASET) == 0) {
            process_dataset_command(connFd, &loop_exit);
        } else if (line.compare(TRIANGLES) == 0) {
            triangles_command(masterIP, connFd, sqlite, perfSqlite, jobScheduler, &loop_exit);
        } else if (line.compare(STREAMING_TRIANGLES) == 0) {
            streaming_triangles_command(masterIP, connFd, jobScheduler, &loop_exit, numberOfPartitions,
                                        &JasmineGraphFrontEnd::strian_exit);
        } else if (line.compare(HISTORY_TRIANGLE) == 0 || line.compare("htria") == 0 ||
                   line.compare("hstria") == 0) {
            history_triangle_command(connFd, sqlite, &loop_exit, masterIP);
        } else if (line.compare(HISTORY_TRIANGLE_TIMESTAMP) == 0) {
            history_triangle_timestamp_command(connFd, sqlite, &loop_exit, masterIP);
        } else if (line.compare(HISTORY_PAGERANK) == 0) {
            history_pagerank_command(connFd, sqlite, &loop_exit, masterIP);
        } else if (line.compare(HISTORY_PAGERANK_TIMESTAMP) == 0) {
            history_pagerank_timestamp_command(connFd, sqlite, &loop_exit, masterIP);
        } else if (line.compare(HISTORY_BFS) == 0) {
            history_bfs_command(connFd, sqlite, &loop_exit);
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
        } else if (line.compare(TEMPORAL_QUERY) == 0) {
            temporal_query_command(connFd, sqlite, &loop_exit);
        } else if (line.compare(TEMPORAL_SNAPSHOT) == 0) {
            temporal_snapshot_command(connFd, sqlite, &loop_exit);
        } else if (line.compare(TEMPORAL_RANGE) == 0) {
            temporal_range_command(connFd, sqlite, &loop_exit);
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
    return nullptr;
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

    memset((char *)&svrAdd, 0, sizeof(svrAdd));

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
        pthread_create(&pt, nullptr, frontendservicesesion, sessionargs);
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

static void writeSocketResultOrEmpty(int connectionFd, const std::string &result, bool *loop_exit_p) {
    if (result.size() == 0) {
        int result_wr = write(connectionFd, EMPTY.c_str(), EMPTY.length());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }

        result_wr =
            write(connectionFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
    } else {
        int result_wr = write(connectionFd, result.c_str(), result.length());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
        }
    }
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
                } else if (std::stoi(j->second) == Conts::GRAPH_STATUS::NON_OPERATIONAL) {
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
    writeSocketResultOrEmpty(connFd, result, loop_exit_p);
    return;
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
    memset(graphIdResponse, 0, FRONTEND_DATA_LENGTH + 1);
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
    memset(query, 0, FRONTEND_DATA_LENGTH + 1);
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
    std::string graphIdResponse = read_frontend_socket_value(connFd);
    std::string user_res_1 = graphIdResponse;
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
    std::string queryString = read_frontend_socket_value(connFd);
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
    string name = "";
    string path = "";

    std::time_t time = chrono::system_clock::to_time_t(chrono::system_clock::now());
    string uploadStartTime = ctime(&time);
    std::string gData = read_frontend_socket_value(connFd);
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
    string name = "";
    string path = "";
    string partitionCount = "";

    std::time_t time = chrono::system_clock::to_time_t(chrono::system_clock::now());
    string uploadStartTime = ctime(&time);
    std::string gData = read_frontend_socket_value(connFd);
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
    memset(type, 0, FRONTEND_GRAPH_TYPE_LENGTH + 1);
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
    string name = "";
    string edgeListPath = "";
    string attributeListPath = "";
    string attrDataType = "";

    std::time_t time = chrono::system_clock::to_time_t(chrono::system_clock::now());
    string uploadStartTime = ctime(&time);
    std::string gData = read_frontend_socket_value(connFd);
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
    string name = "";
    string path = "";
    std::string graphID = read_frontend_socket_value(connFd);
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

    string name = "";
    string path = "";

    std::time_t time = chrono::system_clock::to_time_t(chrono::system_clock::now());
    string uploadStartTime = ctime(&time);
    std::string gData = read_frontend_socket_value(connFd);
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
                                     SQLiteDBInterface *sqlite, bool *loop_exit_p,
                                     bool isCsvMode) {
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
    string kafka_server_IP_from_file;

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
        direction = sqlite->getDirectionByGraphID(graphId);

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
            {"metadata.broker.list", kafka_server_IP},
            {"group.id", group_id},
            {"auto.offset.reset", "earliest"},
            {"enable.auto.commit", "false"},
        };
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
        memset(file_path, 0, FRONTEND_DATA_LENGTH + 1);
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
                        kafka_server_IP_from_file = vec2.at(1);
                    } else {
                        kafka_server_IP_from_file = " ";
                    }
                }
            }
        }

        if (!kafka_server_IP_from_file.empty()) {
            kafka_server_IP = kafka_server_IP_from_file;
        }

        std::string unique_group_id = "knnect_" + graphId + "_" + std::to_string(std::time(nullptr));
        configs = {
            {"metadata.broker.list", kafka_server_IP},
            {"group.id", unique_group_id},
            {"auto.offset.reset", "earliest"},
            {"enable.auto.commit", "false"}  // Disable auto-commit to always read from beginning
        };
    }

    std::string activeCommand = isCsvMode ? ADD_STREAM_KAFKA_CSV : ADD_STREAM_KAFKA;
    frontend_logger.info("Start serving `" + activeCommand + "` command");
    string message = isCsvMode ? "send kafka topic name (CSV edges)" : "send kafka topic name";
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
    memset(topic_name, 0, FRONTEND_DATA_LENGTH + 1);
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
    // Create the KafkaConnector object.
    kstream = new KafkaConnector(configs);
    // Subscribe to the Kafka topic.
    kstream->Subscribe(topic_name_s);
    bool isNewGraph = (existingGraph != "y");
    // Convert graphId string to integer
    int graphIdInt = stoi(graphId);

    // Register the stream in the global registry
    StreamRegistry &registry = StreamRegistry::getInstance();
    std::string userId = "user_" + std::to_string(connFd);  // Use connection FD as simple user identifier
    if (!registry.registerStream(graphIdInt, topic_name_s, connFd, kstream, userId)) {
        frontend_logger.error("Failed to register stream - graphId " + graphId + " may already be streaming");
        string errorMsg = "Error: Stream for this graph ID is already active";
        write(connFd, errorMsg.c_str(), errorMsg.length());
        write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        delete kstream;
        return;
    }

    // Get the stop flag from the registry (lookup by graph ID)
    auto streamMetadata = registry.getStreamByGraphId(graphIdInt);
    if (!streamMetadata) {
        frontend_logger.error("Failed to retrieve stream metadata for topic " + topic_name_s);
        string errorMsg = "Error: Failed to initialize stream metadata";
        write(connFd, errorMsg.c_str(), errorMsg.length());
        write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        registry.unregisterStream(graphIdInt);
        delete kstream;
        *loop_exit_p = true;
        return;
    }
    auto stopFlag = streamMetadata->stopFlag;

    // Create the StreamHandler object with the stop flag
    auto stream_handler = std::make_unique<StreamHandler>(
        kstream, numberOfPartitions, workerClients, sqlite, graphIdInt, direction == Conts::DIRECTED,
        spt::getPartitioner(partitionAlgo), stopFlag);

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
    if (input_stream_handler_thread.joinable()) {
        frontend_logger.warn("Detaching existing Kafka input stream handler thread before starting a new one");
        input_stream_handler_thread.detach();
    }
    input_stream_handler_thread = thread(&StreamHandler::listen_to_kafka_topic, std::move(stream_handler));

    // Update the stream registry with the new thread ID
    registry.updateStreamThreadId(graphIdInt, input_stream_handler_thread.get_id());
}

static bool writeSocketLine(int connectionFd, const std::string &message, bool *loop_exit_p) {
    int resultWr = write(connectionFd, message.c_str(), message.length());
    if (resultWr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return false;
    }

    resultWr = write(connectionFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (resultWr < 0) {
        frontend_logger.error("Error writing to socket");
        *loop_exit_p = true;
        return false;
    }

    return true;
}

static std::string readTrimmedSocketInput(int connectionFd) {
    std::string input;
    input.resize(FRONTEND_DATA_LENGTH);
    if (int bytesRead = read(connectionFd, &input[0], FRONTEND_DATA_LENGTH); bytesRead > 0) {
        input.resize(static_cast<size_t>(bytesRead));
    } else {
        input.clear();
    }

    return Utils::trim_copy(input);
}

static bool sendClientErrorAndExit(int connectionFd, const std::string &logMessage, const std::string &clientMessage,
                                   bool *loop_exit_p) {
    frontend_logger.error(logMessage);
    write(connectionFd, clientMessage.c_str(), clientMessage.length());
    write(connectionFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    *loop_exit_p = true;
    return false;
}

static bool requestGraphIdAndValidate(int connectionFd, SQLiteDBInterface *sqlite, std::string &graphId,
                                      bool *loop_exit_p) {
    if (!writeSocketLine(connectionFd, "Graph ID:", loop_exit_p)) {
        return false;
    }

    graphId = readTrimmedSocketInput(connectionFd);
    frontend_logger.info("Graph ID received: " + graphId);

    std::string graphQuery = "SELECT idgraph, name FROM graph WHERE idgraph = '" + graphId + "'";
    if (std::vector<std::vector<std::pair<std::string, std::string>>> graphResults = sqlite->runSelect(graphQuery);
        graphResults.empty()) {
        return sendClientErrorAndExit(connectionFd, "Graph not found: " + graphId, "Graph not found", loop_exit_p);
    }

    return true;
}

static bool requestHdfsServerConfig(int connectionFd, std::string &hdfsServerIp, std::string &hdfsPort,
                                    bool *loop_exit_p) {
    if (!writeSocketLine(connectionFd, "Do you want to use the default HDFS server(y/n)?", loop_exit_p)) {
        return false;
    }

    std::string userRes = readTrimmedSocketInput(connectionFd);
    std::transform(userRes.begin(), userRes.end(), userRes.begin(), ::tolower);

    if (userRes == "y") {
        hdfsServerIp = Utils::getJasmineGraphProperty("org.jasminegraph.server.streaming.hdfs.host");
        hdfsPort = Utils::getJasmineGraphProperty("org.jasminegraph.server.streaming.hdfs.port");
    } else {
        if (std::string configMsg =
                "Send the file path to the HDFS configuration file."
                " This file needs to be in some directory location"
                " that is accessible for JasmineGraph master";
            !writeSocketLine(connectionFd, configMsg, loop_exit_p)) {
            return false;
        }

        std::string filePath = readTrimmedSocketInput(connectionFd);
        frontend_logger.info("Reading HDFS configuration file: " + filePath);
        parseHdfsConfigFile(filePath, hdfsServerIp, hdfsPort);

        if (hdfsServerIp.empty()) {
            frontend_logger.error("HDFS server IP is empty.");
        }
        if (hdfsPort.empty()) {
            frontend_logger.error("HDFS server port is empty.");
        }
    }

    frontend_logger.info("HDFS Server: " + hdfsServerIp + ":" + hdfsPort);
    return true;
}

static bool requestHdfsDestinationPath(int connectionFd, std::string &hdfsDestinationFilePath, bool *loop_exit_p) {
    if (!writeSocketLine(connectionFd, "HDFS destination file path:", loop_exit_p)) {
        return false;
    }

    hdfsDestinationFilePath = readTrimmedSocketInput(connectionFd);
    frontend_logger.info("HDFS destination file path: " + hdfsDestinationFilePath);

    if (hdfsDestinationFilePath.empty()) {
        return sendClientErrorAndExit(connectionFd, "HDFS destination file path is empty",
                                      "Invalid HDFS destination file path", loop_exit_p);
    }

    return true;
}

static bool loadWorkerPartitions(SQLiteDBInterface *sqlite, const std::string &graphId,
                                 std::map<std::string, std::vector<std::string>, std::less<>> &workerPartitionMap,
                                 std::map<std::string, Utils::worker, std::less<>> &workerMap,
                                 int connectionFd, bool *loop_exit_p) {
    std::string partitionQuery =
        "SELECT DISTINCT worker_idworker, partition_idpartition "
        "FROM worker_has_partition INNER JOIN worker ON worker_has_partition.worker_idworker=worker.idworker "
        "WHERE partition_graph_idgraph=" + graphId;
    std::vector<std::vector<std::pair<std::string, std::string>>> partitionResults = sqlite->runSelect(partitionQuery);

    if (partitionResults.empty()) {
        return sendClientErrorAndExit(connectionFd, "No partitions found for graph ID: " + graphId,
                                      "Graph not found or has no partitions", loop_exit_p);
    }

    for (const auto &row : partitionResults) {
        workerPartitionMap[row[0].second].push_back(row[1].second);
    }

    std::vector<Utils::worker> workerList = Utils::getWorkerList(sqlite);
    for (const auto &worker : workerList) {
        workerMap[worker.workerID] = worker;
    }

    return true;
}

static std::string trimTrailingSlashes(std::string path) {
    while (path.size() > 1 && path.back() == '/') {
        path.pop_back();
    }
    return path;
}

static std::string getParentDirectory(const std::string &path) {
    std::string normalized = trimTrailingSlashes(path);
    size_t pos = normalized.find_last_of('/');
    if (pos == std::string::npos) {
        return std::string();
    }
    if (pos == 0) {
        return std::string("/");
    }
    return normalized.substr(0, pos);
}

static bool prepareHdfsDestination(HDFSConnector &hdfsConnector, const std::string &destinationPath,
                                   std::string &shardDirectory, int connectionFd, bool *loop_exit_p) {
    shardDirectory = trimTrailingSlashes(destinationPath) + "_shards";

    std::string mergedParentDirectory = getParentDirectory(destinationPath);
    if (!mergedParentDirectory.empty() && !hdfsConnector.createDirectory(mergedParentDirectory)) {
        return sendClientErrorAndExit(connectionFd,
                                      "Failed to create parent directory for destination file: " +
                                          mergedParentDirectory,
                                      "Failed to create parent directory for destination file", loop_exit_p);
    }

    if (!hdfsConnector.createDirectory(shardDirectory)) {
        return sendClientErrorAndExit(connectionFd, "Failed to create HDFS shard directory: " + shardDirectory,
                                      "Failed to create HDFS shard directory", loop_exit_p);
    }

    return true;
}

using HdfsEndpoint = std::pair<std::string, std::string>;

static bool exportPartitionShard(const std::string &masterIP, const std::string &graphId, const std::string &workerID,
                                 const std::string &partitionId, const Utils::worker &currentWorker,
                                 const HdfsEndpoint &hdfsEndpoint,
                                 const std::string &shardPath) {
    std::string host = currentWorker.hostname;
    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }
    int workerPort = std::stoi(currentWorker.port);

    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        frontend_logger.error("Cannot create socket for worker " + workerID);
        return false;
    }

    struct hostent hostEntry;
    struct hostent *server = nullptr;
    int hostErrno = 0;
    std::string hostBuffer(HOSTNAME_BUFFER_SIZE, '\0');
    if (int hostLookupResult = gethostbyname_r(host.c_str(), &hostEntry, hostBuffer.data(), hostBuffer.size(), &server,
                                               &hostErrno);
        hostLookupResult != 0 || server == nullptr) {
        frontend_logger.error("No host named " + host);
        close(sockfd);
        return false;
    }

    struct sockaddr_in serv_addr;
    memset((char *)&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    memcpy(&serv_addr.sin_addr.s_addr, server->h_addr, server->h_length);
    serv_addr.sin_port = htons(workerPort);

    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        frontend_logger.error("Error connecting to worker " + workerID + " at " + host);
        close(sockfd);
        return false;
    }

    std::string data(INSTANCE_LONG_DATA_LENGTH + 1, '\0');
    bool success = Utils::performHandshake(sockfd, data.data(), INSTANCE_DATA_LENGTH, masterIP) &&
                   Utils::sendExpectResponse(sockfd, data.data(), INSTANCE_LONG_DATA_LENGTH,
                                             JasmineGraphInstanceProtocol::SEND_EDGES_TO_HDFS,
                                             JasmineGraphInstanceProtocol::OK) &&
                   Utils::sendExpectResponse(sockfd, data.data(), INSTANCE_LONG_DATA_LENGTH,
                                             graphId,
                                             JasmineGraphInstanceProtocol::SEND_PARTITION_ID) &&
                   Utils::sendExpectResponse(sockfd, data.data(), INSTANCE_LONG_DATA_LENGTH,
                                             partitionId,
                                             JasmineGraphInstanceProtocol::OK) &&
                   Utils::sendExpectResponse(sockfd, data.data(), INSTANCE_LONG_DATA_LENGTH,
                                             hdfsEndpoint.first,
                                             JasmineGraphInstanceProtocol::OK) &&
                   Utils::sendExpectResponse(sockfd, data.data(), INSTANCE_LONG_DATA_LENGTH,
                                             hdfsEndpoint.second,
                                             JasmineGraphInstanceProtocol::OK);

    if (success) {
        success = Utils::send_str_wrapper(sockfd, shardPath);
    }

    std::string status;
    if (success) {
        status = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_LONG_DATA_LENGTH);
        success = (status == JasmineGraphInstanceProtocol::OK);
    }

    Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
    close(sockfd);
    return success;
}

struct HdfsShardExportResult {
    int totalPartitions = 0;
    int processedPartitions = 0;
    bool writeError = false;
    std::vector<std::pair<int, std::string>> shardPaths;
};

struct WorkerPartitionExportContext {
    std::string masterIP;
    std::string graphId;
    HdfsEndpoint hdfsEndpoint;
    std::string workerID;
    std::vector<std::string> partitions;
    Utils::worker currentWorker;
    std::string shardDirectory;
    HdfsShardExportResult *result = nullptr;
    std::mutex *exportResultMutex = nullptr;
};

static void exportWorkerPartitions(const WorkerPartitionExportContext &context) {
    auto buildShardPath = [&context](const std::string &localWorkerID,
                                                      const std::string &partitionID) {
        std::string basePath = context.shardDirectory;
        std::string separator = (basePath == "/") ? "" : "/";
        return basePath + separator + "graph_" + context.graphId + "_worker_" + localWorkerID + "_partition_" +
               partitionID + ".txt";
    };

    for (const std::string &partitionId : context.partitions) {
        {
            std::lock_guard lock(*context.exportResultMutex);
            if (context.result->writeError) {
                return;
            }
        }

        std::string shardPath = buildShardPath(context.workerID, partitionId);
        if (bool success =
                exportPartitionShard(context.masterIP, context.graphId, context.workerID, partitionId,
                                     context.currentWorker, context.hdfsEndpoint, shardPath);
            !success) {
            frontend_logger.error("Worker " + context.workerID + " failed to write partition " + partitionId +
                                  " to HDFS path " + shardPath);
            std::lock_guard lock(*context.exportResultMutex);
            context.result->writeError = true;
            break;
        }

        frontend_logger.info("Worker " + context.workerID + " wrote partition " + partitionId +
                             " directly to HDFS path " + shardPath);
        std::lock_guard lock(*context.exportResultMutex);
        context.result->shardPaths.emplace_back(std::stoi(partitionId), shardPath);
        ++context.result->processedPartitions;
    }
}

static HdfsShardExportResult exportWorkerShards(
    const std::string &masterIP, const std::string &graphId, const std::string &hdfsServerIp,
    const std::string &hdfsPort, const std::string &shardDirectory,
    const std::map<std::string, std::vector<std::string>, std::less<>> &workerPartitionMap,
    const std::map<std::string, Utils::worker, std::less<>> &workerMap) {
    HdfsShardExportResult result;
    std::mutex exportResultMutex;
    const HdfsEndpoint hdfsEndpoint{hdfsServerIp, hdfsPort};

    std::vector<std::thread> exportThreads;
    exportThreads.reserve(workerPartitionMap.size());

    for (const auto &[workerID, partitions] : workerPartitionMap) {
        result.totalPartitions += static_cast<int>(partitions.size());

        auto workerIterator = workerMap.find(workerID);
        if (workerIterator == workerMap.end()) {
            frontend_logger.error("Worker " + workerID + " not found in worker list");
            result.writeError = true;
            continue;
        }

        WorkerPartitionExportContext exportContext{masterIP, graphId, hdfsEndpoint, workerID, partitions,
                               workerIterator->second, shardDirectory, &result,
                               &exportResultMutex};
        exportThreads.emplace_back(exportWorkerPartitions, std::move(exportContext));
    }

    for (auto &thread : exportThreads) {
        if (thread.joinable()) {
            thread.join();
        }
    }

    frontend_logger.info("Processed " + std::to_string(result.processedPartitions) + " out of " +
                         std::to_string(result.totalPartitions) + " partitions");
    return result;
}

static bool mergeShardsAndRespond(HDFSConnector &hdfsConnector, const std::string &hdfsDestinationFilePath,
                                  const std::string &shardDirectory, HdfsShardExportResult &exportResult,
                                  int connectionFd, bool *loop_exit_p) {
    if (exportResult.processedPartitions == 0) {
        return sendClientErrorAndExit(connectionFd, "No graph data collected for requested graph",
                                      "Failed to collect graph data", loop_exit_p);
    }

    if (exportResult.writeError || exportResult.processedPartitions != exportResult.totalPartitions) {
        return sendClientErrorAndExit(connectionFd, "Failed to write one or more graph shards to HDFS", ERROR,
                                      loop_exit_p);
    }

    std::sort(exportResult.shardPaths.begin(), exportResult.shardPaths.end(),
              [](const std::pair<int, std::string> &left, const std::pair<int, std::string> &right) {
                  return left.first < right.first;
              });

    std::vector<std::string> orderedShardPaths;
    orderedShardPaths.reserve(exportResult.shardPaths.size());
    for (const auto &[partitionId, shardPath] : exportResult.shardPaths) {
        orderedShardPaths.push_back(shardPath);
    }

    if (!hdfsConnector.concatenateFiles(orderedShardPaths, hdfsDestinationFilePath)) {
        return sendClientErrorAndExit(connectionFd,
                                      "Failed to concatenate worker shards into merged HDFS file: " +
                                          hdfsDestinationFilePath,
                                      ERROR, loop_exit_p);
    }

    if (!hdfsConnector.deletePath(shardDirectory, true)) {
        return sendClientErrorAndExit(connectionFd,
                                      "Failed to delete HDFS shard directory after merge: " + shardDirectory,
                                      ERROR, loop_exit_p);
    }

    frontend_logger.info("Successfully merged graph shards into destination file " + hdfsDestinationFilePath +
                         " and deleted shard directory " + shardDirectory);
    return writeSocketLine(connectionFd, DONE, loop_exit_p);
}

static void send_graph_hdfs_command_impl(const std::string &masterIP, int connectionFd,
                                         SQLiteDBInterface *sqlite, bool *loop_exit_p) {
    frontend_logger.info("Save graph to HDFS command received");

    std::string graphId;
    if (!requestGraphIdAndValidate(connectionFd, sqlite, graphId, loop_exit_p)) {
        return;
    }

    std::string hdfsServerIp;
    std::string hdfsPort;
    if (!requestHdfsServerConfig(connectionFd, hdfsServerIp, hdfsPort, loop_exit_p)) {
        return;
    }

    std::string hdfsDestinationFilePath;
    if (!requestHdfsDestinationPath(connectionFd, hdfsDestinationFilePath, loop_exit_p)) {
        return;
    }

    std::map<std::string, std::vector<std::string>, std::less<>> workerPartitionMap;
    std::map<std::string, Utils::worker, std::less<>> workerMap;
    if (!loadWorkerPartitions(sqlite, graphId, workerPartitionMap, workerMap, connectionFd, loop_exit_p)) {
        return;
    }

    auto hdfsConnector = std::make_unique<HDFSConnector>(hdfsServerIp, hdfsPort);
    std::string shardDirectory;
    if (!prepareHdfsDestination(*hdfsConnector, hdfsDestinationFilePath, shardDirectory, connectionFd, loop_exit_p)) {
        return;
    }

    HdfsShardExportResult exportResult = exportWorkerShards(masterIP, graphId, hdfsServerIp, hdfsPort,
                                                             shardDirectory, workerPartitionMap, workerMap);

    if (!mergeShardsAndRespond(*hdfsConnector, hdfsDestinationFilePath, shardDirectory, exportResult,
                               connectionFd, loop_exit_p)) {
        return;
    }
}

static void send_graph_hdfs_command(const std::string &masterIP, int connectionFd, SQLiteDBInterface *sqlite,
                                    bool *loop_exit_p) {
    send_graph_hdfs_command_impl(masterIP, connectionFd, sqlite, loop_exit_p);
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
        memset(filePath, 0, FRONTEND_DATA_LENGTH + 1);
        read(connFd, filePath, FRONTEND_DATA_LENGTH);
        std::string filePathS(filePath);
        filePathS = Utils::trim_copy(filePathS);

        frontend_logger.info("Reading HDFS configuration file: " + filePathS);

        parseHdfsConfigFile(filePathS, hdfsServerIp, hdfsPort);
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
    memset(isEdgeListTypeRes, 0, FRONTEND_DATA_LENGTH + 1);
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
    memset(isDirectedRes, 0, FRONTEND_DATA_LENGTH + 1);
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
        R"(INSERT INTO graph (name, upload_path, upload_start_time, upload_end_time, graph_status_idgraph_status, )"
        R"(vertexcount, centralpartitioncount, edgecount, is_directed) VALUES(")" +
        hdfsFilePathS + R"(", ")" + path + R"(", ")" + uploadStartTime + R"(", "", ")" +
        std::to_string(Conts::GRAPH_STATUS::NON_OPERATIONAL) + R"(", "", "", "", ")" +
        (directed ? "TRUE" : "FALSE") + R"("))";

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

struct KGStreamingTaskContext {
    JasmineGraphServer::worker designatedWorker;
    std::string masterIP;
    int graphId = 0;
    int numberOfPartitions = 0;
    std::string hdfsServerIp;
    std::string hdfsPort;
    std::string hostnamePort;
    std::string llmInferenceEngine;
    std::string llm;
    std::string chunkSize;
    std::string hdfsFilePath;
    bool graphExists = false;
    SQLiteDBInterface *sqlite = nullptr;
    std::shared_ptr<std::atomic<bool>> stopFlag;
};

static void runKGStreamingTask(KGStreamingTaskContext context) {
    frontend_logger.info("Starting streaming thread for GraphID: " + std::to_string(context.graphId));
    JasmineGraphFrontEnd::kgConstructionRates[context.graphId] = std::make_shared<KGConstructionRate>();
    JasmineGraphFrontEnd::kgConstructionRates[context.graphId]->bytesPerSecond = 0.0;
    JasmineGraphFrontEnd::kgConstructionRates[context.graphId]->triplesPerSecond = 0.0;

    bool success = Pipeline::streamGraphToDesignatedWorker(
        context.designatedWorker.hostname, context.designatedWorker.port, context.masterIP,
        std::to_string(context.graphId), context.numberOfPartitions, context.hdfsServerIp, context.hdfsPort,
        context.hostnamePort, context.llmInferenceEngine, context.llm, context.chunkSize, context.hdfsFilePath,
        context.graphExists, context.sqlite, context.stopFlag,
                JasmineGraphFrontEnd::kgConstructionRates[context.graphId]);

    if (!success) {
        frontend_logger.error("Streaming to worker failed for GraphID: " + std::to_string(context.graphId));
    }

    std::time_t endTime = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::string uploadEndTimeBuffer(CTIME_BUFFER_SIZE, '\0');
    std::string uploadEndTime = ctime_r(&endTime, uploadEndTimeBuffer.data());

    std::string sqlStatementUpdateEndTime =
        "UPDATE graph SET upload_end_time = \"" + uploadEndTime + "\" WHERE idgraph = " +
        std::to_string(context.graphId);
    context.sqlite->runInsert(sqlStatementUpdateEndTime);

    frontend_logger.info("Async streaming finished for GraphID: " + std::to_string(context.graphId));
}

static bool promptAndReadInput(int connectionFd, const std::string &prompt, std::string &response, bool *loop_exit_p) {
    if (!writeSocketLine(connectionFd, prompt, loop_exit_p)) {
        return false;
    }
    response = readTrimmedSocketInput(connectionFd);
    return true;
}

static bool requestKgHdfsEndpoint(int connectionFd, std::string &hdfsServerIp, std::string &hdfsPort,
                                    bool *loop_exit_p) {
    std::string userRes;
    if (!promptAndReadInput(connectionFd, "Do you want to use the default HDFS server(y/n)?", userRes, loop_exit_p)) {
        return false;
    }

    std::transform(userRes.begin(), userRes.end(), userRes.begin(), ::tolower);
    if (userRes == "y") {
        hdfsServerIp = Utils::getJasmineGraphProperty("org.jasminegraph.server.streaming.hdfs.host");
        hdfsPort = Utils::getJasmineGraphProperty("org.jasminegraph.server.streaming.hdfs.port");
    } else {
        if (!promptAndReadInput(connectionFd, "HDFS Server IP:", hdfsServerIp, loop_exit_p)) {
            return false;
        }
        if (!promptAndReadInput(connectionFd, "HDFS Server Port:", hdfsPort, loop_exit_p)) {
            return false;
        }
    }

    frontend_logger.info("HDFS Server IP:" + hdfsServerIp);
    frontend_logger.info("HDFS Server Port:" + hdfsPort);
    if (hdfsServerIp.empty()) {
        frontend_logger.error("HDFS server IP is empty.");
    }
    if (hdfsPort.empty()) {
        frontend_logger.error("HDFS server port is empty.");
    }
    return true;
}

static bool requestKgHdfsPathAndValidate(int connectionFd, const std::string &hdfsServerIp, const std::string &hdfsPort,
                                         std::string &hdfsFilePath, std::unique_ptr<HDFSConnector> &hdfsConnector,
                                         bool *loop_exit_p) {
    if (!promptAndReadInput(connectionFd, "HDFS file path: ", hdfsFilePath, loop_exit_p)) {
        return false;
    }

    hdfsConnector = std::make_unique<HDFSConnector>(hdfsServerIp, hdfsPort);
    if (!hdfsConnector->isPathValid(hdfsFilePath)) {
        frontend_logger.error("Invalid HDFS file path: " + hdfsFilePath);
        writeSocketLine(connectionFd, "The provided HDFS path is invalid.", loop_exit_p);
        *loop_exit_p = true;
        return false;
    }

    return true;
}

static bool requestKgLlmConfiguration(int connectionFd, std::string &hostnamePort, std::string &llmInferenceEngine,
                                      std::string &llm, bool *loop_exit_p) {
    if (!promptAndReadInput(connectionFd, "LLM runner hostname:port: ", hostnamePort, loop_exit_p)) {
        return false;
    }
    frontend_logger.info("Recieved LLM runnners: " + hostnamePort);

    if (!promptAndReadInput(connectionFd, "LLM inference engine? ollama/vllm? ", llmInferenceEngine, loop_exit_p)) {
        return false;
    }
    frontend_logger.info("received Inference Engine: " + llmInferenceEngine);

    if (!promptAndReadInput(connectionFd, "What is the LLM you want to use?:", llm, loop_exit_p)) {
        return false;
    }
    frontend_logger.info("Received LLM " + llm);
    return true;
}

static bool validateKgModelAvailability(int connectionFd, const std::string &hostnamePort,
                                        const std::string &llmInferenceEngine, const std::string &llm,
                                        bool *loop_exit_p) {
    std::vector<std::string> llmServers = Utils::getUniqueLLMRunners(hostnamePort);

    for (const auto &llmServer : llmServers) {
        std::string endpointPath;
        if (llmInferenceEngine == "ollama") {
            endpointPath = "api/tags";
        } else if (llmInferenceEngine == "vllm") {
            endpointPath = "/v1/models";
        } else {
            frontend_logger.error("Unknown inference engine: " + llmInferenceEngine);
            writeSocketLine(connectionFd, "Unknown inference engine '" + llmInferenceEngine + "'", loop_exit_p);
            *loop_exit_p = true;
            return false;
        }

        std::string url = Utils::normalizeURL(llmServer, endpointPath);
        frontend_logger.info("Final LLM endpoint: " + url);

        // Initialize CURL handle
        CURL *curl = curl_easy_init();
        if (curl == nullptr) {
            frontend_logger.error("Failed to initialize CURL for " + llmServer);
            writeSocketLine(connectionFd, "Could not initialize HTTP client for model check.", loop_exit_p);
            *loop_exit_p = true;
            return false;
        }

        std::string response;

        // TLS settings — set immediately after init for static analysis visibility
        long sslver = CURL_SSLVERSION_TLSv1_2;
#if defined(CURL_SSLVERSION_MAX_TLSv1_3)
        sslver |= CURL_SSLVERSION_MAX_TLSv1_3;
#endif

        // We want to enforce TLS 1.2 minimum for security, and allow 1.3 if available.
        // But we don't want to allow older versions
        curl_easy_setopt(curl, CURLOPT_SSLVERSION, sslver);  // NOSONAR

        // only if you are actually using an HTTPS proxy; otherwise you can remove it
        curl_easy_setopt(curl, CURLOPT_PROXY_SSLVERSION, sslver);

        // certificate verification
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);

        // Request settings
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, 5L);

        // Execute request
        CURLcode res = curl_easy_perform(curl);
        curl_easy_cleanup(curl);

        if (res != CURLE_OK) {
            frontend_logger.error("Failed to reach " + llmInferenceEngine + " server at " + llmServer +
                                  " curl error: " + std::string(curl_easy_strerror(res)));
            writeSocketLine(connectionFd, "Could not connect to " + llmInferenceEngine + " server.", loop_exit_p);
            *loop_exit_p = true;
            return false;
        }

        bool modelFound = false;
        if (llmInferenceEngine == "ollama") {
            modelFound = response.find(R"("name":")" + llm + "\"") != std::string::npos;
        } else {  // vllm
            frontend_logger.info(response);
            modelFound = response.find(R"("id\":")" + llm + "\"") != std::string::npos;
        }

        if (!modelFound) {
            frontend_logger.error("Model '" + llm + "' not found on " + llmInferenceEngine + " server.");
            writeSocketLine(connectionFd,
                            "Model '" + llm + "' not available on " + llmInferenceEngine + " server.",
                            loop_exit_p);
            *loop_exit_p = true;
            return false;
        }

        frontend_logger.info("Verified model '" + llm + "' exists on " + llmInferenceEngine + " server.");
    }

    return true;
}

static bool requestKgChunkSize(int connectionFd, std::string &chunkSize, bool *loop_exit_p) {
    if (!promptAndReadInput(connectionFd, "chunk size (Bytes):", chunkSize, loop_exit_p)) {
        return false;
    }
    frontend_logger.info("Received engine chunk size: " + chunkSize);
    return true;
}

static int insertNewKgGraph(SQLiteDBInterface *sqlite, const std::string &hdfsFilePath, const std::string &path,
                            const std::string &uploadStartTime, double_t totalFileSize) {
    std::string insertQuery =
        "INSERT INTO graph (name, upload_path, upload_start_time, "
        "upload_end_time, graph_status_idgraph_status, "
        "vertexcount, centralpartitioncount, edgecount, is_directed , "
        "file_size_bytes ) VALUES(\"" +
        hdfsFilePath + R"(", ")" + path + R"(", ")" + uploadStartTime + R"(", "", ")" +
        std::to_string(Conts::GRAPH_STATUS::NON_OPERATIONAL) + R"(", "", "", "", "TRUE", ")" +
        to_string(totalFileSize) + R"(");)";

    int newGraphId = sqlite->runInsert(insertQuery);
    frontend_logger.info("Constructing new Knowledge Graph with new GraphID: " + to_string(newGraphId));
    return newGraphId;
}

struct KGGraphResolveContext {
    int connectionFd = -1;
    SQLiteDBInterface *sqlite = nullptr;
    std::string hdfsFilePath;
    std::string path;
    std::string uploadStartTime;
    double_t totalFileSize = 0;
    bool *loop_exit_p = nullptr;
};

static bool resolveKgGraphId(const KGGraphResolveContext &context, int &newGraphID, bool &graphExists) {
    std::string checkQuery = "SELECT idgraph FROM graph WHERE upload_path = \"" + context.path + "\";";
    if (auto queryResults = context.sqlite->runSelect(checkQuery); !queryResults.empty() && !queryResults[0].empty()) {
        int existingId = std::stoi(queryResults[0][0].second);
        frontend_logger.info("Graph already exists with ID: " + std::to_string(existingId));

        std::string resumeResponse;
        if (!promptAndReadInput(context.connectionFd,
                                "There exists a graph with the file path, would you like to resume?",
                                resumeResponse, context.loop_exit_p)) {
            return false;
        }

        if (resumeResponse == "y") {
            std::string resumeGraphId;
            if (!promptAndReadInput(context.connectionFd, "Graph Id to resume?", resumeGraphId,
                                    context.loop_exit_p)) {
                return false;
            }

            newGraphID = stoi(resumeGraphId);
            graphExists = true;
            frontend_logger.info("Resuming Knowledge Graph construction from GraphID: " + to_string(newGraphID));
            return true;
        }
    }

    newGraphID = insertNewKgGraph(context.sqlite, context.hdfsFilePath, context.path, context.uploadStartTime,
                                  context.totalFileSize);
    graphExists = false;
    return true;
}

static void launchKgStreamingThread(KGStreamingTaskContext taskContext) {
    JasmineGraphServer::worker designatedWorker = taskContext.designatedWorker;
    auto stopFlag = std::make_shared<std::atomic<bool>>(false);
    {
        std::lock_guard lock(threadMapMutex);
        stopFlags[taskContext.graphId] = stopFlag;
    }

    taskContext.designatedWorker = designatedWorker;
    taskContext.stopFlag = stopFlag;
    std::thread streamingThread(runKGStreamingTask, taskContext);

    {
        std::lock_guard lock(threadMapMutex);
        activeStreamThreads[taskContext.graphId] = streamingThread.get_id();
    }

    streamingThread.detach();
}

bool JasmineGraphFrontEnd::constructKGStreamHDFSCommand(const std::string &masterIP, int connectionFd,
                                    int numberOfPartitions, SQLiteDBInterface *sqlite, bool *loop_exit_p) {
    std::string hdfsPort;
    std::string hdfsServerIp;
    if (!requestKgHdfsEndpoint(connectionFd, hdfsServerIp, hdfsPort, loop_exit_p)) {
        return false;
    }

    std::string hdfsFilePath;
    std::unique_ptr<HDFSConnector> hdfsConnector;
    if (!requestKgHdfsPathAndValidate(connectionFd, hdfsServerIp, hdfsPort, hdfsFilePath, hdfsConnector, loop_exit_p)) {
        return false;
    }

    std::string hostnamePort;
    std::string llmInferenceEngine;
    std::string llm;
    if (!requestKgLlmConfiguration(connectionFd, hostnamePort, llmInferenceEngine, llm, loop_exit_p)) {
        return false;
    }

    if (!validateKgModelAvailability(connectionFd, hostnamePort, llmInferenceEngine, llm, loop_exit_p)) {
        return false;
    }

    std::string chunkSize;
    if (!requestKgChunkSize(connectionFd, chunkSize, loop_exit_p)) {
        return false;
    }

    std::string path = "hdfs:" + hdfsFilePath;
    double_t totalFileSize = hdfsGetPathInfo(hdfsConnector->getFileSystem(), hdfsFilePath.c_str())->mSize;
    std::time_t now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::string uploadStartTimeBuffer(CTIME_BUFFER_SIZE, '\0');
    std::string uploadStartTime = ctime_r(&now, uploadStartTimeBuffer.data());
    uploadStartTime.erase(uploadStartTime.find_last_not_of(Conts::CARRIAGE_RETURN_NEW_LINE) + 1);

    int newGraphID = -1;
    bool graphExists = false;
    KGGraphResolveContext resolveContext{connectionFd, sqlite, hdfsFilePath, path, uploadStartTime,
                                         totalFileSize, loop_exit_p};
    if (!resolveKgGraphId(resolveContext, newGraphID, graphExists)) {
        return false;
    }

    KGStreamingTaskContext taskContext{JasmineGraphServer::getDesignatedWorker(), masterIP, newGraphID,
                                       numberOfPartitions, hdfsServerIp, hdfsPort, hostnamePort,
                                       llmInferenceEngine, llm, chunkSize, hdfsFilePath, graphExists,
                                       sqlite, nullptr};
    launchKgStreamingThread(taskContext);

    return writeSocketLine(connectionFd, "Graph Id: " + std::to_string(newGraphID), loop_exit_p);
}

static void stop_stream_kafka_command(int connFd, const std::string &topicName, bool *loop_exit_p) {
    frontend_logger.info("Started serving `" + STOP_STREAM_KAFKA + "` command for topic=" + topicName);

    StreamRegistry &registry = StreamRegistry::getInstance();

    // Get stream metadata by topic
    auto streamMatches = registry.getStreamsByTopic(topicName);
    if (streamMatches.empty()) {
        string errorMsg = "Error: No active stream found for topic `" + topicName + "`";
        frontend_logger.error(errorMsg);
        write(connFd, errorMsg.c_str(), errorMsg.length());
        write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        return;
    }

    std::shared_ptr<StreamMetadata> streamMetadata;
    if (streamMatches.size() > 1) {
        string prompt =
            "Multiple active streams found for topic `" + topicName + "`. Send graph ID:";
        if (!writeSocketLine(connFd, prompt, loop_exit_p)) {
            return;
        }

        string graphIdInput = readTrimmedSocketInput(connFd);
        if (graphIdInput.empty()) {
            string errorMsg = "Error: Graph ID is required to stop a specific stream";
            frontend_logger.error(errorMsg);
            write(connFd, errorMsg.c_str(), errorMsg.length());
            write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
            return;
        }

        int graphIdValue = -1;
        try {
            graphIdValue = std::stoi(graphIdInput);
        } catch (const std::exception &ex) {
            string errorMsg =
        "Error: Invalid graph ID `" + graphIdInput +
        "`. Reason: " + ex.what();
            frontend_logger.error(errorMsg);
            write(connFd, errorMsg.c_str(), errorMsg.length());
            write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
            return;
        }

        auto matchIt = std::find_if(
            streamMatches.begin(),
            streamMatches.end(),
            [graphIdValue](const std::shared_ptr<StreamMetadata> &metadata) {
                return metadata && metadata->graphId == graphIdValue;
            });

        if (matchIt == streamMatches.end()) {
            string errorMsg =
                "Error: No active stream found for graph ID `" + std::to_string(graphIdValue) +
                "` on topic `" + topicName + "`";
            frontend_logger.error(errorMsg);
            write(connFd, errorMsg.c_str(), errorMsg.length());
            write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
            return;
        }

        streamMetadata = *matchIt;
    } else {
        streamMetadata = streamMatches.front();
    }

    int graphId = streamMetadata->graphId;

    // Signal the stream to stop gracefully
    if (registry.signalStreamStop(graphId)) {
        // Also unsubscribe from Kafka consumer immediately
        if (streamMetadata->kafkaConnector) {
            streamMetadata->kafkaConnector->Unsubscribe();
        }

        string message = "Successfully initiated stop for topic `" + streamMetadata->topicName +
                 "` (graph ID " + std::to_string(graphId) + ")";
        int result_wr = write(connFd, message.c_str(), message.length());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }
        result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(),
                          Conts::CARRIAGE_RETURN_NEW_LINE.size());
        if (result_wr < 0) {
            frontend_logger.error("Error writing to socket");
            *loop_exit_p = true;
            return;
        }

        // Unregister the stream from the registry
        registry.unregisterStream(graphId);
    } else {
        string errorMsg = "Error: Could not signal stop for topic `" + topicName + "`";
        frontend_logger.error(errorMsg);
        if (!writeSocketLine(connFd, errorMsg, loop_exit_p)) {
            return;
        }
    }
}

static void kafka_topics_command(int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p) {
    frontend_logger.info("Serving `" + KTOP + "` command");
    std::set<std::string> topicNames;

    try {
        std::string sql = "SELECT upload_path FROM graph WHERE upload_path LIKE 'kafka:%'";
        auto rows = sqlite->runSelect(sql);
        topicNames = KafkaTopicUtils::extractTopicNames(rows);
    } catch (const std::exception &ex) {
        frontend_logger.error("Failed to fetch Kafka topics from storage: " + std::string(ex.what()));
    }

    writeSocketResultOrEmpty(connFd,
                             KafkaTopicUtils::serializeTopicNames(topicNames, Conts::CARRIAGE_RETURN_NEW_LINE),
                             loop_exit_p);
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
    memset(graph_data, 0, FRONTEND_DATA_LENGTH + 1);

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
    memset(graph_id_data, 0, 301);
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
        memset(priority_data, 0, FRONTEND_DATA_LENGTH + 1);

        read(connFd, priority_data, FRONTEND_DATA_LENGTH);

        string priority(priority_data);

        priority = Utils::trim_copy(priority);

        bool hasNonNumericPriority =
            std::find_if(priority.begin(), priority.end(), [](unsigned char c) { return !std::isdigit(c); }) !=
            priority.end();
        if (hasNonNumericPriority) {
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
    memset(graph_id_data, 0, FRONTEND_DATA_LENGTH + 1);

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
    memset(mode_data, 0, FRONTEND_DATA_LENGTH + 1);

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
    memset(graph_id_data, 0, 301);
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
    memset(graph_id_data, 0, 301);
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
    memset(train_data, 0, 301);
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
    memset(train_data, 0, 301);
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

    std::string graphID = read_frontend_socket_value(connFd);
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
    memset(graph_id, 0, FRONTEND_DATA_LENGTH + 1);

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


    string name = "";
    string path = "";

    std::string pageRankCommand = read_frontend_socket_value(connFd);
    std::vector<std::string> strArr = Utils::split(pageRankCommand, '|');

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
    memset(priority_data, 0, DATA_BUFFER_SIZE);
    read(connFd, priority_data, FRONTEND_DATA_LENGTH);
    string priority(priority_data);
    priority = Utils::trim_copy(priority);

    if (!(std::find_if(priority.begin(), priority.end(), [](unsigned char c) {
              return !std::isdigit(c);
          }) ==
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

    std::string graphID = read_frontend_socket_value(connFd);
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

    std::string graphID = read_frontend_socket_value(connFd);
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


        string graphID = "";
        string modelID = "";
        string path = "";

        std::string predictData = read_socket_value(connFd, 300);
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

        string graphID = "";
        string path = "";

        std::string predictData = read_socket_value(connFd, 300);
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
    memset(worker_data, 0, 301);
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
    memset(category, 0, FRONTEND_DATA_LENGTH + 1);
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

static std::string read_socket_value(int connFd, size_t length) {
    std::string buffer(length, '\0');
    ssize_t bytesRead = read(connFd, buffer.data(), length);
    if (bytesRead <= 0) {
        return "";
    }
    buffer.resize(static_cast<size_t>(bytesRead));
    return Utils::trim_copy(buffer);
}

static std::string read_frontend_socket_value(int connFd) {
    return read_socket_value(connFd, FRONTEND_DATA_LENGTH);
}

static std::string format_local_timestamp(std::time_t timePoint) {
    std::tm localTime = {};
    if (localtime_r(&timePoint, &localTime) == nullptr) {
        return "N/A";
    }

    std::ostringstream oss;
    oss << std::put_time(&localTime, "%Y-%m-%d %H:%M:%S");
    return oss.str();
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

    std::string userResS = read_frontend_socket_value(connFd);

    std::lock_guard lock(threadMapMutex);
    auto it = stopFlags.find(stoi(userResS));
    if (it != stopFlags.end()) {
        *(it->second) = true;

        int noOfMaxWaits = 12;  // Wait up to 1 minute (12 * 5 seconds)
        int waits = 0;
        while (*(it->second) && waits < noOfMaxWaits) {
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

// Temporal query command: Query edges at a specific snapshot
static void temporal_query_command(int connFd, SQLiteDBInterface *, bool *) {
    frontend_logger.info("Temporal query command received");

    std::string message = "Graph ID?";
    int resultWr = write(connFd, message.c_str(), message.length());
    resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());

    std::string graphIdBuf(FRONTEND_DATA_LENGTH, '\0');
    read(connFd, graphIdBuf.data(), FRONTEND_DATA_LENGTH);
    std::string graphIdStr(graphIdBuf);
    graphIdStr = Utils::trim_copy(graphIdStr);
    int graphId = std::stoi(graphIdStr);

    message = "Snapshot ID?";
    resultWr = write(connFd, message.c_str(), message.length());
    resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());

    std::string snapshotStr = read_frontend_socket_value(connFd);
    uint32_t snapshotId = std::stoul(snapshotStr);

    try {
        // Create temporal store and load from disk
        std::string snapshotDir = getTemporalSnapshotDir();
        std::string filePath = TemporalStorePersistence::generateFilePath(snapshotDir, graphId, 0, snapshotId);

        uint64_t timeThreshold = 60;
        uint64_t edgeThreshold = 10000;
        auto temporalStore = std::make_shared<TemporalStore>(graphId, 0, timeThreshold, edgeThreshold,
                                                             SnapshotManager::SnapshotMode::HYBRID);

        if (!temporalStore->loadSnapshotFromDisk(filePath)) {
            std::string error = "Failed to load snapshot " + std::to_string(snapshotId) +
                                " for graph " + std::to_string(graphId) + "\n";
            resultWr = write(connFd, error.c_str(), error.length());
            return;
        }

        // Query edges at snapshot
        TemporalQueryExecutor executor(temporalStore);
        auto result = executor.getEdgesAtSnapshot(snapshotId);

        std::stringstream response;
        response << "Edges in snapshot " << snapshotId << ": " << result.edges.size() << "\n";

        int displayLimit = 10;
        int count = 0;
        for (const auto& [source, destination] : result.edges) {
            if (count++ >= displayLimit) {
                response << "... (showing first " << displayLimit << " edges)\n";
                break;
            }
            response << source << " -> " << destination << "\n";
        }
        response << "Query time: " << result.executionTimeMs << "ms\n";

        std::string responseStr = response.str();
        resultWr = write(connFd, responseStr.c_str(), responseStr.length());
        resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());

        frontend_logger.info("Temporal query completed: " + std::to_string(result.edges.size()) + " edges found");
    } catch (const std::exception& e) {
        std::string error = "Error: " + std::string(e.what());
        resultWr = write(connFd, error.c_str(), error.length());
        resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        frontend_logger.error("Temporal query error: " + std::string(e.what()));
    }
}

// Temporal snapshot command: Get snapshot statistics
static void temporal_snapshot_command(int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p) {
    frontend_logger.info("Temporal snapshot stats command received");

    std::string message = "Graph ID?";
    int resultWr = write(connFd, message.c_str(), message.length());
    resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());

    std::string graphIdStr = read_frontend_socket_value(connFd);
    int graphId = std::stoi(graphIdStr);

    if (!JasmineGraphFrontEndCommon::graphExistsByID(graphIdStr, sqlite)) {
        std::string response = "Error: Graph " + graphIdStr + " does not exist";
        resultWr = write(connFd, response.c_str(), response.length());
        resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        frontend_logger.warn("Temporal snapshot requested for non-existent graph " + graphIdStr);
        return;
    }

    try {
        std::stringstream response;
        response << "Temporal Snapshots for Graph " << graphId << ":\n";
        auto snapMap = loadTemporalSnapshotSummariesForGraph(sqlite, graphId);

        if (snapMap.empty()) {
            response << "No snapshots found\n";
        } else {
            for (const auto& [snapshotId, info] : snapMap) {
                std::time_t t = static_cast<std::time_t>(info.timestamp / 1000000000ULL);
                std::string timeStr = format_local_timestamp(t);
                response << "Snapshot " << snapshotId
                         << "  edges=" << info.totalEdges
                         << "  created=" << timeStr << "\n";
            }
        }

        std::string responseStr = response.str();
        resultWr = write(connFd, responseStr.c_str(), responseStr.length());
        resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());

        frontend_logger.info("Temporal snapshot stats completed");
    } catch (const std::exception& e) {
        std::string error = "Error: " + std::string(e.what());
        resultWr = write(connFd, error.c_str(), error.length());
        resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        frontend_logger.error("Temporal snapshot error: " + std::string(e.what()));
    }
}

// Temporal range command: Query edges in a time range
static void temporal_range_command(int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p) {
    frontend_logger.info("Temporal range query command received");

    std::string message = "Graph ID?";
    int resultWr = write(connFd, message.c_str(), message.length());
    resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());

    std::string graphIdStr = read_frontend_socket_value(connFd);
    int graphId = std::stoi(graphIdStr);

    message = "Start Snapshot ID?";
    resultWr = write(connFd, message.c_str(), message.length());
    resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());

    std::string startStr = read_frontend_socket_value(connFd);
    uint32_t startSnapshot = std::stoul(startStr);

    message = "End Snapshot ID?";
    resultWr = write(connFd, message.c_str(), message.length());
    resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());

    std::string endStr = read_frontend_socket_value(connFd);
    uint32_t endSnapshot = std::stoul(endStr);

    try {
        // Load snapshots from disk and collect edges
        std::vector<std::pair<std::string, std::string>> allEdges;
        std::string snapshotDir = getTemporalSnapshotDir() + "/graph_" + std::to_string(graphId);

        for (uint32_t sid = startSnapshot; sid <= endSnapshot; sid++) {
            uint64_t timeThreshold = 60;
            uint64_t edgeThreshold = 10000;
            auto temporalStore = std::make_shared<TemporalStore>(graphId, 0, timeThreshold, edgeThreshold,
                                                                 SnapshotManager::SnapshotMode::HYBRID);

            std::string filePath = snapshotDir + "/snapshot_" + std::to_string(sid) + ".bin";
            if (temporalStore->loadSnapshotFromDisk(filePath)) {
                TemporalQueryExecutor executor(temporalStore);
                auto result = executor.getEdgesAtSnapshot(sid);
                allEdges.insert(allEdges.end(), result.edges.begin(), result.edges.end());
            }
        }
        auto edges = allEdges;

        std::stringstream response;
        response << "Edges in range [" << startSnapshot << ", " << endSnapshot << "]: " << edges.size() << "\n";

        int displayLimit = 10;
        int count = 0;
        for (const auto& edge : edges) {
            if (count++ >= displayLimit) {
                response << "... (showing first " << displayLimit << " edges)\n";
                break;
            }
            response << edge.first << " -> " << edge.second << "\n";
        }

        std::string responseStr = response.str();
        resultWr = write(connFd, responseStr.c_str(), responseStr.length());
        resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());

        frontend_logger.info("Temporal range query completed: " + std::to_string(edges.size()) + " edges found");
    } catch (const std::exception& e) {
        std::string error = "Error: " + std::string(e.what());
        resultWr = write(connFd, error.c_str(), error.length());
        resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        frontend_logger.error("Temporal range query error: " + std::string(e.what()));
    }
}

// Snapshot Triangle Count Command: Count triangles at a specific snapshot
// NOTE: For partitioned graphs, this aggregates triangle counts from all partitions.
// WARNING: If using edge-cut partitioning, this may miss triangles that span partitions.
// Count triangles at a specific historical snapshot using HistoryTriangles class
static void history_triangle_command(int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p,
                                     const std::string& masterIP) {
    frontend_logger.info("History triangle count command received");
    auto totalStart = std::chrono::high_resolution_clock::now();

    std::string message = "Graph ID?";
    int resultWr = write(connFd, message.c_str(), message.length());
    resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());

    std::string graphIdStr = read_frontend_socket_value(connFd);
    int graphId = std::stoi(graphIdStr);

    message = "Snapshot ID?";
    resultWr = write(connFd, message.c_str(), message.length());
    resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());

    std::string snapshotStr = read_frontend_socket_value(connFd);
    uint32_t snapshotId = std::stoul(snapshotStr);

    try {
        if (!JasmineGraphFrontEndCommon::graphExistsByID(graphIdStr, sqlite)) {
            std::string response = "Error: Graph " + graphIdStr + " does not exist";
            resultWr = write(connFd, response.c_str(), response.length());
            resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
            frontend_logger.warn("History triangle requested for non-existent graph " + graphIdStr);
            return;
        }

        auto snapMap = loadTemporalSnapshotSummariesForGraph(sqlite, graphId);
        if (snapMap.empty()) {
            std::string error = "Error: No snapshots found for graph " + std::to_string(graphId);
            resultWr = write(connFd, error.c_str(), error.length());
            resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
            return;
        }
        if (snapMap.find(snapshotId) == snapMap.end()) {
            uint32_t minSnapshot = snapMap.begin()->first;
            uint32_t maxSnapshot = snapMap.rbegin()->first;
            std::stringstream error;
            error << "Error: Snapshot " << snapshotId << " not found for graph " << graphId
                  << ". Available range: [" << minSnapshot << ", " << maxSnapshot << "]";
            std::string errorText = error.str();
            resultWr = write(connFd, errorText.c_str(), errorText.length());
            resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
            return;
        }

        TemporalTriangleResult result =
            countHistoryTrianglesDistributed(sqlite, graphId, snapshotId, masterIP);
        std::string dataSourceInfo =
            "Data source: distributed-direct (worker-local snapshot files; staging disabled)";

        if (result.partitionsProcessed == 0) {
            bool allowStagedFallback = Utils::parseBoolean(
                Utils::getJasmineGraphProperty("org.jasminegraph.histrian.allow.staged.fallback"));
            if (!allowStagedFallback) {
                std::string error =
                    "Error: History triangle count failed for graph " +
                    std::to_string(graphId) + " at snapshot " +
                    std::to_string(snapshotId) +
                    " (no partitions responded — check worker connectivity, " +
                    "htria protocol, and snapshot availability)";
                resultWr = write(connFd, error.c_str(), error.length());
                resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(),
                                  Conts::CARRIAGE_RETURN_NEW_LINE.size());
                frontend_logger.error(error);
                return;
            }

            frontend_logger.warn("Distributed-direct history triangle count failed for graph " +
                                 std::to_string(graphId) + " snapshot " +
                                 std::to_string(snapshotId) +
                                 "; staged fallback is enabled via org.jasminegraph.histrian.allow.staged.fallback");

            std::string stagedError;
            if (!countHistoryTrianglesFromStagedBitmaps(sqlite, graphId, snapshotId,
                                                        result, stagedError, dataSourceInfo)) {
                std::string error =
                    "Error: History triangle count failed for graph " +
                    std::to_string(graphId) + " at snapshot " +
                    std::to_string(snapshotId) +
                    " (distributed-direct failed; staged failed: " + stagedError + ")";
                resultWr = write(connFd, error.c_str(), error.length());
                resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(),
                                  Conts::CARRIAGE_RETURN_NEW_LINE.size());
                frontend_logger.error(error);
                return;
            }
        }

        {
            auto totalEnd = std::chrono::high_resolution_clock::now();
            long totalDurationMs =
                std::chrono::duration_cast<std::chrono::milliseconds>(totalEnd - totalStart).count();

            std::stringstream response;
            response << "Triangle count is " << result.triangleCount << "\n";
            response << "Time taken (total): " << totalDurationMs << "ms\n";
            response << "Time taken (worker algorithm aggregate): " << result.durationMs << "ms\n";
            if (!dataSourceInfo.empty()) {
                response << dataSourceInfo << "\n";
            }
            if (result.loadShardMs > 0 || result.dedupMs > 0 || result.degreeMs > 0 ||
                result.forwardBuildMs > 0 || result.sortMs > 0 || result.countMs > 0) {
                response << "Time breakdown (histrian): "
                         << "stage=" << result.stagingMs << "ms, "
                         << "load+shard=" << result.loadShardMs << "ms, "
                         << "dedup=" << result.dedupMs << "ms, "
                         << "degree=" << result.degreeMs << "ms, "
                         << "forward-build=" << result.forwardBuildMs << "ms, "
                         << "sort=" << result.sortMs << "ms, "
                         << "triangle-count=" << result.countMs << "ms\n";
                response << "Memory hint (histrian): cached-dedup-edges="
                         << result.cachedDedupEdges << "\n";
            }

            std::string responseStr = response.str();
            resultWr = write(connFd, responseStr.c_str(), responseStr.length());
            resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());

            appendHistoryQueryResultToFile("histrian", graphId,
                                           "snapshot=" + std::to_string(snapshotId),
                                           responseStr);

            frontend_logger.info("Distributed history triangle count completed: " +
                                 std::to_string(result.triangleCount) + " triangles across " +
                                 std::to_string(result.partitionsProcessed) +
                                 " partitions (total=" + std::to_string(totalDurationMs) +
                                 "ms, worker_algorithm_aggregate=" +
                                 std::to_string(result.durationMs) + "ms)");
        }
    } catch (const std::exception& e) {
        std::string error = "Error: " + std::string(e.what());
        resultWr = write(connFd, error.c_str(), error.length());
        resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        frontend_logger.error("Snapshot triangle count error: " + std::string(e.what()));
    }
}
// History Triangle Count by Timestamp Command
static bool parseTemporalTargetTimestamp(const std::string& timestampStr, uint64_t& targetTimestamp) {
    if (timestampStr.find("-") != std::string::npos || timestampStr.find(":") != std::string::npos) {
        struct tm tm = {};
        if (strptime(timestampStr.c_str(), "%Y-%m-%d %H:%M:%S", &tm) == nullptr) {
            return false;
        }
        targetTimestamp = static_cast<uint64_t>(std::mktime(&tm)) * 1000000000ULL;
        return true;
    }

    targetTimestamp = std::stoull(timestampStr);
    if (targetTimestamp < 1000000000000ULL) {
        targetTimestamp *= 1000000000ULL;
    }
    return true;
}

static std::map<uint32_t, uint64_t> loadSnapshotTimestampsForGraph(SQLiteDBInterface* sqlite, int graphId) {
    auto snapMap = loadTemporalSnapshotSummariesForGraph(sqlite, graphId);
    std::map<uint32_t, uint64_t> snapshotTimestamps;

    for (const auto& [snapshotId, info] : snapMap) {
        snapshotTimestamps[snapshotId] = info.timestamp;
    }

    return snapshotTimestamps;
}

static uint32_t findClosestSnapshotId(const std::map<uint32_t, uint64_t>& snapshotTimestamps,
                                      uint64_t targetTimestamp) {
    uint32_t closestSnapshotId = 0;
    uint64_t minDiff = UINT64_MAX;

    for (const auto& [snapshotId, timestamp] : snapshotTimestamps) {
        uint64_t diff = (timestamp <= targetTimestamp)
                      ? (targetTimestamp - timestamp)
                      : (timestamp - targetTimestamp);
        if (diff < minDiff) {
            minDiff = diff;
            closestSnapshotId = snapshotId;
        }
    }

    return closestSnapshotId;
}

static std::string formatSnapshotTimestamp(uint64_t timestampNs) {
    std::time_t snapshotTime = static_cast<std::time_t>(timestampNs / 1000000000ULL);
    return format_local_timestamp(snapshotTime);
}

static void history_triangle_timestamp_command(int connFd, SQLiteDBInterface *sqlite, bool *,
                                               const std::string& masterIP) {
    frontend_logger.info("History triangle count by timestamp command received");
    auto totalStart = std::chrono::high_resolution_clock::now();

    std::string message = "Graph ID?";
    int resultWr = write(connFd, message.c_str(), message.length());
    resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());

    std::string graphIdStr = read_frontend_socket_value(connFd);
    int graphId = std::stoi(graphIdStr);

    message = "Timestamp (YYYY-MM-DD HH:MM:SS or Unix epoch)?";
    resultWr = write(connFd, message.c_str(), message.length());
    resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());

    std::string timestampStr = read_frontend_socket_value(connFd);

    try {
        uint64_t targetTimestamp = 0;
        if (!parseTemporalTargetTimestamp(timestampStr, targetTimestamp)) {
            std::string error = "Error: Invalid timestamp format";
            resultWr = write(connFd, error.c_str(), error.length());
            resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
            return;
        }

        if (!JasmineGraphFrontEndCommon::graphExistsByID(graphIdStr, sqlite)) {
            std::string response = "Error: Graph " + graphIdStr + " does not exist";
            resultWr = write(connFd, response.c_str(), response.length());
            resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
            frontend_logger.warn("History triangle timestamp requested for non-existent graph " + graphIdStr);
            return;
        }

        std::map<uint32_t, uint64_t> snapshotTimestamps = loadSnapshotTimestampsForGraph(sqlite, graphId);

        if (snapshotTimestamps.empty()) {
            std::string error = "Error: No snapshots found for graph " + std::to_string(graphId);
            resultWr = write(connFd, error.c_str(), error.length());
            resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
            return;
        }

        uint32_t closestSnapshotId = findClosestSnapshotId(snapshotTimestamps, targetTimestamp);

        TemporalTriangleResult result =
            countHistoryTrianglesDistributed(sqlite, graphId, closestSnapshotId, masterIP);
        std::string dataSourceInfo =
            "Data source: distributed-direct (worker-local snapshot files; staging disabled)";
        if (result.partitionsProcessed == 0) {
            bool allowStagedFallback = Utils::parseBoolean(
                Utils::getJasmineGraphProperty("org.jasminegraph.histrian.allow.staged.fallback"));
            if (!allowStagedFallback) {
                std::string error =
                    "Error: Failed to process snapshot " +
                    std::to_string(closestSnapshotId) +
                    " (no partitions responded — check worker connectivity, " +
                    "htria protocol, and snapshot availability)";
                resultWr = write(connFd, error.c_str(), error.length());
                resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(),
                                  Conts::CARRIAGE_RETURN_NEW_LINE.size());
                return;
            }

            frontend_logger.warn("Distributed-direct history triangle-by-timestamp failed for graph " +
                                 std::to_string(graphId) + " snapshot " +
                                 std::to_string(closestSnapshotId) +
                                 "; staged fallback is enabled via org.jasminegraph.histrian.allow.staged.fallback");

            std::string stagedError;
            if (!countHistoryTrianglesFromStagedBitmaps(sqlite, graphId, closestSnapshotId,
                                                        result, stagedError, dataSourceInfo)) {
                std::string error =
                    "Error: Failed to process snapshot " +
                    std::to_string(closestSnapshotId) +
                    " (distributed-direct failed; staged failed: " + stagedError + ")";
                resultWr = write(connFd, error.c_str(), error.length());
                resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(),
                                  Conts::CARRIAGE_RETURN_NEW_LINE.size());
                return;
            }
        }

        std::string timeStr = formatSnapshotTimestamp(snapshotTimestamps.at(closestSnapshotId));
        auto totalEnd = std::chrono::high_resolution_clock::now();
        long totalDurationMs =
            std::chrono::duration_cast<std::chrono::milliseconds>(totalEnd - totalStart).count();

        std::stringstream response;
        response << "Closest snapshot: " << closestSnapshotId << " (created: " << timeStr << ")\n";
        response << "Triangle count using cumulative edges from snapshots [0, " << closestSnapshotId
             << "]: " << result.triangleCount << "\n";
        response << "Partitions processed: " << result.partitionsProcessed << "\n";
        response << "Time taken (total): " << totalDurationMs << "ms\n";
        response << "Time taken (worker algorithm aggregate): " << result.durationMs << "ms\n";
        if (!dataSourceInfo.empty()) {
            response << dataSourceInfo << "\n";
        }

        std::string responseStr = response.str();
        resultWr = write(connFd, responseStr.c_str(), responseStr.length());
        resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());

        appendHistoryQueryResultToFile("histrian_ts", graphId,
                                       "target_ts=" + timestampStr +
                                           " closest_snapshot=" + std::to_string(closestSnapshotId),
                                       responseStr);

        frontend_logger.info("History triangle count by timestamp completed");
    } catch (const std::exception& e) {
        std::string error = "Error: " + std::string(e.what());
        resultWr = write(connFd, error.c_str(), error.length());
        resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        frontend_logger.error("Timestamp triangle count error: " + std::string(e.what()));
    }
}

// History PageRank by Snapshot ID Command
// ── DISTRIBUTED PAGERANK WORKER COMMUNICATION ────────────────────────────
// Collects PageRank results from a single worker partition (streaming ranked nodes)
bool collectHistoryPageRankFromWorker(int graphId,
                                       uint32_t snapshotId,
                                       const Utils::worker& worker,
                                       int partitionId,
                                       int topK,
                                       int maxIterations,
                                       double dampingFactor,
                                       const std::string& masterIP,
                                       int threadPriority,
                                       std::vector<std::pair<std::string, double>>& workerRankedNodes,
                                       long& workerDurationMs,
                                       uint64_t& workerRawEdges) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        frontend_logger.error("Cannot create socket for distributed history pagerank");
        return false;
    }

    std::string host = worker.hostname;
    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }

    struct hostent* server = gethostbyname(host.c_str());
    if (server == nullptr) {
        frontend_logger.error("Failed to resolve worker host " + host);
        close(sockfd);
        return false;
    }

    struct sockaddr_in serv_addr;
    bzero((char*)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char*)server->h_addr, (char*)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(std::stoi(worker.port));

    if (Utils::connect_wrapper(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        frontend_logger.error("Failed to connect to worker " + host + ":" + worker.port);
        close(sockfd);
        return false;
    }

    std::string data(INSTANCE_DATA_LENGTH + 1, '\0');
    auto closeAndFail = [&]() {
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return false;
    };

    if (!Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE) ||
        Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH) !=
            JasmineGraphInstanceProtocol::HANDSHAKE_OK) {
        return closeAndFail();
    }

    if (!Utils::send_str_wrapper(sockfd, masterIP) ||
        Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH) !=
            JasmineGraphInstanceProtocol::HOST_OK) {
        return closeAndFail();
    }

    // Send HISTORY_PAGERANK command
    if (!Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::HISTORY_PAGERANK) ||
        Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH) !=
            JasmineGraphInstanceProtocol::OK) {
        return closeAndFail();
    }

    if (!Utils::send_str_wrapper(sockfd, std::to_string(graphId)) ||
        Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH) !=
            JasmineGraphInstanceProtocol::OK) {
        return closeAndFail();
    }

    if (!Utils::send_str_wrapper(sockfd, std::to_string(partitionId)) ||
        Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH) !=
            JasmineGraphInstanceProtocol::OK) {
        return closeAndFail();
    }

    if (!Utils::send_str_wrapper(sockfd, std::to_string(snapshotId)) ||
        Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH) !=
            JasmineGraphInstanceProtocol::OK) {
        return closeAndFail();
    }

    if (!Utils::send_str_wrapper(sockfd, std::to_string(topK)) ||
        Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH) !=
            JasmineGraphInstanceProtocol::OK) {
        return closeAndFail();
    }

    if (!Utils::send_str_wrapper(sockfd, std::to_string(maxIterations)) ||
        Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH) !=
            JasmineGraphInstanceProtocol::OK) {
        return closeAndFail();
    }

    if (!Utils::send_str_wrapper(sockfd, std::to_string(dampingFactor)) ||
        Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH) !=
            JasmineGraphInstanceProtocol::OK) {
        return closeAndFail();
    }

    if (!Utils::send_str_wrapper(sockfd, std::to_string(threadPriority)) ||
        Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH) !=
            JasmineGraphInstanceProtocol::OK) {
        return closeAndFail();
    }

    if (!Utils::send_str_wrapper(sockfd, "trace-disabled")) {
        return closeAndFail();
    }

    // Receive (node_id, score) pairs from worker
    uint32_t nodeCount = 0;
    if (!readUint32(sockfd, nodeCount)) {
        return closeAndFail();
    }

    workerRankedNodes.clear();
    workerRankedNodes.reserve(std::min(static_cast<size_t>(topK), static_cast<size_t>(nodeCount)));

    for (uint32_t i = 0; i < nodeCount; ++i) {
        uint32_t nodeIdLength = 0;
        if (!readUint32(sockfd, nodeIdLength)) {
            return closeAndFail();
        }

        std::string nodeId(nodeIdLength, '\0');
        if (!recvAll(sockfd, nodeId.data(), nodeIdLength)) {
            return closeAndFail();
        }

        double score = 0.0;
        if (!readDouble(sockfd, score)) {
            return closeAndFail();
        }

        workerRankedNodes.emplace_back(nodeId, score);
    }

    uint64_t rawEdgesFromWorker = 0;
    uint64_t durationFromWorker = 0;
    if (!readUint64(sockfd, rawEdgesFromWorker) || !readUint64(sockfd, durationFromWorker)) {
        return closeAndFail();
    }

    workerRawEdges  = rawEdgesFromWorker;
    workerDurationMs = static_cast<long>(durationFromWorker);

    Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
    close(sockfd);
    return true;
}

// ── DISTRIBUTED PAGERANK COORDINATOR ────────────────────────────────────────
// Coordinates PageRank computation across all worker partitions
static HistoryPageRankResult countHistoryPageRankDistributed(SQLiteDBInterface* sqlite,
                                                             int graphId,
                                                             uint32_t snapshotId,
                                                             int topK,
                                                             int maxIterations,
                                                             double dampingFactor,
                                                             const std::string& masterIP) {
    HistoryPageRankResult result{};
    std::vector<Utils::worker> allWorkers = Utils::getWorkerList(sqlite);

    // Query which workers host which partitions for this graph
    std::string sqlStatement =
        "SELECT DISTINCT worker_idworker, partition_idpartition "
        "FROM worker_has_partition "
        "WHERE partition_graph_idgraph=" + std::to_string(graphId) + ";";
    const auto& rows = sqlite->runSelect(sqlStatement);

    // workerID -> list of partition IDs
    std::map<std::string, std::vector<int>> workerPartitionMap;
    for (const auto& row : rows) {
        workerPartitionMap[row.at(0).second].push_back(std::stoi(row.at(1).second));
    }

    if (workerPartitionMap.empty()) {
        frontend_logger.warn("No worker-partition assignments found in worker_has_partition for graph " +
                             std::to_string(graphId) +
                             ". Falling back to remote temporal file discovery");

        std::string snapshotDir = getTemporalSnapshotDir();
        std::map<int, std::string> partitionOwner;
        for (const auto& worker : allWorkers) {
            std::set<int> discoveredPartitions =
                discoverTemporalPartitionsForWorker(worker, graphId, snapshotDir);

            if (discoveredPartitions.empty()) {
                continue;
            }

            for (int partitionId : discoveredPartitions) {
                auto inserted = partitionOwner.emplace(partitionId, worker.workerID);
                if (!inserted.second) {
                    frontend_logger.warn("Skipping duplicate discovered partition " +
                                         std::to_string(partitionId) +
                                         " for worker " + worker.workerID +
                                         " (already assigned to worker " + inserted.first->second + ")");
                    continue;
                }
                workerPartitionMap[worker.workerID].push_back(partitionId);
            }
        }

        if (workerPartitionMap.empty()) {
            frontend_logger.error("No worker-partition assignments found for graph " +
                                  std::to_string(graphId) +
                                  " and remote temporal partition discovery found nothing");
            return result;
        }

        for (auto& [workerId, partitions] : workerPartitionMap) {
            std::sort(partitions.begin(), partitions.end());
            partitions.erase(std::unique(partitions.begin(), partitions.end()), partitions.end());
        }

        frontend_logger.info("Discovered " + std::to_string(partitionOwner.size()) +
                             " remote temporal partitions for graph " +
                             std::to_string(graphId));
    }

    // Per-task state (no shared mutation during async execution)
    struct WorkerTask {
        std::string workerID;
        int partitionId{0};
        std::vector<std::pair<std::string, double>> rankedNodes;
        uint64_t rawEdges{0};
        long durationMs{0};
        std::future<bool> taskSucceeded;
    };
    std::vector<WorkerTask> tasks;
    tasks.reserve(rows.size());

    // Launch one async task per (worker, partition)
    for (const auto& w : allWorkers) {
        auto it = workerPartitionMap.find(w.workerID);
        if (it == workerPartitionMap.end()) continue;

        for (int pid : it->second) {
            tasks.push_back({w.workerID, pid, {}, 0, 0, {}});
            WorkerTask& task = tasks.back();
            task.taskSucceeded = std::async(
                std::launch::async,
                [graphId, snapshotId, w, pid, topK, maxIterations, dampingFactor,
                 &masterIP, &task]() mutable -> bool {
                    long duration = 0;
                    uint64_t rawEdges = 0;
                    bool ok = collectHistoryPageRankFromWorker(
                        graphId, snapshotId, w, pid, topK, maxIterations, dampingFactor,
                        masterIP, Conts::DEFAULT_THREAD_PRIORITY,
                        task.rankedNodes, duration, rawEdges);
                    task.durationMs = duration;
                    task.rawEdges = rawEdges;
                    return ok;
                });
        }
    }

    if (tasks.empty()) {
        frontend_logger.error(
            "No runnable worker-partition tasks found for graph " +
            std::to_string(graphId) +
            ". Partition assignments may reference workers that are not currently registered");
        return result;
    }

    // Collect results
    int partitionsProcessed = 0;
    int partitionsFailed = 0;
    uint64_t totalRawEdges = 0;
    long maxWorkerDuration = 0;
    std::unordered_map<std::string, double> aggregatedNodeScores;  // node -> highest score
    for (auto& task : tasks) {
        bool ok = task.taskSucceeded.get();
        if (ok) {
            partitionsProcessed++;
            totalRawEdges += task.rawEdges;
            if (task.durationMs > maxWorkerDuration) maxWorkerDuration = task.durationMs;

            // Aggregate ranked nodes from this worker partition
            for (const auto& [nodeId, score] : task.rankedNodes) {
                auto it = aggregatedNodeScores.find(nodeId);
                if (it == aggregatedNodeScores.end() || score > it->second) {
                    aggregatedNodeScores[nodeId] = score;
                }
            }

            frontend_logger.info(
                "Worker " + task.workerID +
                " partition=" + std::to_string(task.partitionId) +
                " topK_returned=" + std::to_string(task.rankedNodes.size()) +
                " rawEdges=" + std::to_string(task.rawEdges) +
                " dur=" + std::to_string(task.durationMs) + "ms");
        } else {
            partitionsFailed++;
            frontend_logger.warn(
                "Worker " + task.workerID +
                " partition=" + std::to_string(task.partitionId) + " failed — skipped");
        }
    }

    if (partitionsProcessed == 0) {
        frontend_logger.error(
            "All distributed history pagerank tasks failed for graph " +
            std::to_string(graphId) + " snapshot=" + std::to_string(snapshotId) +
            " (failed_tasks=" + std::to_string(partitionsFailed) + ")");
        return result;
    }

    // Aggregate and sort: take top-K from all workers' results
    std::vector<std::pair<std::string, double>> globalRankedNodes;
    for (const auto& [nodeId, score] : aggregatedNodeScores) {
        globalRankedNodes.emplace_back(nodeId, score);
    }

    // Sort descending by score
    std::sort(globalRankedNodes.begin(), globalRankedNodes.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });

    // Keep only top-K
    if (static_cast<int>(globalRankedNodes.size()) > topK) {
        globalRankedNodes.resize(topK);
    }

    result.rankedNodes = globalRankedNodes;
    result.rawEdges = totalRawEdges;
    result.partitionsProcessed = partitionsProcessed;
    result.durationMs = maxWorkerDuration;
    result.totalNodes = aggregatedNodeScores.size();

    frontend_logger.info(
        "[histpgr distributed] partitions=" + std::to_string(partitionsProcessed) +
        " total_nodes=" + std::to_string(aggregatedNodeScores.size()) +
        " top_k=" + std::to_string(globalRankedNodes.size()) +
        " raw_edges=" + std::to_string(totalRawEdges));

    return result;
}

static bool countHistoryPageRankFromStagedBitmaps(SQLiteDBInterface *sqlite,
                                                  int graphId,
                                                  uint32_t snapshotId,
                                                  int topK,
                                                  int maxIterations,
                                                  double dampingFactor,
                                                  HistoryPageRankResult& result,
                                                  std::string& errorMessage,
                                                  std::string& dataSourceInfo) {
    std::string snapshotDir = getTemporalSnapshotDir();
    std::string localDirectFailureReason;
    std::vector<std::string> localDirs = getLocalTemporalSnapshotCandidateDirs();

    for (const auto& localDir : localDirs) {
        if (!hasTemporalBitmapIndexesForGraphInDirectory(localDir, graphId)) {
            continue;
        }

        try {
            HistoryPageRankResult directResult = HistoryPageRank::computePageRankAtSnapshot(
                graphId, snapshotId, localDir, topK, maxIterations, dampingFactor);
            if (directResult.partitionsProcessed > 0) {
                result = std::move(directResult);
                dataSourceInfo = "Data source: local-direct (path=" + localDir + ")";
                frontend_logger.info("histpgr local-direct path used: " + localDir);
                return true;
            }

            localDirectFailureReason = "local-direct found files in " + localDir +
                                       " but processed zero partitions";
        } catch (const std::exception&) {
            localDirectFailureReason = "local-direct read/count failed in " + localDir;
        }
    }

    if (localDirectFailureReason.empty()) {
        std::stringstream reason;
        reason << "no graph bitmap files found in local candidates";
        if (!localDirs.empty()) {
            reason << " [";
            for (size_t i = 0; i < localDirs.size(); ++i) {
                if (i > 0) {
                    reason << ",";
                }
                reason << localDirs[i];
            }
            reason << "]";
        }
        localDirectFailureReason = reason.str();
    }

    std::string stagedSnapshotDir = stageTemporalBitmapIndexesForGraph(sqlite, graphId, snapshotDir);
    if (stagedSnapshotDir.empty()) {
        errorMessage = "No temporal bitmap index files available for graph " + std::to_string(graphId);
        return false;
    }

    try {
        result = HistoryPageRank::computePageRankAtSnapshot(graphId, snapshotId, stagedSnapshotDir,
                                                            topK, maxIterations, dampingFactor);
        cleanupStagedTemporalBitmapIndexes(stagedSnapshotDir);
        if (result.partitionsProcessed == 0) {
            errorMessage = "No snapshot partitions were processed for graph " + std::to_string(graphId);
            return false;
        }

        dataSourceInfo = "Data source: staged-fallback (reason=" + localDirectFailureReason + ")";
        return true;
    } catch (const std::exception& e) {
        cleanupStagedTemporalBitmapIndexes(stagedSnapshotDir);
        errorMessage = e.what();
        return false;
    }
}

static void history_pagerank_command(int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p,
                                     const std::string& masterIP) {
    frontend_logger.info("History PageRank command received");

    std::string message = "Graph ID?";
    int resultWr = write(connFd, message.c_str(), message.length());
    resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());

    std::string graphIdStr = read_frontend_socket_value(connFd);
    int graphId = std::stoi(graphIdStr);

    message = "Snapshot ID?";
    resultWr = write(connFd, message.c_str(), message.length());
    resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());

    std::string snapshotStr = read_frontend_socket_value(connFd);
    uint32_t snapshotId = std::stoul(snapshotStr);

    message = "Top-K (number of top nodes to return, 0 for all)?";
    resultWr = write(connFd, message.c_str(), message.length());
    resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());

    std::string topKStr = read_frontend_socket_value(connFd);
    int topK = topKStr.empty() ? 10 : std::stoi(topKStr);

    message = "Max iterations (default 100, higher = more accurate)?";
    resultWr = write(connFd, message.c_str(), message.length());
    resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());

    std::string iterStr = read_frontend_socket_value(connFd);
    int maxIterations = iterStr.empty() ? 100 : std::stoi(iterStr);
    std::string dataSourceInfo;

    try {
        if (!JasmineGraphFrontEndCommon::graphExistsByID(graphIdStr, sqlite)) {
            std::string response = "Error: Graph " + graphIdStr + " does not exist";
            resultWr = write(connFd, response.c_str(), response.length());
            resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
            frontend_logger.warn("History PageRank requested for non-existent graph " + graphIdStr);
            return;
        }

        auto snapMap = loadTemporalSnapshotSummariesForGraph(sqlite, graphId);
        if (snapMap.empty()) {
            std::string error = "Error: No snapshots found for graph " + std::to_string(graphId);
            resultWr = write(connFd, error.c_str(), error.length());
            resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
            return;
        }
        if (snapMap.find(snapshotId) == snapMap.end()) {
            uint32_t minSnapshot = snapMap.begin()->first;
            uint32_t maxSnapshot = snapMap.rbegin()->first;
            std::stringstream error;
            error << "Error: Snapshot " << snapshotId << " not found for graph " << graphId
                  << ". Available range: [" << minSnapshot << ", " << maxSnapshot << "]";
            std::string errorText = error.str();
            resultWr = write(connFd, errorText.c_str(), errorText.length());
            resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
            return;
        }

        HistoryPageRankResult result;
        std::string errorMessage;
        if (!countHistoryPageRankFromStagedBitmaps(sqlite, graphId, snapshotId, topK, maxIterations,
                                                   PAGE_RANK_ALPHA, result, errorMessage, dataSourceInfo)) {
            std::string error = "Error: " + errorMessage;
            resultWr = write(connFd, error.c_str(), error.length());
            resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
            frontend_logger.error("History PageRank failed for graph " + std::to_string(graphId) +
                                  " snapshot " + std::to_string(snapshotId) +
                                  ": " + errorMessage);
            return;
        }

        if (result.partitionsProcessed > 0) {
            std::stringstream response;
            response << "Nodes: " << result.totalNodes
                     << "  Iterations: " << result.iterations
                     << "  Time: " << result.durationMs << "ms\n";
            if (!dataSourceInfo.empty()) {
                response << dataSourceInfo << "\n";
            }
            response << "Rank  Node                           Score\n";
            response << "----  ------------------------------  --------------------\n";
            int rank = 1;
            for (const auto& [node, score] : result.rankedNodes) {
                response << rank++ << "\t" << node << "\t" << score << "\n";
            }

            std::string responseStr = response.str();
            resultWr = write(connFd, responseStr.c_str(), responseStr.length());
            resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(),
                             Conts::CARRIAGE_RETURN_NEW_LINE.size());

            appendHistoryQueryResultToFile("histpgr", graphId,
                                           "snapshot=" + std::to_string(snapshotId) +
                                               " topk=" + std::to_string(topK) +
                                               " max_iterations=" + std::to_string(maxIterations),
                                           responseStr);

            frontend_logger.info("History PageRank completed: top " +
                                 std::to_string(result.rankedNodes.size()) +
                                 " nodes from " + std::to_string(result.totalNodes) +
                                 " total nodes, " + std::to_string(result.totalEdges) + " cumulative edges");
        }
    } catch (const std::exception& e) {
        std::string error = "Error: " + std::string(e.what());
        resultWr = write(connFd, error.c_str(), error.length());
        resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(),
                         Conts::CARRIAGE_RETURN_NEW_LINE.size());
        frontend_logger.error("History PageRank error: " + std::string(e.what()));
    }
}

// History PageRank by Timestamp Command
static void history_pagerank_timestamp_command(int connFd, SQLiteDBInterface *sqlite, bool *loop_exit_p,
                                               const std::string& masterIP) {
    frontend_logger.info("History PageRank by timestamp command received");

    std::string message = "Graph ID?";
    int resultWr = write(connFd, message.c_str(), message.length());
    resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());

    std::string graphIdStr = read_frontend_socket_value(connFd);
    int graphId = std::stoi(graphIdStr);

    message = "Timestamp (YYYY-MM-DD HH:MM:SS or Unix epoch)?";
    resultWr = write(connFd, message.c_str(), message.length());
    resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());

    std::string timestampStr = read_frontend_socket_value(connFd);

    message = "Top-K (number of top nodes to return, 0 for all)?";
    resultWr = write(connFd, message.c_str(), message.length());
    resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());

    std::string topKStr = read_frontend_socket_value(connFd);
    int topK = topKStr.empty() ? 10 : std::stoi(topKStr);

    message = "Max iterations (default 100, higher = more accurate)?";
    resultWr = write(connFd, message.c_str(), message.length());
    resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());

    std::string iterStr = read_frontend_socket_value(connFd);
    int maxIterations = iterStr.empty() ? 100 : std::stoi(iterStr);
    std::string dataSourceInfo;

    try {
        uint64_t targetTimestamp = 0;
        if (!parseTemporalTargetTimestamp(timestampStr, targetTimestamp)) {
            std::string error = "Error: Invalid timestamp format. Use YYYY-MM-DD HH:MM:SS or Unix epoch.";
            resultWr = write(connFd, error.c_str(), error.length());
            resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(),
                             Conts::CARRIAGE_RETURN_NEW_LINE.size());
            return;
        }

        if (!JasmineGraphFrontEndCommon::graphExistsByID(graphIdStr, sqlite)) {
            std::string response = "Error: Graph " + graphIdStr + " does not exist";
            resultWr = write(connFd, response.c_str(), response.length());
            resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
            frontend_logger.warn("History PageRank timestamp requested for non-existent graph " + graphIdStr);
            return;
        }

        std::map<uint32_t, uint64_t> snapshotTimestamps = loadSnapshotTimestampsForGraph(sqlite, graphId);

        if (snapshotTimestamps.empty()) {
            std::string error = "Error: No snapshots found for graph " + std::to_string(graphId);
            resultWr = write(connFd, error.c_str(), error.length());
            resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(),
                             Conts::CARRIAGE_RETURN_NEW_LINE.size());
            return;
        }

        uint32_t closestSnapshotId = findClosestSnapshotId(snapshotTimestamps, targetTimestamp);

        HistoryPageRankResult result;
        std::string errorMessage;
        if (!countHistoryPageRankFromStagedBitmaps(sqlite, graphId, closestSnapshotId, topK,
                                                   maxIterations, PAGE_RANK_ALPHA, result,
                                                   errorMessage, dataSourceInfo)) {
            std::string error = "Error: " + errorMessage;
            resultWr = write(connFd, error.c_str(), error.length());
            resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
            frontend_logger.error("History PageRank timestamp failed for graph " +
                                  std::to_string(graphId) + " snapshot " +
                                  std::to_string(closestSnapshotId) + ": " + errorMessage);
            return;
        }

        if (result.partitionsProcessed > 0) {
            std::string timeStr = formatSnapshotTimestamp(snapshotTimestamps[closestSnapshotId]);

            std::stringstream response;
            response << "Closest snapshot: " << closestSnapshotId << " (created: " << timeStr << ")\n";
            response << "Nodes: " << result.totalNodes
                     << "  Iterations: " << result.iterations
                     << "  Time: " << result.durationMs << "ms\n";
            if (!dataSourceInfo.empty()) {
                response << dataSourceInfo << "\n";
            }
            response << "Rank  Node                           Score\n";
            response << "----  ------------------------------  --------------------\n";
            int rank = 1;
            for (const auto& [node, score] : result.rankedNodes) {
                response << rank++ << "\t" << node << "\t" << score << "\n";
            }

            std::string responseStr = response.str();
            resultWr = write(connFd, responseStr.c_str(), responseStr.length());
            resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(),
                             Conts::CARRIAGE_RETURN_NEW_LINE.size());

            appendHistoryQueryResultToFile("histpgr_ts", graphId,
                                           "target_ts=" + timestampStr +
                                               " closest_snapshot=" + std::to_string(closestSnapshotId) +
                                               " topk=" + std::to_string(topK) +
                                               " max_iterations=" + std::to_string(maxIterations),
                                           responseStr);

            frontend_logger.info("History PageRank by timestamp completed: snapshot " +
                                  std::to_string(closestSnapshotId) + " (" + timeStr + ")");
        }
    } catch (const std::exception& e) {
        std::string error = "Error: " + std::string(e.what());
        resultWr = write(connFd, error.c_str(), error.length());
        resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(),
                         Conts::CARRIAGE_RETURN_NEW_LINE.size());
        frontend_logger.error("History PageRank timestamp error: " + std::string(e.what()));
    }
}

// History BFS by Snapshot ID Command
static void history_bfs_command(int connFd, SQLiteDBInterface *sqlite, bool *) {
    frontend_logger.info("History BFS command received");

    std::string message = "Graph ID?";
    int resultWr = write(connFd, message.c_str(), message.length());
    resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());

    std::string graphIdStr = read_frontend_socket_value(connFd);
    int graphId = std::stoi(graphIdStr);

    message = "Snapshot ID?";
    resultWr = write(connFd, message.c_str(), message.length());
    resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());

    std::string snapshotStr = read_frontend_socket_value(connFd);
    uint32_t snapshotId = std::stoul(snapshotStr);

    message = "Source Node?";
    resultWr = write(connFd, message.c_str(), message.length());
    resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());

    std::string sourceNode = read_frontend_socket_value(connFd);

    message = "Max Depth (0 for full traversal)?";
    resultWr = write(connFd, message.c_str(), message.length());
    resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());

    std::string depthStr = read_frontend_socket_value(connFd);
    int maxDepth = depthStr.empty() ? 0 : std::stoi(depthStr);

    std::string stagedSnapshotDir;

    try {
        if (sourceNode.empty()) {
            std::string error = "Error: Source node cannot be empty";
            resultWr = write(connFd, error.c_str(), error.length());
            resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
            return;
        }

        if (!JasmineGraphFrontEndCommon::graphExistsByID(graphIdStr, sqlite)) {
            std::string response = "Error: Graph " + graphIdStr + " does not exist";
            resultWr = write(connFd, response.c_str(), response.length());
            resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
            frontend_logger.warn("History BFS requested for non-existent graph " + graphIdStr);
            return;
        }

        std::string snapshotDir = getTemporalSnapshotDir();
        auto snapMap = loadTemporalSnapshotSummariesForGraph(sqlite, graphId);
        if (snapMap.empty()) {
            std::string error = "Error: No snapshots found for graph " + std::to_string(graphId);
            resultWr = write(connFd, error.c_str(), error.length());
            resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
            return;
        }
        if (snapMap.find(snapshotId) == snapMap.end()) {
            uint32_t minSnapshot = snapMap.begin()->first;
            uint32_t maxSnapshot = snapMap.rbegin()->first;
            std::stringstream error;
            error << "Error: Snapshot " << snapshotId << " not found for graph " << graphId
                  << ". Available range: [" << minSnapshot << ", " << maxSnapshot << "]";
            std::string errorText = error.str();
            resultWr = write(connFd, errorText.c_str(), errorText.length());
            resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
            return;
        }

        stagedSnapshotDir = stageTemporalBitmapIndexesForGraph(sqlite, graphId, snapshotDir);
        if (stagedSnapshotDir.empty()) {
            std::string error = "Error: No temporal bitmap index files found for graph " + std::to_string(graphId);
            resultWr = write(connFd, error.c_str(), error.length());
            resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
            frontend_logger.error(error);
            return;
        }

        HistoryBFSResult result = HistoryBFS::runBFSAtSnapshot(
            graphId, snapshotId, sourceNode, maxDepth, stagedSnapshotDir);

        cleanupStagedTemporalBitmapIndexes(stagedSnapshotDir);

        if (result.partitionsProcessed == 0) {
            std::string error = "Error: No snapshot files found for graph " +
                                std::to_string(graphId) + " at snapshot " +
                                std::to_string(snapshotId);
            resultWr = write(connFd, error.c_str(), error.length());
            resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
            return;
        }

        std::stringstream response;
        response << "BFS result saved to: " << result.outputPath << "\n";
        response << "Visited nodes: " << result.visitedNodes << "\n";
        response << "Nodes: " << result.totalNodes
                 << "  Edges: " << result.totalEdges
                 << "  Time: " << result.durationMs << "ms\n";

        std::string responseStr = response.str();
        resultWr = write(connFd, responseStr.c_str(), responseStr.length());
        resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());

        frontend_logger.info("History BFS completed: graph " + std::to_string(graphId) +
                             " snapshot " + std::to_string(snapshotId) +
                             " output " + result.outputPath);
    } catch (const std::exception& e) {
        cleanupStagedTemporalBitmapIndexes(stagedSnapshotDir);
        std::string error = "Error: " + std::string(e.what());
        resultWr = write(connFd, error.c_str(), error.length());
        resultWr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
        frontend_logger.error("History BFS error: " + std::string(e.what()));
    }
}
