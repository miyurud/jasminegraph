/**
Copyright 2021 JasmineGraph Team
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

#include "TriangleCountExecutor.h"

#include <time.h>
#include <unistd.h>

#include "../../../../../globals.h"
#include "../../../../k8s/K8sWorkerController.h"
#include "../../../../scale/scaler.h"
#include "../../../../util/telemetry/OpenTelemetryUtil.h"

using namespace std::chrono;

Logger triangleCount_logger;
bool isStatCollect = false;

std::mutex processStatusMutex;
std::mutex responseVectorMutex;
static std::mutex fileCombinationMutex;
static std::mutex aggregateWeightMutex;

constexpr time_t SCHEDULER_EXECUTION_GUARD_SECONDS = 8;
constexpr time_t SCHEDULER_NEXT_EXECUTION_OFFSET_SECONDS = 9;

static time_t last_exec_time = 0;

static string isFileAccessibleToWorker(std::string graphId, std::string partitionId, std::string aggregatorHostName,
                                       std::string aggregatorPort, std::string masterIP, std::string fileType,
                                       std::string fileName);
static long aggregateCentralStoreTriangles(SQLiteDBInterface *sqlite, std::string graphId, std::string masterIP,
                                        int threadPriority, const std::map<std::string, std::vector<string>,
                                        std::less<>> &partitionMap);
static int updateTriangleTreeAndGetTriangleCount(
    const std::vector<std::string> &triangles,
    std::unordered_map<long, std::unordered_map<long, std::unordered_set<long>>> *triangleTree_p,
    std::mutex *triangleTreeMutex_p);

static void insertProcessInfo(const ProcessInfo &processInformation) {
    const std::scoped_lock lock(processStatusMutex);
    processData.insert(processInformation);
}

static void removeProcessInfoById(int uniqueId) {
    const std::scoped_lock lock(processStatusMutex);
    for (auto processCompleteIterator = processData.begin(); processCompleteIterator != processData.end();
         ++processCompleteIterator) {
        if (processCompleteIterator->id == uniqueId) {
            processData.erase(processCompleteIterator);
            break;
        }
    }
}

static void joinAllThreads(std::vector<std::thread> &threads) {
    for (auto &thread : threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

TriangleCountExecutor::TriangleCountExecutor() {}

TriangleCountExecutor::TriangleCountExecutor(SQLiteDBInterface *db, PerformanceSQLiteDBInterface *perfDb,
                                             JobRequest jobRequest) {
    this->sqlite = db;
    this->perfDB = perfDb;
    this->request = jobRequest;
}

void allocate(int partitionId, const string &workerId, std::map<int, string> &allocation,
              std::set<int> &remainingPartitions, std::map<int, std::vector<string>> &availableWorkersByPartition,
              std::map<string, int, std::less<>> &workerLoads) {
    allocation[partitionId] = workerId;
    remainingPartitions.erase(partitionId);
    availableWorkersByPartition.erase(partitionId);
    workerLoads[workerId]++;
    if (workerLoads[workerId] >= 3) {
           for (auto iterator = availableWorkersByPartition.begin();
               iterator != availableWorkersByPartition.end(); iterator++) {
            auto &partitionWorkers = iterator->second;
            auto workerIterator = std::find(partitionWorkers.begin(), partitionWorkers.end(), workerId);
            if (workerIterator != partitionWorkers.end()) {
                partitionWorkers.erase(workerIterator);
            }
        }
    }
}

static int get_min_partition(std::set<int> &remainingPartitions,
                             std::map<int, std::vector<string>> &availableWorkersByPartition) {
    int selectedPartition = *remainingPartitions.begin();
    size_t minimum = 1000000000;
    for (auto iterator = remainingPartitions.begin(); iterator != remainingPartitions.end(); iterator++) {
        int partition = *iterator;
        auto &workers = availableWorkersByPartition[partition];
        if (workers.size() > 0 && workers.size() < minimum) {
            minimum = workers.size();
            selectedPartition = partition;
        }
    }
    return selectedPartition;
}

static const std::vector<int> LOAD_PREFERENCE = {2, 3, 1, 0};

int alloc_plan(std::map<int, string> &allocation, std::set<int> &remainingPartitions,
               std::map<int, std::vector<string>> &availableWorkersByPartition,
               std::map<string, int, std::less<>> &workerLoads) {
    for (bool done = false; !done;) {
        string workerId = "";
        done = true;
        int partitionId = *remainingPartitions.begin();
        for (auto iterator = remainingPartitions.begin(); iterator != remainingPartitions.end(); iterator++) {
            partitionId = *iterator;
            if (availableWorkersByPartition[partitionId].size() == 1) {
                workerId = availableWorkersByPartition[partitionId][0];
                done = false;
                break;
            }
        }
        if (!workerId.empty()) {
            allocate(partitionId, workerId, allocation, remainingPartitions, availableWorkersByPartition, workerLoads);
        }
    }
    if (remainingPartitions.empty()) return 0;
    int selectedPartition = get_min_partition(remainingPartitions, availableWorkersByPartition);
    auto &availableWorkers = availableWorkersByPartition[selectedPartition];
    if (availableWorkers.empty()) return (int)remainingPartitions.size();
    sort(availableWorkers.begin(), availableWorkers.end(), [&workerLoads](string &firstWorker, string &secondWorker) {
        return LOAD_PREFERENCE[workerLoads[firstWorker]] > LOAD_PREFERENCE[workerLoads[secondWorker]];
    });  // load=1 goes first and load=3 goes last. The order is 1,0,2,3 for 4 cores.
    struct best_alloc {
        std::map<int, string> alloc;
        std::set<int> remain;
        std::map<int, std::vector<string>> p_avail;
        std::map<string, int, std::less<>> loads;
    };
    int bestRemainingPartitions = remainingPartitions.size();
    struct best_alloc best = {.alloc = allocation, .remain = remainingPartitions,
                              .p_avail = availableWorkersByPartition, .loads = workerLoads};
    for (auto iterator = availableWorkers.begin(); iterator != availableWorkers.end(); iterator++) {
        string workerId = *iterator;
        auto allocationCopy = allocation;
        auto remainingPartitionsCopy = remainingPartitions;
        auto availableWorkersCopy = availableWorkersByPartition;
        auto workerLoadsCopy = workerLoads;
        allocate(selectedPartition, workerId, allocationCopy, remainingPartitionsCopy,
             availableWorkersCopy, workerLoadsCopy);
        int remainingCount = alloc_plan(allocationCopy, remainingPartitionsCopy, availableWorkersCopy, workerLoadsCopy);
        if (remainingCount == 0) {
            remainingPartitions.clear();
            availableWorkersByPartition.clear();
            allocation.insert(allocationCopy.begin(), allocationCopy.end());
            workerLoads.insert(workerLoadsCopy.begin(), workerLoadsCopy.end());
            return 0;
        }
        if (remainingCount < bestRemainingPartitions) {
            bestRemainingPartitions = remainingCount;
            best = {.alloc = allocationCopy, .remain = remainingPartitionsCopy,
                    .p_avail = availableWorkersCopy, .loads = workerLoadsCopy};
        }
    }
    allocation.insert(best.alloc.begin(), best.alloc.end());
    remainingPartitions.clear();
    remainingPartitions.insert(best.remain.begin(), best.remain.end());
    availableWorkersByPartition.clear();
    availableWorkersByPartition.insert(best.p_avail.begin(), best.p_avail.end());
    workerLoads.clear();
    workerLoads.insert(best.loads.begin(), best.loads.end());
    return bestRemainingPartitions;
}

std::vector<int> reallocate_parts(std::map<int, string> &allocation, std::set<int> &remainingPartitions,
                                  const std::map<int, std::vector<string>> &availableWorkersByPartition) {
    map<int, int> partitionCopyCounts;
        for (auto iterator = availableWorkersByPartition.begin();
            iterator != availableWorkersByPartition.end(); iterator++) {
        partitionCopyCounts[iterator->first] = iterator->second.size();
    }
    vector<int> remainingPartitionList(remainingPartitions.begin(), remainingPartitions.end());
    sort(remainingPartitionList.begin(), remainingPartitionList.end(),
         [&partitionCopyCounts](int &firstPartition, int &secondPartition) {
             return partitionCopyCounts[firstPartition] > partitionCopyCounts[secondPartition];
         });  // partitions with more copies goes first
    vector<int> allPartitions;
    for (auto iterator = partitionCopyCounts.begin(); iterator != partitionCopyCounts.end(); iterator++) {
        allPartitions.push_back(iterator->first);
    }
    sort(allPartitions.begin(), allPartitions.end(), [&partitionCopyCounts](int &firstPartition, int &secondPartition) {
        return partitionCopyCounts[firstPartition] < partitionCopyCounts[secondPartition];
    });  // partitions with fewer copies goes first
    vector<int> partitionsToCopy;
    while (!remainingPartitionList.empty()) {
        int partitionToCopy = remainingPartitionList.back();
        remainingPartitionList.pop_back();
        int copyCount = partitionCopyCounts[partitionToCopy];
        if (copyCount == 1) {
            partitionsToCopy.push_back(partitionToCopy);
            continue;
        }
        const auto &availableWorkers = availableWorkersByPartition.find(partitionToCopy)->second;
        bool needsDataPush = true;
        bool partitionCopyAdded = false;
        for (auto iterator = allPartitions.begin();
             iterator != allPartitions.end() && !partitionCopyAdded; iterator++) {
            int candidatePartition = *iterator;
            if (copyCount <= partitionCopyCounts[candidatePartition]) {
                // allPartitions are sorted by copy count
                partitionsToCopy.push_back(partitionToCopy);
                needsDataPush = false;
                partitionCopyAdded = true;
            } else if (allocation.find(candidatePartition) == allocation.end()) {
                continue;
            }
            auto assignedWorker = allocation[candidatePartition];
            if (std::find(availableWorkers.begin(), availableWorkers.end(), assignedWorker) != availableWorkers.end()) {
                allocation.erase(candidatePartition);
                allocation[partitionToCopy] = assignedWorker;
                remainingPartitionList.push_back(candidatePartition);
                needsDataPush = false;
                break;
            }
        }
        if (needsDataPush) partitionsToCopy.push_back(partitionToCopy);
    }
    return partitionsToCopy;
}

void scale_up(std::map<string, int, std::less<>> &workerLoads, map<string, string, std::less<>> &workers,
              int partitionsToCopy) {
    int currentLoad = 0;
    for (auto iterator = workerLoads.begin(); iterator != workerLoads.end(); iterator++) {
        currentLoad += iterator->second;
    }
    int requiredCores = partitionsToCopy + currentLoad - 3 * workerLoads.size();
    if (requiredCores < 0) {
        return;
    }
    int workersToAdd = requiredCores / 2 + 1;  // allocate a little more to prevent saturation
    if (requiredCores % 2 > 0) workersToAdd++;
    if (workersToAdd == 0) return;

    K8sWorkerController *k8sController = K8sWorkerController::getInstance();
    map<string, string> newWorkers = k8sController->scaleUp(workersToAdd);

    for (auto iterator = newWorkers.begin(); iterator != newWorkers.end(); iterator++) {
        workerLoads[iterator->first] = 0.1;
        workers[iterator->first] = iterator->second;
    }
}

int alloc_net_plan(std::map<int, string> &allocation, std::vector<int> &partitionsToAllocate,
                   std::map<int, std::pair<string, string>> &transfer,
                   std::map<string, int, std::less<>> &networkLoads,
                   std::map<string, int, std::less<>> &workerLoads,
                   const std::map<int, std::vector<string>> &availableWorkersByPartition,
                   int currentBestNetworkLoad) {
    int currentNetworkLoad = std::max_element(networkLoads.begin(), networkLoads.end(),
                                     [](const auto &firstLoad, const auto &secondLoad) {
                                         return firstLoad.second < secondLoad.second;
                                     })
                        ->second;
    if (currentNetworkLoad >= currentBestNetworkLoad) {
        return currentNetworkLoad;
    }
    if (partitionsToAllocate.empty()) {
        if (networkLoads.empty()) return 0;
        return currentNetworkLoad;
    }
    struct best_net_alloc {
        std::map<int, string> alloc;
        std::map<int, std::pair<string, string>> transfer;
        std::map<string, int, std::less<>> net_loads;
        std::map<string, int, std::less<>> loads;
    };
    int bestNetworkLoad = currentBestNetworkLoad;
    struct best_net_alloc bestPlan = {.transfer = transfer, .net_loads = networkLoads, .loads = workerLoads};
    int partitionId = partitionsToAllocate.back();
    partitionsToAllocate.pop_back();
    vector<string> candidateWorkers;
    for (auto iterator = workerLoads.begin(); iterator != workerLoads.end(); iterator++) {
        candidateWorkers.push_back(iterator->first);
    }
    sort(candidateWorkers.begin(), candidateWorkers.end(), [&workerLoads](string &firstWorker, string &secondWorker) {
        int firstLoad = workerLoads[firstWorker];
        int secondLoad = workerLoads[secondWorker];
        if (firstLoad < 3 || secondLoad < 3) return firstLoad < secondLoad;
        return firstLoad <= secondLoad;
    });  // load=1 goes first and load=3 goes last. The order is 1,0,2,3 for 4 cores.
    const auto &availableWorkers = availableWorkersByPartition.find(partitionId)->second;
    int minimumWorkerLoad = 100000000;
    for (auto iterator = workerLoads.begin(); iterator != workerLoads.end(); iterator++) {
        int workerLoad = iterator->second;
        if (minimumWorkerLoad > workerLoad) {
            minimumWorkerLoad = workerLoad;
        }
    }
    for (auto sourceIterator = availableWorkers.begin();
         sourceIterator != availableWorkers.end(); sourceIterator++) {
        auto sourceWorker = *sourceIterator;
        for (auto targetIterator = candidateWorkers.begin();
             targetIterator != candidateWorkers.end(); targetIterator++) {
            auto targetWorker = *targetIterator;
            int targetLoad = workerLoads[targetWorker];
            if (targetLoad > minimumWorkerLoad) continue;
            auto allocationCopy = allocation;
            auto partitionsCopy = partitionsToAllocate;
            auto transferCopy = transfer;
            auto networkLoadsCopy = networkLoads;
            auto workerLoadsCopy = workerLoads;
            if (sourceWorker != targetWorker) {
                transferCopy[partitionId] = {sourceWorker, targetWorker};  // assume
                networkLoadsCopy[sourceWorker]++;
                networkLoadsCopy[targetWorker]++;
            }
            allocationCopy[partitionId] = targetWorker;
            workerLoadsCopy[targetWorker]++;
            int newNetworkLoad = alloc_net_plan(allocationCopy, partitionsCopy, transferCopy, networkLoadsCopy,
                                                workerLoadsCopy, availableWorkersByPartition, bestNetworkLoad);
            if (newNetworkLoad < bestNetworkLoad) {
                bestNetworkLoad = newNetworkLoad;
                bestPlan = {.alloc = allocationCopy, .transfer = transferCopy,
                             .net_loads = networkLoadsCopy, .loads = workerLoadsCopy};
            }
        }
    }
    allocation.clear();
    allocation.insert(bestPlan.alloc.begin(), bestPlan.alloc.end());
    auto &bestTransfers = bestPlan.transfer;
    for (auto iterator = bestTransfers.begin(); iterator != bestTransfers.end(); iterator++) {
        transfer[iterator->first] = iterator->second;
    }
    networkLoads.clear();
    networkLoads.insert(bestPlan.net_loads.begin(), bestPlan.net_loads.end());
    workerLoads.clear();
    workerLoads.insert(bestPlan.loads.begin(), bestPlan.loads.end());
    return bestNetworkLoad;
}

static map<string, string, std::less<>> get_workers(SQLiteDBInterface *sqlite) {
    map<string, string, std::less<>> workers;
    const std::vector<vector<pair<string, string>>> &results =
        sqlite->runSelect("SELECT DISTINCT idworker,ip,server_port FROM worker;");
    for (size_t i = 0; i < results.size(); i++) {
        string workerId = results[i][0].second;
        string ip = results[i][1].second;
        string port = results[i][2].second;
        workers[workerId] = ip + ":" + port;
    }
    return workers;
}

static map<string, int, std::less<>> get_loads(const map<string, string, std::less<>> &workers) {
    map<string, int, std::less<>> loads;
    const map<string, string> &cpu_map = Utils::getMetricMap("cpu_usage");
    for (auto it = workers.begin(); it != workers.end(); it++) {
        auto workerId = it->first;
        auto worker = it->second;
        const auto workerLoadIt = cpu_map.find(worker);
        if (workerLoadIt != cpu_map.end()) {
            double load = stod(workerLoadIt->second.c_str());
            if (load < 0) load = 0;
            loads[workerId] = (int)(4 * load);
        } else {
            loads[workerId] = 0;
        }
    }
    return loads;
}

static void build_avail_partitions(const std::map<string, std::vector<string>, std::less<>> &partitionMap,
                                   std::map<int, std::vector<string>> &p_avail, std::set<int> &remain) {
    for (auto it = partitionMap.begin(); it != partitionMap.end(); it++) {
        auto worker = it->first;
        auto &partitions = it->second;
        for (auto partitionIt = partitions.begin(); partitionIt != partitions.end(); partitionIt++) {
            auto partition = stoi(*partitionIt);
            p_avail[partition].push_back(worker);
            remain.insert(partition);
        }
    }
}

static void filter_overloaded_workers(const map<string, int, std::less<>> &loads,
                                     std::map<int, std::vector<string>> &p_avail) {
    for (auto loadIt = loads.begin(); loadIt != loads.end(); loadIt++) {
        if (loadIt->second < 3) continue;
        auto w = loadIt->first;
        for (auto it = p_avail.begin(); it != p_avail.end(); it++) {
            auto &partitionWorkers = it->second;
            for (auto workerIt = partitionWorkers.begin(); workerIt != partitionWorkers.end();) {
                if (*workerIt == w) {
                    partitionWorkers.erase(workerIt);
                } else {
                    workerIt++;
                }
            }
        }
    }
}

static void execute_partition_transfers(SQLiteDBInterface *sqlite,
                                        const std::map<int, std::pair<string, string>> &transfer,
                                        const map<string, string, std::less<>> &workers,
                                        const string &graphId) {
    if (transfer.empty()) return;
    const std::vector<vector<pair<string, string>>> &workerData =
        sqlite->runSelect("SELECT DISTINCT ip,server_port,server_data_port FROM worker;");
    map<string, string> dataPortMap;  // "ip:port" => data_port
    for (const auto &row : workerData) {
        string ip = row[0].second;
        string port = row[1].second;
        string dport = row[2].second;
        dataPortMap[ip + ":" + port] = dport;
    }
    std::vector<std::thread> transferThreads;
    transferThreads.reserve(transfer.size());
    for (auto it = transfer.begin(); it != transfer.end(); it++) {
        auto partition = it->first;
        auto from_worker = it->second.first;
        auto to_worker = it->second.second;
        auto w_from = workers.find(from_worker)->second;
        auto w_to = workers.find(to_worker)->second;
        const auto &ip_port_from = Utils::split(w_from, ':');
        auto ip_from = ip_port_from[0];
        auto port_from = stoi(ip_port_from[1]);
        const auto &ip_port_to = Utils::split(w_to, ':');
        auto ip_to = ip_port_to[0];
        auto dport_to = stoi(dataPortMap[w_to]);
        transferThreads.emplace_back(&Utils::transferPartition, ip_from, port_from, ip_to,
                                     dport_to, graphId, to_string(partition), to_worker, sqlite);
    }
    for (auto &t : transferThreads) {
        t.join();
    }
}

void filter_partitions(std::map<string, std::vector<string>, std::less<>> &partitionMap, SQLiteDBInterface *sqlite,
                       const string &graphId) {
    auto workers = get_workers(sqlite);
    auto loads = get_loads(workers);

    std::map<int, std::vector<string>> p_avail;
    std::set<int> remain;
    build_avail_partitions(partitionMap, p_avail, remain);

    const std::map<int, std::vector<string>> originalAvailableWorkersByPartition = p_avail;

    filter_overloaded_workers(loads, p_avail);

    std::map<int, string> alloc;
    int unallocated = alloc_plan(alloc, remain, p_avail, loads);
    if (unallocated > 0) {
        triangleCount_logger.info(to_string(unallocated) + " partitions remaining after alloc_plan");
        auto partitionsToCopy = reallocate_parts(alloc, remain, originalAvailableWorkersByPartition);
        scale_up(loads, workers, partitionsToCopy.size());
        triangleCount_logger.info("Scale up completed");

        map<string, int, std::less<>> net_loads;
        for (auto it = loads.begin(); it != loads.end(); it++) {
            net_loads[it->first] = 0;
        }
        for (auto it = workers.begin(); it != workers.end(); it++) {
            if (loads.find(it->first) == loads.end()) {
                loads[it->first] = 3;
            }
        }

        std::map<int, std::pair<string, string>> transfer;
        alloc_net_plan(alloc, partitionsToCopy, transfer, net_loads, loads,
                   originalAvailableWorkersByPartition, 100000000);
        for (auto it = transfer.begin(); it != transfer.end(); it++) {
            auto partitionId = it->first;
            auto w_to = it->second.second;
            alloc[partitionId] = w_to;
        }

        execute_partition_transfers(sqlite, transfer, workers, graphId);
    }
    partitionMap.clear();
    for (auto it = alloc.begin(); it != alloc.end(); it++) {
        auto partition = it->first;
        auto worker = it->second;
        partitionMap[worker].push_back(to_string(partition));
    }
}


void TriangleCountExecutor::execute() {
    executeTriangleCount(sqlite, perfDB, request, TriangleCountCommandType::TRIANGLES,
                        ThreadingStrategy::THREAD_BASED, triangleCount_logger);
}

long TriangleCountExecutor::getTriangleCount(
    int graphId, std::string host, int port, int dataPort, int partitionId, std::string masterIP, int uniqueId,
    bool isCompositeAggregation, int threadPriority, std::vector<std::vector<string>> fileCombinations,
    std::map<std::string, std::string, std::less<>> *combinationWorkerMap_p,
    std::unordered_map<long, std::unordered_map<long, std::unordered_set<long>>> *triangleTree_p,
    std::mutex *triangleTreeMutex_p, const std::string& masterTraceContext) {

    OTEL_TRACE_OPERATION(OTelTraceOperations::WORKER_COMMUNICATION + host +
                         OTelTraceOperations::PARTITION +
                         std::to_string(partitionId));

    int sockfd;
    std::string data(INSTANCE_DATA_LENGTH + 1, '\0');
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;
    long triangleCount;
    int result_wr;
    string response;

    // Create socket
    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        triangleCount_logger.error("Cannot create socket");
        return 0;
    }

    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }

    triangleCount_logger.log("###TRIANGLE-COUNT-EXECUTOR### Get Host By Name : " + host, "info");

    // DNS resolution
    server = gethostbyname(host.c_str());
    if (server == NULL) {
        triangleCount_logger.error("ERROR, no host named " + host);
        return 0;
    }

    // Connection establishment
    memset((char *)&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    memcpy(&serv_addr.sin_addr.s_addr, server->h_addr, server->h_length);
    serv_addr.sin_port = htons(port);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        triangleCount_logger.error("ERROR connecting");
        return 0;
    }

    // Protocol handshake
    result_wr = write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(),
                      JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if (result_wr < 0) {
        triangleCount_logger.log("Error writing to socket", "error");
    }

    triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if (result_wr < 0) {
            triangleCount_logger.log("Error writing to socket", "error");
        }
        triangleCount_logger.log("Sent : " + masterIP, "info");

        response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        } else {
            triangleCount_logger.log("Received : " + response, "error");
        }
        result_wr = write(sockfd, JasmineGraphInstanceProtocol::TRIANGLES.c_str(),
                          JasmineGraphInstanceProtocol::TRIANGLES.size());

        if (result_wr < 0) {
            triangleCount_logger.log("Error writing to socket", "error");
        }
        triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::TRIANGLES, "info");

        response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, std::to_string(graphId).c_str(), std::to_string(graphId).size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }
            triangleCount_logger.log("Sent : Graph ID " + std::to_string(graphId), "info");

            response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, std::to_string(partitionId).c_str(), std::to_string(partitionId).size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }

            triangleCount_logger.log("Sent : Partition ID " + std::to_string(partitionId), "info");

            response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, std::to_string(threadPriority).c_str(), std::to_string(threadPriority).size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }
            triangleCount_logger.log("Sent : Thread Priority " + std::to_string(threadPriority), "info");

            response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);

            // Send trace context to worker for trace propagation
            if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
                triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");

                // Use the master trace context passed as parameter to ensure all workers inherit from same parent
                std::string traceContext = masterTraceContext;
                if (traceContext.empty()) {
                    traceContext = "NO_TRACE_CONTEXT";
                }

                // Send trace context for distributed tracing
                result_wr = write(sockfd, traceContext.c_str(), traceContext.size());

                if (result_wr < 0) {
                    triangleCount_logger.log("Error writing trace context to socket", "error");
                }

                triangleCount_logger.log("Sent : Trace Context " + traceContext, "info");
                response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
            }

            triangleCount_logger.log("Got response : |" + response + "|", "info");
            triangleCount = atol(response.c_str());
        }

        if (isCompositeAggregation) {
            triangleCount_logger.log("###COMPOSITE### Started Composite aggregation ", "info");
            for (int combinationIndex = 0; combinationIndex < fileCombinations.size(); ++combinationIndex) {
                const std::vector<string> &fileList = fileCombinations.at(combinationIndex);
                std::set<string> partitionIdSet;
                std::set<string> partitionSet;
                std::map<int, int> tempWeightMap;
                std::set<string> transferRequireFiles;
                std::string combinationKey = "";
                std::string availableFiles = "";
                std::string transferredFiles = "";
                bool isAggregateValid = false;

                for (auto listIterator = fileList.begin(); listIterator != fileList.end(); ++listIterator) {
                    std::string fileName = *listIterator;

                    size_t lastIndex = fileName.find_last_of(".");
                    string rawFileName = fileName.substr(0, lastIndex);

                    const std::vector<std::string> &fileNameParts = Utils::split(rawFileName, '_');

                    /*Partition numbers are extracted from  the file name. The starting index of partition number
                     * is 2. Therefore the loop starts with 2*/
                    for (int index = 2; index < fileNameParts.size(); ++index) {
                        partitionSet.insert(fileNameParts[index]);
                    }
                }

                if (partitionSet.find(std::to_string(partitionId)) == partitionSet.end()) {
                    continue;
                }

                if (!proceedOrNot(partitionSet, partitionId)) {
                    continue;
                }

                for (auto fileListIterator = fileList.begin(); fileListIterator != fileList.end(); ++fileListIterator) {
                    std::string fileName = *fileListIterator;
                    bool isTransferRequired = true;

                    combinationKey = fileName + ":" + combinationKey;

                    size_t lastindex = fileName.find_last_of(".");
                    string rawFileName = fileName.substr(0, lastindex);

                    std::vector<std::string> fileNameParts = Utils::split(rawFileName, '_');

                    for (int index = 2; index < fileNameParts.size(); ++index) {
                        if (fileNameParts[index] == std::to_string(partitionId)) {
                            isTransferRequired = false;
                        }
                        partitionIdSet.insert(fileNameParts[index]);
                    }

                    if (isTransferRequired) {
                        transferRequireFiles.insert(fileName);
                        transferredFiles = fileName + ":" + transferredFiles;
                    } else {
                        availableFiles = fileName + ":" + availableFiles;
                    }
                }

                std::string adjustedCombinationKey = combinationKey.substr(0, combinationKey.size() - 1);
                std::string adjustedAvailableFiles = availableFiles.substr(0, availableFiles.size() - 1);
                std::string adjustedTransferredFile = transferredFiles.substr(0, transferredFiles.size() - 1);

                fileCombinationMutex.lock();
                std::map<std::string, std::string, std::less<>> &combinationWorkerMap = *combinationWorkerMap_p;
                if (combinationWorkerMap.find(combinationKey) == combinationWorkerMap.end()) {
                    if (partitionIdSet.find(std::to_string(partitionId)) != partitionIdSet.end()) {
                        combinationWorkerMap[combinationKey] = std::to_string(partitionId);
                        isAggregateValid = true;
                    }
                }
                fileCombinationMutex.unlock();

                if (isAggregateValid) {
                    for (auto transferRequireFileIterator = transferRequireFiles.begin();
                         transferRequireFileIterator != transferRequireFiles.end(); ++transferRequireFileIterator) {
                        std::string transferFileName = *transferRequireFileIterator;
                        std::string fileAccessible = isFileAccessibleToWorker(
                            std::to_string(graphId), std::string(), host, std::to_string(port), masterIP,
                            JasmineGraphInstanceProtocol::FILE_TYPE_CENTRALSTORE_COMPOSITE, transferFileName);

                        if (fileAccessible.compare("false") == 0) {
                            copyCompositeCentralStoreToAggregator(host, std::to_string(port), std::to_string(dataPort),
                                                                  transferFileName, masterIP);
                        }
                    }

                    triangleCount_logger.log("###COMPOSITE### Retrieved Composite triangle list ", "debug");

                    const auto &triangles =
                        countCompositeCentralStoreTriangles(host, std::to_string(port), adjustedTransferredFile,
                                                            masterIP, adjustedAvailableFiles, threadPriority);
                    if (triangles.size() > 0) {
                        triangleCount +=
                            updateTriangleTreeAndGetTriangleCount(triangles, triangleTree_p, triangleTreeMutex_p);
                    }
                }
                updateMap(partitionId);
            }
        }

        triangleCount_logger.info("###COMPOSITE### Returning Total Triangles from executer ");
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return triangleCount;

    } else {
        triangleCount_logger.log("There was an error in the upload process and the response is :: " + response,
                                 "error");
    }
    Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
    close(sockfd);
    return 0;
}

bool TriangleCountExecutor::proceedOrNot(std::set<string> partitionSet, int partitionId) {
    const std::lock_guard<std::mutex> lock(aggregateWeightMutex);

    std::map<int, int> tempWeightMap;
    for (auto partitionSetIterator = partitionSet.begin(); partitionSetIterator != partitionSet.end();
         ++partitionSetIterator) {
        std::string partitionIdString = *partitionSetIterator;
        int currentPartitionId = atoi(partitionIdString.c_str());
        tempWeightMap[currentPartitionId] = aggregateWeightMap[currentPartitionId];
    }

    int currentWorkerWeight = tempWeightMap[partitionId];
    pair<int, int> entryWithMinValue = make_pair(partitionId, currentWorkerWeight);

    for (auto currentEntry = aggregateWeightMap.begin(); currentEntry != aggregateWeightMap.end(); ++currentEntry) {
        if (entryWithMinValue.second > currentEntry->second) {
            entryWithMinValue = make_pair(currentEntry->first, currentEntry->second);
        }
    }

    bool result = false;
    if (entryWithMinValue.first == partitionId) {
        int currentWeight = aggregateWeightMap[entryWithMinValue.first];
        currentWeight++;
        aggregateWeightMap[entryWithMinValue.first] = currentWeight;
        triangleCount_logger.log("###COMPOSITE### Aggregator Initiated : Partition ID: " + std::to_string(partitionId) +
                                     " Weight : " + std::to_string(currentWeight),
                                 "info");
        result = true;
    }

    return result;
}

void TriangleCountExecutor::updateMap(int partitionId) {
    const std::lock_guard<std::mutex> lock(aggregateWeightMutex);

    int currentWeight = aggregateWeightMap[partitionId];
    currentWeight--;
    aggregateWeightMap[partitionId] = currentWeight;
    triangleCount_logger.log("###COMPOSITE### Aggregator Completed : Partition ID: " + std::to_string(partitionId) +
                                 " Weight : " + std::to_string(currentWeight),
                             "info");
}

static int updateTriangleTreeAndGetTriangleCount(
    const std::vector<std::string> &triangles,
    std::unordered_map<long, std::unordered_map<long, std::unordered_set<long>>> *triangleTree_p,
    std::mutex *triangleTreeMutex_p) {
    OTEL_TRACE_FUNCTION();

    std::mutex &triangleTreeMutex = *triangleTreeMutex_p;
    const std::lock_guard<std::mutex> lock1(triangleTreeMutex);
    int aggregateCount = 0;
    auto &triangleTree = *triangleTree_p;

    triangleCount_logger.log("###COMPOSITE### Triangle Tree locked ", "debug");

    for (auto triangleIterator = triangles.begin(); triangleIterator != triangles.end(); ++triangleIterator) {
        std::string triangle = *triangleIterator;

        if (!triangle.empty() && triangle != "NILL") {
            std::vector<std::string> triangleVertexList = Utils::split(triangle, ',');

            long vertexOne = std::atol(triangleVertexList.at(0).c_str());
            long vertexTwo = std::atol(triangleVertexList.at(1).c_str());
            long vertexThree = std::atol(triangleVertexList.at(2).c_str());

            auto &itemRes = triangleTree[vertexOne];
            auto itemResIterator = itemRes.find(vertexTwo);
            if (itemResIterator != itemRes.end()) {
                auto &set2 = itemResIterator->second;
                auto set2Iter = set2.find(vertexThree);
                if (set2Iter == set2.end()) {
                    set2.insert(vertexThree);
                    aggregateCount++;
                }
            } else {
                triangleTree[vertexOne][vertexTwo].insert(vertexThree);
                aggregateCount++;
            }
        }
    }

    return aggregateCount;
}

static std::pair<std::vector<string>, std::map<string, string, std::less<>>> buildPartitionInfo(
    const std::map<string, std::vector<string>, std::less<>> &partitionMap) {
    std::vector<string> partitionsVector;
    std::map<string, string, std::less<>> partitionWorkerMap;  // partition_id => worker_id
    for (auto it = partitionMap.begin(); it != partitionMap.end(); it++) {
        const auto &parts = it->second;
        string worker = it->first;
        for (auto partsIt = parts.begin(); partsIt != parts.end(); partsIt++) {
            string partition = *partsIt;
            partitionWorkerMap[partition] = worker;
            partitionsVector.push_back(partition);
        }
    }
    return {partitionsVector, partitionWorkerMap};
}

static std::map<string, std::vector<string>, std::less<>> fetchWorkerDataMap(SQLiteDBInterface *sqlite) {
    const std::vector<vector<pair<string, string>>> &workerDataResult =
        sqlite->runSelect("SELECT DISTINCT idworker,ip,server_port,server_data_port FROM worker;");
    std::map<string, std::vector<string>, std::less<>> workerDataMap;  // worker_id => [ip,port,data_port]
    for (auto it = workerDataResult.begin(); it != workerDataResult.end(); it++) {
        const auto &ipPortDport = *it;
        string id = ipPortDport[0].second;
        string ip = ipPortDport[1].second;
        string port = ipPortDport[2].second;
        string dport = ipPortDport[3].second;
        workerDataMap[id] = {ip, port, dport};
    }
    return workerDataMap;
}

static std::pair<std::string, std::string> findMinWeightWorker(
    const std::vector<string> &partitionCombination,
    const std::map<string, string, std::less<>> &partitionWorkerMap,
    std::map<string, int, std::less<>> &workerWeightMap) {

    int minimumWeight = 0;
    std::string minWeightWorker;
    std::string minWeightWorkerPartition;

    for (auto partCombinationIterator = partitionCombination.begin();
         partCombinationIterator != partitionCombination.end(); partCombinationIterator++) {
        string part = *partCombinationIterator;
        auto iter = partitionWorkerMap.find(part);
        if (iter == partitionWorkerMap.end()) {
            continue;
        }
        string workerId = iter->second;
        auto workerWeightMapIterator = workerWeightMap.find(workerId);
        if (workerWeightMapIterator != workerWeightMap.end()) {
            int weight = workerWeightMapIterator->second;

            if (minimumWeight == 0 || minimumWeight > weight) {
                minimumWeight = weight + 1;
                minWeightWorker = workerId;
                minWeightWorkerPartition = part;
            }
        } else {
            minimumWeight = 1;
            minWeightWorker = workerId;
            minWeightWorkerPartition = part;
        }
    }
    workerWeightMap[minWeightWorker] = minimumWeight;
    return {minWeightWorker, minWeightWorkerPartition};
}

struct CopyContext {
    const std::string &graphId;
    const std::string &masterIP;
    const std::string &minWeightWorker;
    const std::string &minWeightWorkerPartition;
    const std::string &aggregatorIp;
    const std::string &aggregatorPort;
    const std::string &aggregatorDataPort;
};

static std::string buildPartitionIdListAndCopyFiles(
    const std::vector<string> &partitionCombination,
    const std::map<string, string, std::less<>> &partitionWorkerMap,
    const CopyContext &ctx) {

    std::string partitionIdList = "";
    std::vector<std::thread> remoteGraphCopyThreads;

    for (auto partitionCombinationIterator = partitionCombination.begin();
         partitionCombinationIterator != partitionCombination.end(); ++partitionCombinationIterator) {
        string part = *partitionCombinationIterator;
        auto iter = partitionWorkerMap.find(part);
        if (iter == partitionWorkerMap.end()) {
            continue;
        }
        string workerId = iter->second;

        if (part != ctx.minWeightWorkerPartition) {
            partitionIdList += part + ",";
        }
        if (workerId != ctx.minWeightWorker) {
            std::string centralStoreAvailable = isFileAccessibleToWorker(
                ctx.graphId, part, ctx.aggregatorIp, ctx.aggregatorPort, ctx.masterIP,
                JasmineGraphInstanceProtocol::FILE_TYPE_CENTRALSTORE_AGGREGATE, std::string());

            if (centralStoreAvailable.compare("false") == 0) {
                int graphIdInt = atoi(ctx.graphId.c_str());
                int partInt = atoi(part.c_str());
                std::string aggregatorIp = ctx.aggregatorIp;
                std::string aggregatorPort = ctx.aggregatorPort;
                std::string aggregatorDataPort = ctx.aggregatorDataPort;
                std::string masterIP = ctx.masterIP;
                remoteGraphCopyThreads.push_back(std::thread([aggregatorIp, aggregatorPort, aggregatorDataPort,
                    graphIdInt, partInt, masterIP]() {
                    TriangleCountExecutor::copyCentralStoreToAggregator(aggregatorIp,
                        aggregatorPort, aggregatorDataPort, graphIdInt, partInt, masterIP);
                }));
            }
        }
    }

    for (auto &thread : remoteGraphCopyThreads) {
        if (thread.joinable()) {
            thread.join();
        }
    }

    if (partitionIdList.empty()) {
        return "";
    }
    return partitionIdList.substr(0, partitionIdList.size() - 1);
}

long TriangleCountExecutor::aggregateCentralStoreTriangles(
    SQLiteDBInterface *sqlite, std::string graphId, std::string masterIP, int threadPriority,
    const std::map<string, std::vector<string>, std::less<>> &partitionMap) {
    OTEL_TRACE_FUNCTION();

    auto [partitionsVector, partitionWorkerMap] = buildPartitionInfo(partitionMap);

    const std::vector<std::vector<string>> &partitionCombinations = AbstractExecutor::getCombinations(partitionsVector);
    std::map<string, int, std::less<>> workerWeightMap;
    std::vector<std::thread> triangleCountThreads;
    std::vector<std::string> triangleCountResponse(partitionCombinations.size(), "");
    std::string result = "";
    long aggregatedTriangleCount = 0;

    std::map<string, std::vector<string>, std::less<>> workerDataMap = fetchWorkerDataMap(sqlite);

    int comboIndex = 0;
    for (auto partitonCombinationsIterator = partitionCombinations.begin();
         partitonCombinationsIterator != partitionCombinations.end(); partitonCombinationsIterator++) {
        const std::vector<string> &partitionCombination = *partitonCombinationsIterator;

        auto [minWeightWorker, minWeightWorkerPartition] = findMinWeightWorker(
            partitionCombination, partitionWorkerMap, workerWeightMap);

        const auto &workerData = workerDataMap[minWeightWorker];
        std::string aggregatorIp = workerData[0];
        std::string aggregatorPort = workerData[1];
        std::string aggregatorDataPort = workerData[2];

        std::string aggregatorPartitionId = minWeightWorkerPartition;

        CopyContext copyCtx{
            graphId,
            masterIP,
            minWeightWorker,
            minWeightWorkerPartition,
            aggregatorIp,
            aggregatorPort,
            aggregatorDataPort
        };

        std::string adjustedPartitionIdList = buildPartitionIdListAndCopyFiles(
            partitionCombination, partitionWorkerMap, copyCtx);

        // Capture current trace context before async call
        std::string currentTraceContext = OpenTelemetryUtil::getCurrentTraceContext();

        triangleCountThreads.push_back(std::thread([&triangleCountResponse, comboIndex, aggregatorPort, aggregatorIp,
            aggregatorPartitionId, adjustedPartitionIdList, graphId, masterIP, threadPriority, currentTraceContext]() {
            triangleCountResponse[comboIndex] = TriangleCountExecutor::countCentralStoreTriangles(aggregatorPort,
                aggregatorIp, aggregatorPartitionId, adjustedPartitionIdList, graphId, masterIP, threadPriority,
                currentTraceContext);
        }));
        comboIndex++;
    }

    for (auto &thread : triangleCountThreads) {
        if (thread.joinable()) {
            thread.join();
        }
    }

    for (const auto &res : triangleCountResponse) {
        result = result + ":" + res;
    }

    const std::vector<std::string> &triangles = Utils::split(result, ':');
    std::set<std::string, std::less<>> uniqueTriangleSet;
    for (auto triangleIterator = triangles.begin(); triangleIterator != triangles.end(); ++triangleIterator) {
        std::string triangle = *triangleIterator;

        if (!triangle.empty() && triangle != "NILL") {
            uniqueTriangleSet.insert(triangle);
        }
    }

    aggregatedTriangleCount = uniqueTriangleSet.size();
    uniqueTriangleSet.clear();

    return aggregatedTriangleCount;
}

static string isFileAccessibleToWorker(std::string graphId, std::string partitionId, std::string aggregatorHostName,
                                       std::string aggregatorPort, std::string masterIP, std::string fileType,
                                       std::string fileName) {
    int sockfd;
    std::string data(INSTANCE_DATA_LENGTH + 1, '\0');
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;
    string isFileAccessible = "false";

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        triangleCount_logger.error("Cannot create socket");
        return 0;
    }

    server = gethostbyname(aggregatorHostName.c_str());
    if (server == NULL) {
        triangleCount_logger.error("ERROR, no host named " + aggregatorHostName);
        return 0;
    }

    memset((char *)&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    memcpy(&serv_addr.sin_addr.s_addr, server->h_addr, server->h_length);
    serv_addr.sin_port = htons(atoi(aggregatorPort.c_str()));
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        triangleCount_logger.error("ERROR connecting");
        return 0;
    }

    int result_wr =
        write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if (result_wr < 0) {
        triangleCount_logger.log("Error writing to socket", "error");
    }
    triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");

    string response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if (result_wr < 0) {
            triangleCount_logger.log("Error writing to socket", "error");
        }
        triangleCount_logger.log("Sent : " + masterIP, "info");

        response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        } else {
            triangleCount_logger.log("Received : " + response, "error");
        }
        result_wr = write(sockfd, JasmineGraphInstanceProtocol::CHECK_FILE_ACCESSIBLE.c_str(),
                          JasmineGraphInstanceProtocol::CHECK_FILE_ACCESSIBLE.size());

        if (result_wr < 0) {
            triangleCount_logger.log("Error writing to socket", "error");
        }

        response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_TYPE) == 0) {
            result_wr = write(sockfd, fileType.c_str(), fileType.size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }

            response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
            if (fileType.compare(JasmineGraphInstanceProtocol::FILE_TYPE_CENTRALSTORE_AGGREGATE) == 0) {
                if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
                    result_wr = write(sockfd, graphId.c_str(), graphId.size());

                    if (result_wr < 0) {
                        triangleCount_logger.log("Error writing to socket", "error");
                    }

                    response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
                    if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
                        result_wr = write(sockfd, partitionId.c_str(), partitionId.size());

                        if (result_wr < 0) {
                            triangleCount_logger.log("Error writing to socket", "error");
                        }

                        isFileAccessible = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
                    }
                }
            } else if (fileType.compare(JasmineGraphInstanceProtocol::FILE_TYPE_CENTRALSTORE_COMPOSITE) == 0) {
                if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
                    size_t lastindex = fileName.find_last_of(".");
                    string rawname = fileName.substr(0, lastindex);
                    result_wr = write(sockfd, rawname.c_str(), rawname.size());

                    if (result_wr < 0) {
                        triangleCount_logger.log("Error writing to socket", "error");
                    }

                    isFileAccessible = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
                }
            }
        }
    }
    Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
    close(sockfd);
    return isFileAccessible;
}

std::string TriangleCountExecutor::copyCompositeCentralStoreToAggregator(std::string aggregatorHostName,
                                                                         std::string aggregatorPort,
                                                                         std::string aggregatorDataPort,
                                                                         std::string fileName, std::string masterIP) {
    int sockfd;
    std::string data(INSTANCE_DATA_LENGTH + 1, '\0');
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;
    std::string aggregatorFilePath = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.aggregatefolder");
    std::string aggregateStoreFile = aggregatorFilePath + "/" + fileName;

    int fileSize = Utils::getFileSize(aggregateStoreFile);
    std::string fileLength = to_string(fileSize);

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        triangleCount_logger.error("Cannot create socket");
        return 0;
    }

    if (aggregatorHostName.find('@') != std::string::npos) {
        aggregatorHostName = Utils::split(aggregatorHostName, '@')[1];
    }

    server = gethostbyname(aggregatorHostName.c_str());
    if (server == NULL) {
        triangleCount_logger.error("ERROR, no host named " + aggregatorHostName);
        return 0;
    }

    memset((char *)&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    memcpy(&serv_addr.sin_addr.s_addr, server->h_addr, server->h_length);
    serv_addr.sin_port = htons(atoi(aggregatorPort.c_str()));
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        triangleCount_logger.error("ERROR connecting");
        return 0;
    }

    int result_wr =
        write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if (result_wr < 0) {
        triangleCount_logger.log("Error writing to socket", "error");
    }
    triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");

    string response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if (result_wr < 0) {
            triangleCount_logger.log("Error writing to socket", "error");
        }
        triangleCount_logger.log("Sent : " + masterIP, "info");

        response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        } else {
            triangleCount_logger.log("Received : " + response, "error");
        }
        result_wr = write(sockfd, JasmineGraphInstanceProtocol::SEND_COMPOSITE_CENTRALSTORE_TO_AGGREGATOR.c_str(),
                          JasmineGraphInstanceProtocol::SEND_COMPOSITE_CENTRALSTORE_TO_AGGREGATOR.size());

        if (result_wr < 0) {
            triangleCount_logger.log("Error writing to socket", "error");
        }
        triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_COMPOSITE_CENTRALSTORE_TO_AGGREGATOR,
                                 "info");

        response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_NAME) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME, "info");
            result_wr = write(sockfd, fileName.c_str(), fileName.size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }
            triangleCount_logger.log("Sent : File Name " + fileName, "info");

            response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
            if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_LEN) == 0) {
                triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");
                result_wr = write(sockfd, fileLength.c_str(), fileLength.size());

                if (result_wr < 0) {
                    triangleCount_logger.log("Error writing to socket", "error");
                }
                triangleCount_logger.log("Sent : File Length: " + fileLength, "info");

                response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
                if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_CONT) == 0) {
                    triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_CONT, "info");
                    triangleCount_logger.log("Going to send file through service", "info");
                    Utils::sendFileThroughService(aggregatorHostName, std::atoi(aggregatorDataPort.c_str()), fileName,
                                                  aggregateStoreFile);
                }
            }
        }

        int count = 0;

        while (true) {
            result_wr = write(sockfd, JasmineGraphInstanceProtocol::FILE_RECV_CHK.c_str(),
                              JasmineGraphInstanceProtocol::FILE_RECV_CHK.size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }

            triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::FILE_RECV_CHK, "info");
            triangleCount_logger.log("Checking if file is received", "info");

            response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
            if (response.compare(JasmineGraphInstanceProtocol::FILE_RECV_WAIT) == 0) {
                triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_RECV_WAIT, "info");
                triangleCount_logger.log("Checking file status : " + to_string(count), "info");
                count++;
                sleep(1);
                continue;
            } else if (response.compare(JasmineGraphInstanceProtocol::FILE_ACK) == 0) {
                triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_ACK, "info");
                triangleCount_logger.log("File transfer completed for file : " + aggregateStoreFile, "info");
                break;
            } else {
                triangleCount_logger.error("Invalid response " + response);
            }
        }

        // Next we wait till the batch upload completes
        while (true) {
            result_wr = write(sockfd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.c_str(),
                              JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }
            triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");

            response = Utils::read_str_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
            if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT) == 0) {
                triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT, "info");
                sleep(1);
                continue;
            } else if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK) == 0) {
                triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK, "info");
                triangleCount_logger.log("CentralStore partition file upload completed", "info");
                break;
            }
        }
    } else {
        triangleCount_logger.log("There was an error in the upload process and the response is :: " + response,
                                 "error");
    }
    Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
    close(sockfd);
    return response;
}

std::vector<string> TriangleCountExecutor::countCompositeCentralStoreTriangles(
    std::string aggregatorHostName, std::string aggregatorPort, std::string compositeCentralStoreFileList,
    std::string masterIP, std::string availableFileList, int threadPriority) {
    int sockfd;
    std::string data(INSTANCE_DATA_LENGTH + 1, '\0');
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        triangleCount_logger.error("Cannot create socket");
        return {};
    }

    server = gethostbyname(aggregatorHostName.c_str());
    if (server == NULL) {
        triangleCount_logger.error("ERROR, no host named " + aggregatorHostName);
        return {};
    }

    memset((char *)&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    memcpy(&serv_addr.sin_addr.s_addr, server->h_addr, server->h_length);
    serv_addr.sin_port = htons(atoi(aggregatorPort.c_str()));
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        triangleCount_logger.error("ERROR connecting");
        return {};
    }

    int result_wr =
        write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if (result_wr < 0) {
        triangleCount_logger.log("Error writing to socket", "error");
    }
    triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");

    string response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if (result_wr < 0) {
            triangleCount_logger.log("Error writing to socket", "error");
        }
        triangleCount_logger.log("Sent : " + masterIP, "info");
        triangleCount_logger.log("Port : " + aggregatorPort, "info");

        response = Utils::read_str_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        } else {
            triangleCount_logger.log("Received : " + response, "error");
        }
        result_wr = write(sockfd, JasmineGraphInstanceProtocol::AGGREGATE_COMPOSITE_CENTRALSTORE_TRIANGLES.c_str(),
                          JasmineGraphInstanceProtocol::AGGREGATE_COMPOSITE_CENTRALSTORE_TRIANGLES.size());

        if (result_wr < 0) {
            triangleCount_logger.log("Error writing to socket", "error");
        }
        triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::AGGREGATE_COMPOSITE_CENTRALSTORE_TRIANGLES,
                                 "info");

        response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, availableFileList.c_str(), availableFileList.size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }

            triangleCount_logger.log("Sent : Available File List " + availableFileList, "info");

            response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");

            std::vector<std::string> chunksVector;

            for (unsigned i = 0; i < compositeCentralStoreFileList.length(); i += INSTANCE_DATA_LENGTH - 10) {
                std::string chunk = compositeCentralStoreFileList.substr(i, INSTANCE_DATA_LENGTH - 10);
                if (i + INSTANCE_DATA_LENGTH - 10 < compositeCentralStoreFileList.length()) {
                    chunk += "/SEND";
                } else {
                    chunk += "/CMPT";
                }
                chunksVector.push_back(chunk);
            }

            for (int loopCount = 0; loopCount < chunksVector.size(); loopCount++) {
                if (loopCount == 0) {
                    std::string chunk = chunksVector.at(loopCount);
                    write(sockfd, chunk.c_str(), chunk.size());
                } else {
                    string chunkStatus = Utils::read_str_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
                    std::string chunk = chunksVector.at(loopCount);
                    write(sockfd, chunk.c_str(), chunk.size());
                }
            }

            response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, std::to_string(threadPriority).c_str(), std::to_string(threadPriority).size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }

            triangleCount_logger.log("Sent : Thread Priority " + std::to_string(threadPriority), "info");

            response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
            string status = response.substr(response.size() - 5);
            std::basic_ostringstream<char> resultStream;
            resultStream << response.substr(0, response.size() - 5);
            while (status.compare("/SEND") == 0) {
                result_wr = write(sockfd, status.c_str(), status.size());

                if (result_wr < 0) {
                    triangleCount_logger.log("Error writing to socket", "error");
                }
                response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
                status = response.substr(response.size() - 5);
                resultStream << response.substr(0, response.size() - 5);
            }
            response = resultStream.str();
        }

        triangleCount_logger.log("Aggregate Response Received", "info");

    } else {
        triangleCount_logger.log("There was an error in the upload process and the response is :: " + response,
                                 "error");
    }
    Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
    close(sockfd);
    return Utils::split(response, ':');
}

std::string TriangleCountExecutor::copyCentralStoreToAggregator(std::string aggregatorHostName,
                                                                std::string aggregatorPort,
                                                                std::string aggregatorDataPort, int graphId,
                                                                int partitionId, std::string masterIP) {
    int sockfd;
    std::string data(INSTANCE_DATA_LENGTH + 1, '\0');
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;
    std::string aggregatorDirPath = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.aggregatefolder");
    std::string fileName = std::to_string(graphId) + "_centralstore_" + std::to_string(partitionId) + ".gz";
    std::string centralStoreFile = aggregatorDirPath + "/" + fileName;

    int fileSize = Utils::getFileSize(centralStoreFile);
    std::string fileLength = to_string(fileSize);

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        triangleCount_logger.error("Cannot create socket");
        return "";
    }

    server = gethostbyname(aggregatorHostName.c_str());
    if (server == NULL) {
        triangleCount_logger.error("ERROR, no host named " + aggregatorHostName);
        return "";
    }

    memset((char *)&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    memcpy(&serv_addr.sin_addr.s_addr, server->h_addr, server->h_length);
    serv_addr.sin_port = htons(atoi(aggregatorPort.c_str()));
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        triangleCount_logger.error("ERROR connecting");
        return "";
    }

    int result_wr =
        write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if (result_wr < 0) {
        triangleCount_logger.log("Error writing to socket", "error");
    }
    triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");

    string response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if (result_wr < 0) {
            triangleCount_logger.log("Error writing to socket", "error");
        }
        triangleCount_logger.log("Sent : " + masterIP, "info");

        response = Utils::read_str_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        } else {
            triangleCount_logger.log("Received : " + response, "error");
        }
        result_wr = write(sockfd, JasmineGraphInstanceProtocol::SEND_CENTRALSTORE_TO_AGGREGATOR.c_str(),
                          JasmineGraphInstanceProtocol::SEND_CENTRALSTORE_TO_AGGREGATOR.size());

        if (result_wr < 0) {
            triangleCount_logger.log("Error writing to socket", "error");
        }
        triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_CENTRALSTORE_TO_AGGREGATOR, "info");

        response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_NAME) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME, "info");
            result_wr = write(sockfd, fileName.c_str(), fileName.size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }
            triangleCount_logger.log("Sent : File Name " + fileName, "info");

            response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
            if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_LEN) == 0) {
                triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");
                result_wr = write(sockfd, fileLength.c_str(), fileLength.size());

                if (result_wr < 0) {
                    triangleCount_logger.log("Error writing to socket", "error");
                }
                triangleCount_logger.log("Sent : File Length: " + fileLength, "info");

                response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
                if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_CONT) == 0) {
                    triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_CONT, "info");
                    triangleCount_logger.log("Going to send file through service", "info");
                    Utils::sendFileThroughService(aggregatorHostName, std::atoi(aggregatorDataPort.c_str()), fileName,
                                                  centralStoreFile);
                }
            }
        }

        int count = 0;

        while (true) {
            result_wr = write(sockfd, JasmineGraphInstanceProtocol::FILE_RECV_CHK.c_str(),
                              JasmineGraphInstanceProtocol::FILE_RECV_CHK.size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }

            triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::FILE_RECV_CHK, "info");
            triangleCount_logger.log("Checking if file is received", "info");

            response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
            if (response.compare(JasmineGraphInstanceProtocol::FILE_RECV_WAIT) == 0) {
                triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_RECV_WAIT, "info");
                triangleCount_logger.log("Checking file status : " + to_string(count), "info");
                count++;
                sleep(1);
                continue;
            } else if (response.compare(JasmineGraphInstanceProtocol::FILE_ACK) == 0) {
                triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_ACK, "info");
                triangleCount_logger.log("File transfer completed for file : " + centralStoreFile, "info");
                break;
            } else {
                triangleCount_logger.error("Invalid response " + response);
            }
        }

        // Next we wait till the batch upload completes
        while (true) {
            result_wr = write(sockfd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.c_str(),
                              JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }
            triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");

            response = Utils::read_str_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
            if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT) == 0) {
                triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT, "info");
                sleep(1);
                continue;
            } else if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK) == 0) {
                triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK, "info");
                triangleCount_logger.log("CentralStore partition file upload completed", "info");
                break;
            }
        }
    } else {
        triangleCount_logger.log("There was an error in the upload process and the response is :: " + response,
                                 "error");
    }
    Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
    close(sockfd);
    return response;
}

string TriangleCountExecutor::countCentralStoreTriangles(std::string aggregatorPort, std::string host,
                                                         std::string partitionId, std::string partitionIdList,
                                                         std::string graphId, std::string masterIP,
                                                         int threadPriority, std::string traceContext) {
    // Set the trace context in this async thread
    OpenTelemetryUtil::receiveAndSetTraceContext(traceContext, "async worker communication");
    int sockfd;
    std::string data(INSTANCE_DATA_LENGTH + 1, '\0');
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        triangleCount_logger.error("Cannot create socket");
        return 0;
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        triangleCount_logger.error("ERROR, no host named " + host);
        return 0;
    }

    memset((char *)&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    memcpy(&serv_addr.sin_addr.s_addr, server->h_addr, server->h_length);
    serv_addr.sin_port = htons(atoi(aggregatorPort.c_str()));
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        triangleCount_logger.error("ERROR connecting");
        return 0;
    }

    int result_wr =
        write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if (result_wr < 0) {
        triangleCount_logger.log("Error writing to socket", "error");
    }
    triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");

    string response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if (result_wr < 0) {
            triangleCount_logger.log("Error writing to socket", "error");
        }
        triangleCount_logger.log("Sent : " + masterIP, "info");

        response = Utils::read_str_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        } else {
            triangleCount_logger.log("Received : " + response, "error");
        }
        result_wr = write(sockfd, JasmineGraphInstanceProtocol::AGGREGATE_CENTRALSTORE_TRIANGLES.c_str(),
                          JasmineGraphInstanceProtocol::AGGREGATE_CENTRALSTORE_TRIANGLES.size());

        if (result_wr < 0) {
            triangleCount_logger.log("Error writing to socket", "error");
        }
        triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::AGGREGATE_CENTRALSTORE_TRIANGLES, "info");

        response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, graphId.c_str(), graphId.size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }
            triangleCount_logger.log("Sent : Graph ID " + graphId, "info");

            response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK + " (after graph ID)", "info");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, partitionId.c_str(), partitionId.size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }
            triangleCount_logger.log("Sent : Partition ID " + partitionId, "info");

            response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, partitionIdList.c_str(), partitionIdList.size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }
            triangleCount_logger.log("Sent : Partition ID List : " + partitionIdList, "info");

            response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, std::to_string(threadPriority).c_str(), std::to_string(threadPriority).size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }

            triangleCount_logger.log("Sent : Thread Priority " + std::to_string(threadPriority), "info");

            response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK +
                                   " (after thread priority)", "info");

            // Get current trace context and send it to worker for aggregation tracing
            std::string traceContext = OpenTelemetryUtil::getCurrentTraceContext();

            result_wr = write(sockfd, traceContext.c_str(), traceContext.size());
            if (result_wr < 0) {
                triangleCount_logger.log("Error writing trace context to socket", "error");
            }
            triangleCount_logger.log("Sent trace context: " + traceContext, "info");

            response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
            string status = response.substr(response.size() - 5);
            std::basic_ostringstream<char> resultStream;
            resultStream << response.substr(0, response.size() - 5);

            while (status == "/SEND") {
                result_wr = write(sockfd, status.c_str(), status.size());

                if (result_wr < 0) {
                    triangleCount_logger.log("Error writing to socket", "error");
                }
                response = Utils::read_str_trim_wrapper(sockfd, data.data(), INSTANCE_DATA_LENGTH);
                status = response.substr(response.size() - 5);
                resultStream << response.substr(0, response.size() - 5);
            }
            response = resultStream.str();
            }
        }
    } else {
        triangleCount_logger.log("There was an error in the upload process and the response is :: " + response,
                                 "error");
    }
    Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
    close(sockfd);
    return response;
}

int TriangleCountExecutor::getUid() {
    static std::atomic<std::uint32_t> uid{0};
    return ++uid;
}

static std::map<string, std::vector<string>, std::less<>> buildPartitionMap(
    SQLiteDBInterface *sqlite, const std::string &graphId, const std::string &logPrefix, Logger &logger) {
    string sqlStatement =
        "SELECT DISTINCT worker_idworker,partition_idpartition "
        "FROM worker_has_partition INNER JOIN worker ON worker_has_partition.worker_idworker=worker.idworker "
        "WHERE partition_graph_idgraph=" + graphId + ";";

    const std::vector<vector<pair<string, string>>> &results = sqlite->runSelect(sqlStatement);
    std::map<string, std::vector<string>, std::less<>> partitionMap;

    for (auto i = results.begin(); i != results.end(); ++i) {
        const std::vector<pair<string, string>> &rowData = *i;
        string workerID = rowData.at(0).second;
        string partitionId = rowData.at(1).second;
        if (partitionMap.find(workerID) == partitionMap.end()) {
            std::vector<string> partitionVec;
            partitionVec.push_back(partitionId);
            partitionMap[workerID] = partitionVec;
        } else {
            partitionMap[workerID].push_back(partitionId);
        }
        logger.info(logPrefix + " Getting Triangle Count : PartitionId " + partitionId);
    }

    if (jasminegraph_profile == PROFILE_K8S) {
        std::unique_ptr<K8sInterface> k8sInterface(new K8sInterface());
        if (k8sInterface->getJasmineGraphConfig("AUTO_SCALING_ENABLED") == "true") {
            filter_partitions(partitionMap, sqlite, graphId);
        }
    }
    return partitionMap;
}

static std::vector<std::vector<string>> getCompositeFileCombinations(const std::string &graphId) {
    std::vector<std::string> compositeCentralStoreFiles;
    std::string aggregatorFilePath = Utils::getJasmineGraphProperty(
        "org.jasminegraph.server.instance.aggregatefolder");
    std::vector<std::string> graphFiles = Utils::getListOfFilesInDirectory(aggregatorFilePath);
    std::string compositeFileNameFormat = graphId + "_compositecentralstore_";
    for (auto graphFilesIterator = graphFiles.begin(); graphFilesIterator != graphFiles.end();
         ++graphFilesIterator) {
        std::string graphFileName = *graphFilesIterator;
        if ((graphFileName.find(compositeFileNameFormat) == 0) &&
            (graphFileName.find(".gz") != std::string::npos)) {
            compositeCentralStoreFiles.push_back(graphFileName);
        }
    }
    return AbstractExecutor::getCombinations(compositeCentralStoreFiles);
}

struct WorkerTaskContext {
    int graphIdInt;
    std::string masterIP;
    int uniqueId;
    bool isCompositeAggregation;
    int threadPriority;
    const std::vector<std::vector<string>> &fileCombinations;
    std::map<std::string, std::string, std::less<>> &combinationWorkerMap;
    std::unordered_map<long, std::unordered_map<long, std::unordered_set<long>>> &triangleTree;
    std::mutex &triangleTreeMutex;
    std::string masterTraceContext;
};

struct TaskCollector {
    std::vector<std::tuple<string, string, string>> &workerTaskInfo;
    std::vector<std::thread> &intermThreads;
    std::vector<long> &intermResThread;
    std::vector<std::future<long>> &intermResFuture;
    ThreadingStrategy strategy;
};

static int distributeTasksToWorkers(
    SQLiteDBInterface *sqlite,
    const std::map<string, std::vector<string>, std::less<>> &partitionMap,
    const WorkerTaskContext &taskCtx,
    TaskCollector &collector) {

    vector<Utils::worker> workerList = Utils::getWorkerList(sqlite);
    int workerListSize = workerList.size();
    int partitionCount = 0;

    for (int i = 0; i < workerListSize; i++) {
        Utils::worker currentWorker = workerList.at(i);
        string host = currentWorker.hostname;
        string workerID = currentWorker.workerID;
        int workerPort = atoi(string(currentWorker.port).c_str());
        int workerDataPort = atoi(string(currentWorker.dataPort).c_str());
        triangleCount_logger.info("worker_" + workerID + " host=" + host + ":" + to_string(workerPort) + ":" +
                    to_string(workerDataPort));

        auto partitionIter = partitionMap.find(workerID);
        if (partitionIter == partitionMap.end()) {
            continue;
        }
        const std::vector<string> &partitionList = partitionIter->second;
        for (auto partitionIterator = partitionList.begin(); partitionIterator != partitionList.end();
             ++partitionIterator) {
            string partitionId = *partitionIterator;
            triangleCount_logger.info("> partition" + partitionId);
            collector.workerTaskInfo.emplace_back(workerID, partitionId, host);
            OTEL_TRACE_OPERATION(OTelTraceOperations::DISTRIBUTE_TO_WORKER + workerID +
                                 OTelTraceOperations::PARTITION +
                                 partitionId);
            int partitionIdInt = atoi(partitionId.c_str());

            if (collector.strategy == ThreadingStrategy::THREAD_BASED) {
                int taskIndex = partitionCount;
                collector.intermResThread.push_back(0);

                std::map<std::string, std::string, std::less<>> *combinationWorkerMapPtr =
                    &taskCtx.combinationWorkerMap;
                std::unordered_map<long, std::unordered_map<long, std::unordered_set<long>>> *triangleTreePtr =
                    &taskCtx.triangleTree;
                std::mutex *triangleTreeMutexPtr = &taskCtx.triangleTreeMutex;
                std::vector<std::vector<string>> fileCombinationsForTask = taskCtx.fileCombinations;
                std::string masterTraceContextForTask = taskCtx.masterTraceContext;
                std::vector<long> &threadResultBuffer = collector.intermResThread;
                std::string masterIp = taskCtx.masterIP;
                int graphIdForTask = taskCtx.graphIdInt;
                int uniqueRequestId = taskCtx.uniqueId;
                bool isCompositeAggregationEnabled = taskCtx.isCompositeAggregation;
                int workerThreadPriority = taskCtx.threadPriority;

                collector.intermThreads.push_back(std::thread(
                    [&threadResultBuffer, taskIndex, graphIdForTask, host, workerPort, workerDataPort,
                     partitionIdInt, masterIp, uniqueRequestId, isCompositeAggregationEnabled,
                     workerThreadPriority, fileCombinationsForTask, combinationWorkerMapPtr, triangleTreePtr,
                     triangleTreeMutexPtr, masterTraceContextForTask]() {
                        threadResultBuffer[taskIndex] = TriangleCountExecutor::getTriangleCount(graphIdForTask, host,
                            workerPort, workerDataPort, partitionIdInt, masterIp, uniqueRequestId,
                            isCompositeAggregationEnabled, workerThreadPriority, fileCombinationsForTask,
                            combinationWorkerMapPtr, triangleTreePtr, triangleTreeMutexPtr, masterTraceContextForTask);
                }));
            } else {
                std::map<std::string, std::string, std::less<>> *combinationWorkerMapPtr =
                    &taskCtx.combinationWorkerMap;
                std::unordered_map<long, std::unordered_map<long, std::unordered_set<long>>> *triangleTreePtr =
                    &taskCtx.triangleTree;
                std::mutex *triangleTreeMutexPtr = &taskCtx.triangleTreeMutex;
                std::vector<std::vector<string>> fileCombinationsForTask = taskCtx.fileCombinations;
                std::string masterTraceContextForTask = taskCtx.masterTraceContext;
                std::string masterIp = taskCtx.masterIP;
                int graphIdForTask = taskCtx.graphIdInt;
                int uniqueRequestId = taskCtx.uniqueId;
                bool isCompositeAggregationEnabled = taskCtx.isCompositeAggregation;
                int workerThreadPriority = taskCtx.threadPriority;

                std::packaged_task<long()> task([graphIdForTask, host, workerPort, workerDataPort,
                    partitionIdInt, masterIp, uniqueRequestId, isCompositeAggregationEnabled, workerThreadPriority,
                    fileCombinationsForTask, combinationWorkerMapPtr, triangleTreePtr, triangleTreeMutexPtr,
                    masterTraceContextForTask]() {
                        return TriangleCountExecutor::getTriangleCount(graphIdForTask, host,
                            workerPort, workerDataPort, partitionIdInt, masterIp, uniqueRequestId,
                            isCompositeAggregationEnabled, workerThreadPriority, fileCombinationsForTask,
                            combinationWorkerMapPtr, triangleTreePtr, triangleTreeMutexPtr, masterTraceContextForTask);
                });
                collector.intermResFuture.push_back(task.get_future());
                collector.intermThreads.push_back(std::thread(std::move(task)));
            }
            partitionCount++;
        }
    }
    return partitionCount;
}

static std::vector<std::thread> handleSLACalibration(
    PerformanceSQLiteDBInterface *perfDB,
    const std::string &graphId,
    int partitionCount,
    const std::string &commandName,
    const std::string &masterIP,
    bool autoCalibrate,
    bool &canCalibrate) {

    PerformanceUtil::init();
    std::string logPrefix = (commandName == TRIANGLES) ? "###TRIANGLE-COUNT-EXECUTOR###"
                                                       : "###SHEEP-TRIANGLE-COUNT-EXECUTOR###";
    std::string query = "SELECT attempt from graph_sla INNER JOIN sla_category where "
                        "graph_sla.id_sla_category=sla_category.id and graph_sla.graph_id='" + graphId +
                        "' and graph_sla.partition_count='" + std::to_string(partitionCount) +
                        "' and sla_category.category='" + Conts::SLA_CATEGORY::LATENCY +
                        "' and sla_category.command='" + commandName + "';";
    const std::vector<vector<pair<string, string>>> &queryResults = perfDB->runSelect(query);
    std::vector<std::thread> statThreads;

    if (queryResults.size() > 0) {
        std::string attemptString = queryResults[0][0].second;
        int calibratedAttempts = atoi(attemptString.c_str());
        if (calibratedAttempts >= Conts::MAX_SLA_CALIBRATE_ATTEMPTS) {
            canCalibrate = false;
        }
    } else {
        triangleCount_logger.log(logPrefix + " Inserting initial record for SLA ", "info");
        Utils::updateSLAInformation(perfDB, graphId, partitionCount, 0, commandName, Conts::SLA_CATEGORY::LATENCY);
        statThreads.push_back(std::thread([perfDB, graphId, commandName, partitionCount, masterIP, autoCalibrate]() {
            AbstractExecutor::collectPerformaceData(perfDB, graphId, commandName, Conts::SLA_CATEGORY::LATENCY,
                                                   partitionCount, masterIP, autoCalibrate);
        }));
        isStatCollect = true;
    }
    return statThreads;
}

static long gatherWorkerResults(
    ThreadingStrategy strategy,
    int uniqueId,
    const std::vector<std::tuple<string, string, string>> &workerTaskInfo,
    std::vector<std::thread> &intermThreads,
    const std::vector<long> &intermResThread,
    std::vector<std::future<long>> &intermResFuture,
    Logger &logger) {

    long totalResult = 0;
    int taskIndex = 0;
    if (strategy == ThreadingStrategy::THREAD_BASED) {
        for (auto &intermThread : intermThreads) {
            const auto& [workerID, partitionId, host] = workerTaskInfo[taskIndex];
            OTEL_TRACE_OPERATION(OTelTraceOperations::WAIT_FOR_WORKER + workerID +
                                 OTelTraceOperations::PARTITION + partitionId +
                                 OTelTraceOperations::ON + host);
            logger.info("Waiting for result from worker_" + workerID + " partition_" + partitionId +
                        " host_" + host + " uuid=" + to_string(uniqueId));

            if (intermThread.joinable()) {
                intermThread.join();
            }

            long worker_result = intermResThread[taskIndex];
            logger.info("Received result " + std::to_string(worker_result) + " from worker_" +
                        workerID + " partition_" + partitionId);
            OTEL_TRACE_OPERATION(OTelTraceOperations::AGGREGATE_RESULT_WORKER + workerID +
                                 OTelTraceOperations::PARTITION + partitionId);
            totalResult += worker_result;
            taskIndex++;
        }
    } else {
        for (auto &&futureCall : intermResFuture) {
            const auto& [workerID, partitionId, host] = workerTaskInfo[taskIndex];
            OTEL_TRACE_OPERATION(OTelTraceOperations::WAIT_FOR_WORKER + workerID +
                                 OTelTraceOperations::PARTITION + partitionId +
                                 OTelTraceOperations::ON + host);
            logger.info("Waiting for result from worker_" + workerID + " partition_" + partitionId +
                        " host_" + host + " uuid=" + to_string(uniqueId));
            long worker_result = futureCall.get();
            logger.info("Received result " + std::to_string(worker_result) + " from worker_" +
                        workerID + " partition_" + partitionId);
            OTEL_TRACE_OPERATION(OTelTraceOperations::AGGREGATE_RESULT_WORKER + workerID +
                                 OTelTraceOperations::PARTITION + partitionId);
            totalResult += worker_result;
            taskIndex++;
        }
        for (auto &intermThread : intermThreads) {
            if (intermThread.joinable()) {
                intermThread.join();
            }
        }
    }
    return totalResult;
}

void TriangleCountExecutor::executeTriangleCount(SQLiteDBInterface *sqlite, PerformanceSQLiteDBInterface *perfDB,
                                                 const JobRequest &request, TriangleCountCommandType commandType,
                                                 ThreadingStrategy strategy, Logger &logger) {
    std::string graphId = request.getParameter(Conts::PARAM_KEYS::GRAPH_ID);
    OTEL_TRACE_FUNCTION();

    std::unique_lock<std::mutex> lock(schedulerMutex, std::defer_lock);
    lock.lock();
    if (time_t curr_time = time(nullptr); curr_time < last_exec_time + SCHEDULER_EXECUTION_GUARD_SECONDS) {
        time_t sleep_duration = last_exec_time + SCHEDULER_NEXT_EXECUTION_OFFSET_SECONDS - curr_time;
        last_exec_time = curr_time + sleep_duration;
        lock.unlock();
        std::this_thread::sleep_for(std::chrono::seconds(sleep_duration));
        lock.lock();
    } else {
        last_exec_time = curr_time;
    }

    int uniqueId = getUid();
    std::string masterIP = request.getMasterIP();
    std::string canCalibrateString = request.getParameter(Conts::PARAM_KEYS::CAN_CALIBRATE);
    std::string queueTime = request.getParameter(Conts::PARAM_KEYS::QUEUE_TIME);
    bool canCalibrate = Utils::parseBoolean(canCalibrateString);
    int threadPriority = request.getPriority();
    std::string autoCalibrateString = request.getParameter(Conts::PARAM_KEYS::AUTO_CALIBRATION);
    bool autoCalibrate = Utils::parseBoolean(autoCalibrateString);

    if (threadPriority == Conts::HIGH_PRIORITY_DEFAULT_VALUE) {
        highPriorityGraphList.push_back(graphId);
    }

    std::string commandName = (commandType == TriangleCountCommandType::TRIANGLES) ? TRIANGLES : SHEEP_TRIANGLES;
    std::string logPrefix = (commandType == TriangleCountCommandType::TRIANGLES)
                                ? "###TRIANGLE-COUNT-EXECUTOR###"
                                : "###SHEEP-TRIANGLE-COUNT-EXECUTOR###";

    std::chrono::milliseconds startTime = duration_cast<milliseconds>(system_clock::now().time_since_epoch());
    struct ProcessInfo processInformation;
    processInformation.id = uniqueId;
    processInformation.graphId = graphId;
    processInformation.processName = commandName;
    processInformation.priority = threadPriority;
    processInformation.startTimestamp = startTime.count();

    if (!queueTime.empty()) {
        long sleepTime = atol(queueTime.c_str());
        processInformation.sleepTime = sleepTime;
        insertProcessInfo(processInformation);
        std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime));
    } else {
        insertProcessInfo(processInformation);
    }

    logger.log(logPrefix + " Started with graph ID : " + graphId + " Master IP : " + masterIP, "info");

    long result = 0;
    bool isCompositeAggregation = false;
    auto begin = chrono::high_resolution_clock::now();

    std::map<string, std::vector<string>, std::less<>> partitionMap =
        buildPartitionMap(sqlite, graphId, logPrefix, logger);

    size_t totalPartitions = 0;
    for (const auto &[worker, partitions] : partitionMap) {
        totalPartitions += partitions.size();
    }
    if (totalPartitions > Conts::COMPOSITE_CENTRAL_STORE_WORKER_THRESHOLD) {
        isCompositeAggregation = true;
    }

    std::vector<std::vector<string>> fileCombinations;
    if (isCompositeAggregation) {
        fileCombinations = getCompositeFileCombinations(graphId);
    }

    for (const auto &[worker, partitions] : partitionMap) {
        if (used_workers.find(worker) != used_workers.end()) {
            used_workers[worker]++;
        } else {
            used_workers[worker] = 1;
        }
    }

    std::map<std::string, std::string, std::less<>> combinationWorkerMap;
    std::unordered_map<long, std::unordered_map<long, std::unordered_set<long>>> triangleTree;
    std::mutex triangleTreeMutex;
    std::vector<std::tuple<string, string, string>> workerTaskInfo;
    std::string masterTraceContext = OpenTelemetryUtil::getCurrentTraceContext();

    std::vector<std::thread> intermThreads;
    std::vector<long> intermResThread;
    std::vector<std::future<long>> intermResFuture;

    WorkerTaskContext taskCtx{
        atoi(graphId.c_str()),
        masterIP,
        uniqueId,
        isCompositeAggregation,
        threadPriority,
        fileCombinations,
        combinationWorkerMap,
        triangleTree,
        triangleTreeMutex,
        masterTraceContext
    };

    TaskCollector collector{
        workerTaskInfo,
        intermThreads,
        intermResThread,
        intermResFuture,
        strategy
    };

    int partitionCount = distributeTasksToWorkers(sqlite, partitionMap, taskCtx, collector);

    std::vector<std::thread> statThreads = handleSLACalibration(
        perfDB, graphId, partitionCount, commandName, masterIP, autoCalibrate, canCalibrate);

    if (time_t actual_time = time(nullptr); actual_time > last_exec_time) {
        last_exec_time = actual_time;
    }
    lock.unlock();

    {
        OTEL_TRACE_OPERATION(OTelTraceOperations::COLLECT_WORKER_RESULTS);
        result += gatherWorkerResults(strategy, uniqueId, workerTaskInfo, intermThreads, intermResThread,
            intermResFuture, logger);

        OTEL_TRACE_OPERATION(OTelTraceOperations::CLEANUP_WORKER_DATA_STRUCTURES);
        triangleTree.clear();
        combinationWorkerMap.clear();
    }

    if (!isCompositeAggregation) {
        OpenTelemetryUtil::receiveAndSetTraceContext(masterTraceContext, "central store aggregation");
        OTEL_TRACE_OPERATION(OTelTraceOperations::CENTRAL_STORE_AGGREGATION);
        long aggregatedTriangleCount = aggregateCentralStoreTriangles(sqlite, graphId, masterIP,
                                                                      threadPriority, partitionMap);
        result += aggregatedTriangleCount;
        workerResponded = true;
        logger.log(logPrefix + " Getting Triangle Count : Completed: Triangles " + to_string(result), "info");
    }

    {
        std::scoped_lock<std::mutex> scheduler_lock(schedulerMutex);
        for (auto it = partitionMap.begin(); it != partitionMap.end(); it++) {
            string worker = it->first;
            used_workers[worker]--;
        }
        for (auto it = used_workers.cbegin(); it != used_workers.cend();) {
            if (it->second <= 0) {
                used_workers.erase(it++);
            } else {
                it++;
            }
        }
    }
    workerResponded = true;

    JobResponse jobResponse;
    jobResponse.setJobId(request.getJobId());
    jobResponse.addParameter(Conts::PARAM_KEYS::TRIANGLE_COUNT, std::to_string(result));
    responseVectorMutex.lock();
    responseVector.push_back(jobResponse);
    responseMap[request.getJobId()] = jobResponse;
    responseVectorMutex.unlock();

    auto end = chrono::high_resolution_clock::now();
    auto dur = end - begin;
    auto msDuration = std::chrono::duration_cast<std::chrono::milliseconds>(dur).count();
    if (canCalibrate || autoCalibrate) {
        Utils::updateSLAInformation(perfDB, graphId, partitionCount, msDuration, commandName,
                                   Conts::SLA_CATEGORY::LATENCY);
        isStatCollect = false;
    }
    removeProcessInfoById(uniqueId);
    joinAllThreads(statThreads);
}
