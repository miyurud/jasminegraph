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

#include "../../../../../globals.h"
#include "../../../../k8s/K8sWorkerController.h"

using namespace std::chrono;

Logger triangleCount_logger;
bool isStatCollect = false;

std::mutex processStatusMutex;
std::mutex responseVectorMutex;
static std::mutex fileCombinationMutex;
static std::mutex aggregateWeightMutex;
static std::mutex schedulerMutex;

static string isFileAccessibleToWorker(std::string graphId, std::string partitionId, std::string aggregatorHostName,
                                       std::string aggregatorPort, std::string masterIP, std::string fileType,
                                       std::string fileName);
static long aggregateCentralStoreTriangles(SQLiteDBInterface *sqlite, std::string graphId, std::string masterIP,
                                           int threadPriority,
                                           const std::map<std::string, std::vector<string>> &partitionMap);
static int updateTriangleTreeAndGetTriangleCount(
    const std::vector<std::string> &triangles,
    std::unordered_map<long, std::unordered_map<long, std::unordered_set<long>>> *triangleTree_p,
    std::mutex *triangleTreeMutex_p);

TriangleCountExecutor::TriangleCountExecutor() {}

TriangleCountExecutor::TriangleCountExecutor(SQLiteDBInterface *db, PerformanceSQLiteDBInterface *perfDb,
                                             JobRequest jobRequest) {
    this->sqlite = db;
    this->perfDB = perfDb;
    this->request = jobRequest;
}

static void allocate(int p, string w, std::map<int, string> &alloc, std::set<int> &remain,
                     std::map<int, std::vector<string>> &p_avail, std::map<string, int> &loads) {
    alloc[p] = w;
    remain.erase(p);
    p_avail.erase(p);
    loads[w]++;
    if (loads[w] >= 3) {
        for (auto it = p_avail.begin(); it != p_avail.end(); it++) {
            auto &ws = it->second;
            auto itr = std::find(ws.begin(), ws.end(), w);
            if (itr != ws.end()) {
                ws.erase(itr);
            }
        }
    }
}

static int get_min_partition(std::set<int> &remain, std::map<int, std::vector<string>> &p_avail) {
    int p0 = *remain.begin();
    size_t m = 1000000000;
    for (auto it = remain.begin(); it != remain.end(); it++) {
        int p = *it;
        auto &ws = p_avail[p];
        if (ws.size() > 0 && ws.size() < m) {
            m = ws.size();
            p0 = p;
        }
    }
    return p0;
}

static const std::vector<int> LOAD_PREFERENCE = {2, 3, 1, 0};

static int alloc_plan(std::map<int, string> &alloc, std::set<int> &remain, std::map<int, std::vector<string>> &p_avail,
                      std::map<string, int> &loads) {
    for (bool done = false; !done;) {
        string w = "";
        done = true;
        int p;
        for (auto it = remain.begin(); it != remain.end(); it++) {
            p = *it;
            if (p_avail[p].size() == 1) {
                w = p_avail[p][0];
                done = false;
                break;
            }
        }
        if (!w.empty()) allocate(p, w, alloc, remain, p_avail, loads);
    }
    if (remain.empty()) return 0;
    int p0 = get_min_partition(remain, p_avail);
    auto &ws = p_avail[p0];
    if (ws.empty()) return (int)remain.size();
    sort(ws.begin(), ws.end(), [&loads](string &w1, string &w2) {
        return LOAD_PREFERENCE[loads[w1]] > LOAD_PREFERENCE[loads[w2]];
    });  // load=1 goes first and load=3 goes last
    struct best_alloc {
        std::map<int, string> alloc;
        std::set<int> remain;
        std::map<int, std::vector<string>> p_avail;
        std::map<string, int> loads;
    };
    int best_rem = remain.size();
    struct best_alloc best = {.alloc = alloc, .remain = remain, .p_avail = p_avail, .loads = loads};
    for (auto it = ws.begin(); it != ws.end(); it++) {
        string w = *it;
        auto alloc2 = alloc;      // need copy => do not copy reference
        auto remain2 = remain;    // need copy => do not copy reference
        auto p_avail2 = p_avail;  // need copy => do not copy reference
        auto loads2 = loads;      // need copy => do not copy reference
        allocate(p0, w, alloc2, remain2, p_avail2, loads2);
        int rem = alloc_plan(alloc2, remain2, p_avail2, loads2);
        if (rem == 0) {
            remain.clear();
            p_avail.clear();
            alloc.insert(alloc2.begin(), alloc2.end());
            loads.insert(loads2.begin(), loads2.end());
            return 0;
        }
        if (rem < best_rem) {
            best_rem = rem;
            best = {.alloc = alloc2, .remain = remain2, .p_avail = p_avail2, .loads = loads2};
        }
    }
    alloc.insert(best.alloc.begin(), best.alloc.end());
    remain.clear();
    remain.insert(best.remain.begin(), best.remain.end());
    p_avail.clear();
    p_avail.insert(best.p_avail.begin(), best.p_avail.end());
    loads.clear();
    loads.insert(best.loads.begin(), best.loads.end());
    return best_rem;
}

static std::vector<int> reallocate_parts(std::map<int, string> &alloc, std::set<int> &remain,
                                         const std::map<int, std::vector<string>> &P_AVAIL) {
    map<int, int> P_COUNT;
    for (auto it = P_AVAIL.begin(); it != P_AVAIL.end(); it++) {
        P_COUNT[it->first] = it->second.size();
    }
    vector<int> remain_l(remain.begin(), remain.end());
    sort(remain_l.begin(), remain_l.end(),
         [&P_COUNT](int &p1, int &p2) { return P_COUNT[p1] > P_COUNT[p2]; });  // partitions with more copies goes first
    vector<int> PARTITIONS;
    for (auto it = P_COUNT.begin(); it != P_COUNT.end(); it++) {
        PARTITIONS.push_back(it->first);
    }
    sort(PARTITIONS.begin(), PARTITIONS.end(), [&P_COUNT](int &p1, int &p2) {
        return P_COUNT[p1] < P_COUNT[p2];
    });  // partitions with fewer copies goes first
    vector<int> copying;
    while (!remain_l.empty()) {
        int p0 = remain_l.back();
        remain_l.pop_back();
        int w_cnt = P_COUNT[p0];
        if (w_cnt == 1) {
            copying.push_back(p0);
            continue;
        }
        const auto &ws = P_AVAIL.find(p0)->second;
        bool need_pushing = true;
        for (auto it = PARTITIONS.begin(); it != PARTITIONS.end(); it++) {
            int p = *it;
            if (w_cnt <= P_COUNT[p]) {
                copying.push_back(p0);  // assuming PARTITIONS are in sorted order of copy count
                need_pushing = false;
                break;
            }
            if (alloc.find(p) == alloc.end()) {
                continue;
            }
            auto w = alloc[p];
            if (std::find(ws.begin(), ws.end(), w) != ws.end()) {
                alloc.erase(p);
                alloc[p0] = w;
                remain_l.push_back(p);
                need_pushing = false;
                break;
            }
        }
        if (need_pushing) copying.push_back(p0);
    }
    return copying;
}

static void scale_up(std::map<string, int> &loads, map<string, string> &workers, int copy_cnt) {
    int curr_load = 0;
    for (auto it = loads.begin(); it != loads.end(); it++) {
        curr_load += it->second;
    }
    int n_cores = copy_cnt + curr_load - 3 * loads.size();
    if (n_cores < 0) {
        return;
    }
    int n_workers = n_cores / 2 + 1;  // allocate a little more to prevent saturation
    if (n_cores % 2 > 0) n_workers++;
    if (n_workers == 0) return;

    K8sWorkerController *k8sController = K8sWorkerController::getInstance();
    map<string, string> w_new = k8sController->scaleUp(n_workers);

    for (auto it = w_new.begin(); it != w_new.end(); it++) {
        loads[it->first] = 0.1;
        workers[it->first] = it->second;
    }
}

static int alloc_net_plan(std::map<int, string> &alloc, std::vector<int> &parts,
                          std::map<int, std::pair<string, string>> &transfer, std::map<string, int> &net_loads,
                          std::map<string, int> &loads, const std::map<int, std::vector<string>> &p_avail,
                          int curr_best) {
    int curr_load = std::max_element(net_loads.begin(), net_loads.end(),
                                     [](const std::map<string, int>::value_type &p1,
                                        const std::map<string, int>::value_type &p2) { return p1.second < p2.second; })
                        ->second;
    if (curr_load >= curr_best) {
        return curr_load;
    }
    if (parts.empty()) {
        if (net_loads.empty()) return 0;
        return curr_load;
    }
    struct best_net_alloc {
        std::map<int, string> alloc;
        std::map<int, std::pair<string, string>> transfer;
        std::map<string, int> net_loads;
        std::map<string, int> loads;
    };
    int best = curr_best;
    struct best_net_alloc best_plan = {.transfer = transfer, .net_loads = net_loads, .loads = loads};
    int p = parts.back();
    parts.pop_back();
    vector<string> wts;
    for (auto it = loads.begin(); it != loads.end(); it++) {
        wts.push_back(it->first);
    }
    sort(wts.begin(), wts.end(), [&loads](string &w1, string &w2) {
        int l1 = loads[w1];
        int l2 = loads[w2];
        // TODO: temporarily commented for scale up only
        // if (l1 < 3 && l2 < 3) return LOAD_PREFERENCE[l1] > LOAD_PREFERENCE[loads[w2]];
        if (l1 < 3 || l2 < 3) return l1 < l2;
        return l1 <= l2;
    });  // load=1 goes first and load=3 goes last
    const auto &ws = p_avail.find(p)->second;
    int minLoad = 100000000;
    for (auto it = loads.begin(); it != loads.end(); it++) {
        int load = it->second;
        if (minLoad > load) {
            minLoad = load;
        }
    }
    for (auto itf = ws.begin(); itf != ws.end(); itf++) {
        auto wf = *itf;
        for (auto itt = wts.begin(); itt != wts.end(); itt++) {
            auto wt = *itt;
            int currLoad = loads[wt];
            if (currLoad > minLoad) continue;
            auto alloc2 = alloc;          // need copy => do not copy reference
            auto parts2 = parts;          // need copy => do not copy reference
            auto transfer2 = transfer;    // need copy => do not copy reference
            auto net_loads2 = net_loads;  // need copy => do not copy reference
            auto loads2 = loads;          // need copy => do not copy reference
            if (wf != wt) {
                transfer2[p] = {wf, wt};  // assume
                net_loads2[wf]++;
                net_loads2[wt]++;
            }
            alloc2[p] = wt;
            loads2[wt]++;
            int new_net_load = alloc_net_plan(alloc2, parts2, transfer2, net_loads2, loads2, p_avail, best);
            if (new_net_load < best) {
                best = new_net_load;
                best_plan = {.alloc = alloc2, .transfer = transfer2, .net_loads = net_loads2, .loads = loads2};
            }
        }
    }
    alloc.clear();
    alloc.insert(best_plan.alloc.begin(), best_plan.alloc.end());
    auto &b_transfer = best_plan.transfer;
    for (auto it = b_transfer.begin(); it != b_transfer.end(); it++) {
        transfer[it->first] = it->second;
    }
    net_loads.clear();
    net_loads.insert(best_plan.net_loads.begin(), best_plan.net_loads.end());
    loads.clear();
    loads.insert(best_plan.loads.begin(), best_plan.loads.end());
    return best;
}

static void filter_partitions(std::map<string, std::vector<string>> &partitionMap, SQLiteDBInterface *sqlite,
                              string graphId) {
    map<string, string> workers;  // id => "ip:port"
    const std::vector<vector<pair<string, string>>> &results =
        sqlite->runSelect("SELECT DISTINCT idworker,ip,server_port FROM worker;");
    for (int i = 0; i < results.size(); i++) {
        string workerId = results[i][0].second;
        string ip = results[i][1].second;
        string port = results[i][2].second;
        workers[workerId] = ip + ":" + port;
    }

    map<string, int> loads;
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

    std::map<int, std::vector<string>> p_avail;
    std::set<int> remain;
    for (auto it = partitionMap.begin(); it != partitionMap.end(); it++) {
        auto worker = it->first;
        auto &partitions = it->second;
        for (auto partitionIt = partitions.begin(); partitionIt != partitions.end(); partitionIt++) {
            auto partition = stoi(*partitionIt);
            p_avail[partition].push_back(worker);
            remain.insert(partition);
        }
    }
    const std::map<int, std::vector<string>> P_AVAIL = p_avail;  // get a copy and make it const

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

    std::map<int, string> alloc;
    cout << "calling alloc_plan" << endl;
    int unallocated = alloc_plan(alloc, remain, p_avail, loads);
    cout << "alloc_plan returned " << unallocated << endl;
    if (unallocated > 0) {
        cout << "calling reallocate_parts" << endl;
        auto copying = reallocate_parts(alloc, remain, P_AVAIL);
        cout << "reallocate_parts completed" << endl;
        scale_up(loads, workers, copying.size());
        cout << "scale_up completed" << endl;

        map<string, int> net_loads;
        for (auto it = loads.begin(); it != loads.end(); it++) {
            net_loads[it->first] = 0;
        }
        for (auto it = workers.begin(); it != workers.end(); it++) {
            if (loads.find(it->first) == loads.end()) {
                loads[it->first] = 3;
            }
        }

        std::map<int, std::pair<string, string>> transfer;
        int net_load = alloc_net_plan(alloc, copying, transfer, net_loads, loads, P_AVAIL, 100000000);
        cout << "alloc_net_plan completed with net_load=" << net_load << endl;
        for (auto it = transfer.begin(); it != transfer.end(); it++) {
            auto p = it->first;
            auto w_to = it->second.second;
            alloc[p] = w_to;
            cout << "planned sending partition " << p << " from " << it->second.first << " to " << it->second.second
                 << endl;
        }

        if (!transfer.empty()) {
            const std::vector<vector<pair<string, string>>> &workerData =
                sqlite->runSelect("SELECT DISTINCT ip,server_port,server_data_port FROM worker;");
            map<string, string> dataPortMap;  // "ip:port" => data_port
            for (int i = 0; i < workerData.size(); i++) {
                string ip = workerData[i][0].second;
                string port = workerData[i][1].second;
                string dport = workerData[i][2].second;
                dataPortMap[ip + ":" + port] = dport;
                cout << "dataPortMap[" << ip + ":" + port << "] = " << dport << endl;
            }
            map<string, string> workers_r;  // "ip:port" => id
            for (auto it = workers.begin(); it != workers.end(); it++) {
                workers_r[it->second] = it->first;
            }
            thread transferThreads[transfer.size()];
            int threadCnt = 0;
            for (auto it = transfer.begin(); it != transfer.end(); it++) {
                auto partition = it->first;
                auto from_worker = it->second.first;
                auto to_worker = it->second.second;
                auto w_from = workers[from_worker];
                auto w_to = workers[to_worker];
                const auto &ip_port_from = Utils::split(w_from, ':');
                auto ip_from = ip_port_from[0];
                auto port_from = stoi(ip_port_from[1]);
                const auto &ip_port_to = Utils::split(w_to, ':');
                auto ip_to = ip_port_to[0];
                auto dport_to = stoi(dataPortMap[w_to]);
                transferThreads[threadCnt++] = std::thread(&Utils::transferPartition, ip_from, port_from, ip_to,
                                                           dport_to, graphId, to_string(partition), to_worker, sqlite);
            }
            for (int i = 0; i < threadCnt; i++) {
                transferThreads[i].join();
            }
        }
    }
    partitionMap.clear();
    for (auto it = alloc.begin(); it != alloc.end(); it++) {
        auto partition = it->first;
        auto worker = it->second;
        partitionMap[worker].push_back(to_string(partition));
    }
}

void TriangleCountExecutor::execute() {
    schedulerMutex.lock();
    int uniqueId = getUid();
    std::string masterIP = request.getMasterIP();
    std::string graphId = request.getParameter(Conts::PARAM_KEYS::GRAPH_ID);
    std::string canCalibrateString = request.getParameter(Conts::PARAM_KEYS::CAN_CALIBRATE);
    std::string queueTime = request.getParameter(Conts::PARAM_KEYS::QUEUE_TIME);

    bool canCalibrate = Utils::parseBoolean(canCalibrateString);
    int threadPriority = request.getPriority();

    std::string autoCalibrateString = request.getParameter(Conts::PARAM_KEYS::AUTO_CALIBRATION);
    bool autoCalibrate = Utils::parseBoolean(autoCalibrateString);

    if (threadPriority == Conts::HIGH_PRIORITY_DEFAULT_VALUE) {
        highPriorityGraphList.push_back(graphId);
    }

    // Below code is used to update the process details
    processStatusMutex.lock();
    bool processInfoExists = false;
    std::chrono::milliseconds startTime = duration_cast<milliseconds>(system_clock::now().time_since_epoch());

    struct ProcessInfo processInformation;
    processInformation.id = uniqueId;
    processInformation.graphId = graphId;
    processInformation.processName = TRIANGLES;
    processInformation.priority = threadPriority;
    processInformation.startTimestamp = startTime.count();

    if (!queueTime.empty()) {
        long sleepTime = atol(queueTime.c_str());
        processInformation.sleepTime = sleepTime;
        processData.insert(processInformation);
        processStatusMutex.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime));
    } else {
        processData.insert(processInformation);
        processStatusMutex.unlock();
    }

    triangleCount_logger.log(
        "###TRIANGLE-COUNT-EXECUTOR### Started with graph ID : " + graphId + " Master IP : " + masterIP, "info");

    long result = 0;
    bool isCompositeAggregation = false;
    Utils::worker aggregatorWorker;
    std::vector<std::future<long>> intermRes;
    std::vector<std::future<int>> statResponse;
    std::vector<std::string> compositeCentralStoreFiles;

    auto begin = chrono::high_resolution_clock::now();

    string sqlStatement =
        "SELECT DISTINCT worker_idworker,partition_idpartition "
        "FROM worker_has_partition INNER JOIN worker ON worker_has_partition.worker_idworker=worker.idworker "
        "WHERE partition_graph_idgraph=" +
        graphId + ";";

    const std::vector<vector<pair<string, string>>> &results = sqlite->runSelect(sqlStatement);

    std::map<string, std::vector<string>> partitionMap;

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

        triangleCount_logger.info("###TRIANGLE-COUNT-EXECUTOR### Getting Triangle Count : PartitionId " + partitionId);
    }

    if (results.size() > Conts::COMPOSITE_CENTRAL_STORE_WORKER_THRESHOLD) {
        isCompositeAggregation = true;
    }

    cout << "initial partitionMap = {" << endl;
    for (auto it = partitionMap.begin(); it != partitionMap.end(); it++) {
        cout << "  " << it->first << ": [";
        auto &pts = it->second;
        for (auto it2 = pts.begin(); it2 != pts.end(); it2++) {
            cout << *it2 << ", ";
        }
        cout << "]" << endl;
    }
    cout << "}" << endl;
    cout << endl;

    auto *k8sInterface = new K8sInterface();
    if (jasminegraph_profile == PROFILE_K8S && k8sInterface->getJasmineGraphConfig("auto_scaling_enabled") == "true") {
        filter_partitions(partitionMap, sqlite, graphId);
    }
    delete k8sInterface;

    cout << "final partitionMap = {" << endl;
    for (auto it = partitionMap.begin(); it != partitionMap.end(); it++) {
        cout << "  " << it->first << ": [";
        auto &pts = it->second;
        for (auto it2 = pts.begin(); it2 != pts.end(); it2++) {
            cout << *it2 << ", ";
        }
        cout << "]" << endl;
    }
    cout << "}" << endl;
    cout << endl;

    std::vector<std::vector<string>> fileCombinations;
    if (isCompositeAggregation) {
        std::string aggregatorFilePath =
            Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.aggregatefolder");
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
        fileCombinations = AbstractExecutor::getCombinations(compositeCentralStoreFiles);
    }

    std::map<std::string, std::string> combinationWorkerMap;
    std::unordered_map<long, std::unordered_map<long, std::unordered_set<long>>> triangleTree;
    std::mutex triangleTreeMutex;
    int partitionCount = 0;
    vector<Utils::worker> workerList = Utils::getWorkerList(sqlite);
    int workerListSize = workerList.size();
    cout << "workerListSize = " << workerListSize << endl;
    for (int i = 0; i < workerListSize; i++) {
        Utils::worker currentWorker = workerList.at(i);
        string host = currentWorker.hostname;
        string workerID = currentWorker.workerID;
        string partitionId;
        int workerPort = atoi(string(currentWorker.port).c_str());
        int workerDataPort = atoi(string(currentWorker.dataPort).c_str());
        triangleCount_logger.info("worker_" + workerID + " host=" + host + ":" + to_string(workerPort) + ":" +
                                  to_string(workerDataPort));
        const std::vector<string> &partitionList = partitionMap[workerID];
        for (auto partitionIterator = partitionList.begin(); partitionIterator != partitionList.end();
             ++partitionIterator) {
            partitionCount++;
            partitionId = *partitionIterator;
            triangleCount_logger.info("> partition" + partitionId);
            intermRes.push_back(std::async(
                std::launch::async, TriangleCountExecutor::getTriangleCount, atoi(graphId.c_str()), host, workerPort,
                workerDataPort, atoi(partitionId.c_str()), masterIP, uniqueId, isCompositeAggregation, threadPriority,
                fileCombinations, &combinationWorkerMap, &triangleTree, &triangleTreeMutex));
        }
    }

    PerformanceUtil::init();

    std::string query =
        "SELECT attempt from graph_sla INNER JOIN sla_category where graph_sla.id_sla_category=sla_category.id and "
        "graph_sla.graph_id='" +
        graphId + "' and graph_sla.partition_count='" + std::to_string(partitionCount) +
        "' and sla_category.category='" + Conts::SLA_CATEGORY::LATENCY + "' and sla_category.command='" + TRIANGLES +
        "';";

    const std::vector<vector<pair<string, string>>> &queryResults = perfDB->runSelect(query);

    if (queryResults.size() > 0) {
        std::string attemptString = queryResults[0][0].second;
        int calibratedAttempts = atoi(attemptString.c_str());

        if (calibratedAttempts >= Conts::MAX_SLA_CALIBRATE_ATTEMPTS) {
            canCalibrate = false;
        }
    } else {
        triangleCount_logger.log("###TRIANGLE-COUNT-EXECUTOR### Inserting initial record for SLA ", "info");
        Utils::updateSLAInformation(perfDB, graphId, partitionCount, 0, TRIANGLES, Conts::SLA_CATEGORY::LATENCY);
        statResponse.push_back(std::async(std::launch::async, AbstractExecutor::collectPerformaceData, perfDB,
                                          graphId.c_str(), TRIANGLES, Conts::SLA_CATEGORY::LATENCY, partitionCount,
                                          masterIP, autoCalibrate));
        isStatCollect = true;
    }

    schedulerMutex.unlock();

    for (auto &&futureCall : intermRes) {
        triangleCount_logger.info("Waiting for result. uuid=" + to_string(uniqueId));
        result += futureCall.get();
    }
    triangleTree.clear();
    combinationWorkerMap.clear();

    if (!isCompositeAggregation) {
        long aggregatedTriangleCount =
            aggregateCentralStoreTriangles(sqlite, graphId, masterIP, threadPriority, partitionMap);
        result += aggregatedTriangleCount;
        workerResponded = true;
        triangleCount_logger.log(
            "###TRIANGLE-COUNT-EXECUTOR### Getting Triangle Count : Completed: Triangles " + to_string(result), "info");
    }

    workerResponded = true;

    JobResponse jobResponse;
    jobResponse.setJobId(request.getJobId());
    jobResponse.addParameter(Conts::PARAM_KEYS::TRIANGLE_COUNT, std::to_string(result));
    responseVector.push_back(jobResponse);

    responseVectorMutex.lock();
    responseMap[request.getJobId()] = jobResponse;
    responseVectorMutex.unlock();

    auto end = chrono::high_resolution_clock::now();
    auto dur = end - begin;
    auto msDuration = std::chrono::duration_cast<std::chrono::milliseconds>(dur).count();

    std::string durationString = std::to_string(msDuration);

    if (canCalibrate || autoCalibrate) {
        Utils::updateSLAInformation(perfDB, graphId, partitionCount, msDuration, TRIANGLES,
                                    Conts::SLA_CATEGORY::LATENCY);
        isStatCollect = false;
    }

    processStatusMutex.lock();
    for (auto processCompleteIterator = processData.begin(); processCompleteIterator != processData.end();
         ++processCompleteIterator) {
        ProcessInfo processInformation = *processCompleteIterator;

        if (processInformation.id == uniqueId) {
            processData.erase(processInformation);
            break;
        }
    }
    processStatusMutex.unlock();
}

long TriangleCountExecutor::getTriangleCount(
    int graphId, std::string host, int port, int dataPort, int partitionId, std::string masterIP, int uniqueId,
    bool isCompositeAggregation, int threadPriority, std::vector<std::vector<string>> fileCombinations,
    std::map<std::string, std::string> *combinationWorkerMap_p,
    std::unordered_map<long, std::unordered_map<long, std::unordered_set<long>>> *triangleTree_p,
    std::mutex *triangleTreeMutex_p) {
    int sockfd;
    char data[301];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;
    long triangleCount;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot create socket" << std::endl;
        return 0;
    }

    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }

    triangleCount_logger.log("###TRIANGLE-COUNT-EXECUTOR### Get Host By Name : " + host, "info");

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        triangleCount_logger.error("ERROR, no host named " + host);
        return 0;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
        return 0;
    }

    int result_wr =
        write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if (result_wr < 0) {
        triangleCount_logger.log("Error writing to socket", "error");
    }

    triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    string response = Utils::read_str_trim_wrapper(sockfd, data, 300);

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if (result_wr < 0) {
            triangleCount_logger.log("Error writing to socket", "error");
        }
        triangleCount_logger.log("Sent : " + masterIP, "info");

        response = Utils::read_str_trim_wrapper(sockfd, data, 300);
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

        response = Utils::read_str_trim_wrapper(sockfd, data, 300);
        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, std::to_string(graphId).c_str(), std::to_string(graphId).size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }
            triangleCount_logger.log("Sent : Graph ID " + std::to_string(graphId), "info");

            response = Utils::read_str_trim_wrapper(sockfd, data, 300);
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, std::to_string(partitionId).c_str(), std::to_string(partitionId).size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }

            triangleCount_logger.log("Sent : Partition ID " + std::to_string(partitionId), "info");

            response = Utils::read_str_trim_wrapper(sockfd, data, 300);
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, std::to_string(threadPriority).c_str(), std::to_string(threadPriority).size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }
            triangleCount_logger.log("Sent : Thread Priority " + std::to_string(threadPriority), "info");

            response = Utils::read_str_trim_wrapper(sockfd, data, 300);
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
                std::map<std::string, std::string> &combinationWorkerMap = *combinationWorkerMap_p;
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

static long aggregateCentralStoreTriangles(SQLiteDBInterface *sqlite, std::string graphId, std::string masterIP,
                                           int threadPriority,
                                           const std::map<string, std::vector<string>> &partitionMap) {
    vector<string> partitionsVector;
    std::map<string, string> partitionWorkerMap;  // partition_id => worker_id
    for (auto it = partitionMap.begin(); it != partitionMap.end(); it++) {
        const auto &parts = it->second;
        string worker = it->first;
        for (auto partsIt = parts.begin(); partsIt != parts.end(); partsIt++) {
            string partition = *partsIt;
            partitionWorkerMap[partition] = worker;
            partitionsVector.push_back(partition);
        }
    }

    const std::vector<std::vector<string>> &partitionCombinations = AbstractExecutor::getCombinations(partitionsVector);
    std::map<string, int> workerWeightMap;
    std::vector<std::future<string>> triangleCountResponse;
    std::string result = "";
    long aggregatedTriangleCount = 0;

    const std::vector<vector<pair<string, string>>> &workerDataResult =
        sqlite->runSelect("SELECT DISTINCT idworker,ip,server_port,server_data_port FROM worker;");
    map<string, vector<string>> workerDataMap;  // worker_id => [ip,port,data_port]
    for (auto it = workerDataResult.begin(); it != workerDataResult.end(); it++) {
        const auto &ipPortDport = *it;
        string id = ipPortDport[0].second;
        string ip = ipPortDport[1].second;
        string port = ipPortDport[2].second;
        string dport = ipPortDport[3].second;
        workerDataMap[id] = {ip, port, dport};
    }

    for (auto partitonCombinationsIterator = partitionCombinations.begin();
         partitonCombinationsIterator != partitionCombinations.end(); partitonCombinationsIterator++) {
        const std::vector<string> &partitionCombination = *partitonCombinationsIterator;
        std::vector<std::future<string>> remoteGraphCopyResponse;
        int minimumWeight = 0;
        std::string minWeightWorker;
        std::string minWeightWorkerPartition;

        for (auto partCombinationIterator = partitionCombination.begin();
             partCombinationIterator != partitionCombination.end(); partCombinationIterator++) {
            string part = *partCombinationIterator;
            string workerId = partitionWorkerMap[part];
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

        const auto &workerData = workerDataMap[minWeightWorker];
        std::string aggregatorIp = workerData[0];
        std::string aggregatorPort = workerData[1];
        std::string aggregatorDataPort = workerData[2];

        std::string aggregatorPartitionId = minWeightWorkerPartition;

        std::string partitionIdList = "";
        for (auto partitionCombinationIterator = partitionCombination.begin();
             partitionCombinationIterator != partitionCombination.end(); ++partitionCombinationIterator) {
            string part = *partitionCombinationIterator;
            string workerId = partitionWorkerMap[part];

            if (part != minWeightWorkerPartition) {
                partitionIdList += part + ",";
            }
            if (workerId != minWeightWorker) {
                std::string centralStoreAvailable = isFileAccessibleToWorker(
                    graphId, part, aggregatorIp, aggregatorPort, masterIP,
                    JasmineGraphInstanceProtocol::FILE_TYPE_CENTRALSTORE_AGGREGATE, std::string());

                if (centralStoreAvailable.compare("false") == 0) {
                    remoteGraphCopyResponse.push_back(std::async(
                        std::launch::async, TriangleCountExecutor::copyCentralStoreToAggregator, aggregatorIp,
                        aggregatorPort, aggregatorDataPort, atoi(graphId.c_str()), atoi(part.c_str()), masterIP));
                }
            }
        }

        for (auto &&futureCallCopy : remoteGraphCopyResponse) {
            futureCallCopy.get();
        }

        std::string adjustedPartitionIdList = partitionIdList.substr(0, partitionIdList.size() - 1);

        triangleCountResponse.push_back(std::async(
            std::launch::async, TriangleCountExecutor::countCentralStoreTriangles, aggregatorPort, aggregatorIp,
            aggregatorPartitionId, adjustedPartitionIdList, graphId, masterIP, threadPriority));
    }

    for (auto &&futureCall : triangleCountResponse) {
        result = result + ":" + futureCall.get();
    }

    const std::vector<std::string> &triangles = Utils::split(result, ':');
    std::set<std::string> uniqueTriangleSet;
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
    char data[301];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;
    string isFileAccessible = "false";

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot create socket" << std::endl;
        return 0;
    }

    server = gethostbyname(aggregatorHostName.c_str());
    if (server == NULL) {
        triangleCount_logger.error("ERROR, no host named " + aggregatorHostName);
        return 0;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(atoi(aggregatorPort.c_str()));
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
        return 0;
    }

    bzero(data, 301);
    int result_wr =
        write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if (result_wr < 0) {
        triangleCount_logger.log("Error writing to socket", "error");
    }

    triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = Utils::trim_copy(response);

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if (result_wr < 0) {
            triangleCount_logger.log("Error writing to socket", "error");
        }

        triangleCount_logger.log("Sent : " + masterIP, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);

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

        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = Utils::trim_copy(response);

        if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_TYPE) == 0) {
            result_wr = write(sockfd, fileType.c_str(), fileType.size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = Utils::trim_copy(response);

            if (fileType.compare(JasmineGraphInstanceProtocol::FILE_TYPE_CENTRALSTORE_AGGREGATE) == 0) {
                if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
                    result_wr = write(sockfd, graphId.c_str(), graphId.size());

                    if (result_wr < 0) {
                        triangleCount_logger.log("Error writing to socket", "error");
                    }

                    bzero(data, 301);
                    read(sockfd, data, 300);
                    response = (data);
                    response = Utils::trim_copy(response);

                    if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
                        result_wr = write(sockfd, partitionId.c_str(), partitionId.size());

                        if (result_wr < 0) {
                            triangleCount_logger.log("Error writing to socket", "error");
                        }

                        bzero(data, 301);
                        read(sockfd, data, 300);
                        response = (data);
                        isFileAccessible = Utils::trim_copy(response);
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

                    bzero(data, 301);
                    read(sockfd, data, 300);
                    response = (data);
                    isFileAccessible = Utils::trim_copy(response);
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
    char data[301];
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
        std::cerr << "Cannot create socket" << std::endl;
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

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(atoi(aggregatorPort.c_str()));
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
        return 0;
    }

    bzero(data, 301);
    int result_wr =
        write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if (result_wr < 0) {
        triangleCount_logger.log("Error writing to socket", "error");
    }

    triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = Utils::trim_copy(response);

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if (result_wr < 0) {
            triangleCount_logger.log("Error writing to socket", "error");
        }

        triangleCount_logger.log("Sent : " + masterIP, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);

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
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = Utils::trim_copy(response);

        if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_NAME) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME, "info");
            result_wr = write(sockfd, fileName.c_str(), fileName.size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }

            triangleCount_logger.log("Sent : File Name " + fileName, "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = Utils::trim_copy(response);

            if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_LEN) == 0) {
                triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");
                result_wr = write(sockfd, fileLength.c_str(), fileLength.size());

                if (result_wr < 0) {
                    triangleCount_logger.log("Error writing to socket", "error");
                }

                triangleCount_logger.log("Sent : File Length: " + fileLength, "info");

                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);
                response = Utils::trim_copy(response);

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

            response = Utils::read_str_trim_wrapper(sockfd, data, 300);
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
            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);

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
    char data[301];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot create socket" << std::endl;
        return {};
    }

    server = gethostbyname(aggregatorHostName.c_str());
    if (server == NULL) {
        triangleCount_logger.error("ERROR, no host named " + aggregatorHostName);
        return {};
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(atoi(aggregatorPort.c_str()));
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
        return {};
    }

    bzero(data, 301);
    int result_wr =
        write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if (result_wr < 0) {
        triangleCount_logger.log("Error writing to socket", "error");
    }

    triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = Utils::trim_copy(response);

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if (result_wr < 0) {
            triangleCount_logger.log("Error writing to socket", "error");
        }

        triangleCount_logger.log("Sent : " + masterIP, "info");
        triangleCount_logger.log("Port : " + aggregatorPort, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);

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
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = Utils::trim_copy(response);

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, availableFileList.c_str(), availableFileList.size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }

            triangleCount_logger.log("Sent : Available File List " + availableFileList, "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = Utils::trim_copy(response);
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
                    bzero(data, 301);
                    read(sockfd, data, 300);
                    string chunkStatus = (data);
                    std::string chunk = chunksVector.at(loopCount);
                    write(sockfd, chunk.c_str(), chunk.size());
                }
            }

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = Utils::trim_copy(response);
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, std::to_string(threadPriority).c_str(), std::to_string(threadPriority).size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }

            triangleCount_logger.log("Sent : Thread Priority " + std::to_string(threadPriority), "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = Utils::trim_copy(response);
            string status = response.substr(response.size() - 5);
            std::basic_ostringstream<char> resultStream;
            resultStream << response.substr(0, response.size() - 5);
            while (status.compare("/SEND") == 0) {
                result_wr = write(sockfd, status.c_str(), status.size());

                if (result_wr < 0) {
                    triangleCount_logger.log("Error writing to socket", "error");
                }
                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);
                response = Utils::trim_copy(response);
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
    ;
}

std::string TriangleCountExecutor::copyCentralStoreToAggregator(std::string aggregatorHostName,
                                                                std::string aggregatorPort,
                                                                std::string aggregatorDataPort, int graphId,
                                                                int partitionId, std::string masterIP) {
    int sockfd;
    char data[301];
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
        std::cerr << "Cannot create socket" << std::endl;
        return "";
    }

    server = gethostbyname(aggregatorHostName.c_str());
    if (server == NULL) {
        triangleCount_logger.error("ERROR, no host named " + aggregatorHostName);
        return "";
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(atoi(aggregatorPort.c_str()));
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
        return "";
    }

    int result_wr =
        write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if (result_wr < 0) {
        triangleCount_logger.log("Error writing to socket", "error");
    }

    triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = Utils::trim_copy(response);

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if (result_wr < 0) {
            triangleCount_logger.log("Error writing to socket", "error");
        }

        triangleCount_logger.log("Sent : " + masterIP, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);

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
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = Utils::trim_copy(response);

        if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_NAME) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME, "info");
            result_wr = write(sockfd, fileName.c_str(), fileName.size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }

            triangleCount_logger.log("Sent : File Name " + fileName, "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = Utils::trim_copy(response);

            if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_LEN) == 0) {
                triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");
                result_wr = write(sockfd, fileLength.c_str(), fileLength.size());

                if (result_wr < 0) {
                    triangleCount_logger.log("Error writing to socket", "error");
                }

                triangleCount_logger.log("Sent : File Length: " + fileLength, "info");

                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);
                response = Utils::trim_copy(response);

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

            response = Utils::read_str_trim_wrapper(sockfd, data, 300);
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
            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);

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
                                                         int threadPriority) {
    int sockfd;
    char data[301];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot create socket" << std::endl;
        return 0;
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        triangleCount_logger.error("ERROR, no host named " + host);
        return 0;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(atoi(aggregatorPort.c_str()));
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
        return 0;
    }

    bzero(data, 301);
    int result_wr =
        write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if (result_wr < 0) {
        triangleCount_logger.log("Error writing to socket", "error");
    }

    triangleCount_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = Utils::trim_copy(response);

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if (result_wr < 0) {
            triangleCount_logger.log("Error writing to socket", "error");
        }

        triangleCount_logger.log("Sent : " + masterIP, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);

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
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = Utils::trim_copy(response);

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, graphId.c_str(), graphId.size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }

            triangleCount_logger.log("Sent : Graph ID " + graphId, "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = Utils::trim_copy(response);
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, partitionId.c_str(), partitionId.size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }

            triangleCount_logger.log("Sent : Partition ID " + partitionId, "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = Utils::trim_copy(response);
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, partitionIdList.c_str(), partitionIdList.size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }

            triangleCount_logger.log("Sent : Partition ID List : " + partitionIdList, "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = Utils::trim_copy(response);
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            triangleCount_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, std::to_string(threadPriority).c_str(), std::to_string(threadPriority).size());

            if (result_wr < 0) {
                triangleCount_logger.log("Error writing to socket", "error");
            }

            triangleCount_logger.log("Sent : Thread Priority " + std::to_string(threadPriority), "info");

            response = Utils::read_str_trim_wrapper(sockfd, data, 300);
            string status = response.substr(response.size() - 5);
            std::basic_ostringstream<char> resultStream;
            resultStream << response.substr(0, response.size() - 5);

            while (status == "/SEND") {
                result_wr = write(sockfd, status.c_str(), status.size());

                if (result_wr < 0) {
                    triangleCount_logger.log("Error writing to socket", "error");
                }
                response = Utils::read_str_trim_wrapper(sockfd, data, 300);
                status = response.substr(response.size() - 5);
                resultStream << response.substr(0, response.size() - 5);
            }
            response = resultStream.str();
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
