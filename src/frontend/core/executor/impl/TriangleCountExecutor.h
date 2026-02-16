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

#ifndef JASMINEGRAPH_TRIANGLECOUNTEXECUTOR_H
#define JASMINEGRAPH_TRIANGLECOUNTEXECUTOR_H

#include <chrono>
#include <unordered_map>
#include <unordered_set>

#include "../../../../metadb/SQLiteDBInterface.h"
#include "../../../../performance/metrics/PerformanceUtil.h"
#include "../../../../performancedb/PerformanceSQLiteDBInterface.h"
#include "../../../../server/JasmineGraphInstanceProtocol.h"
#include "../../../../server/JasmineGraphServer.h"
#include "../../../JasmineGraphFrontEndProtocol.h"
#include "../../CoreConstants.h"
#include "../AbstractExecutor.h"

class TriangleCountExecutor : public AbstractExecutor {
 public:
    TriangleCountExecutor();

    TriangleCountExecutor(SQLiteDBInterface *db, PerformanceSQLiteDBInterface *perfDb, JobRequest jobRequest);

    void execute();

    int getUid();

    static long getTriangleCount(
        int graphId, std::string host, int port, int dataPort, int partitionId, std::string masterIP, int uniqueId,
        bool isCompositeAggregation, int threadPriority, std::vector<std::vector<string>> fileCombinations,
        std::map<std::string, std::string> *combinationWorkerMap_p,
        std::unordered_map<long, std::unordered_map<long, std::unordered_set<long>>> *triangleTree_p,
        std::mutex *triangleTreeMutex_p, const std::string& masterTraceContext);

    static std::string copyCompositeCentralStoreToAggregator(std::string aggregatorHostName, std::string aggregatorPort,
                                                             std::string aggregatorDataPort, std::string fileName,
                                                             std::string masterIP);

    static std::vector<string> countCompositeCentralStoreTriangles(std::string aggregatorHostName,
                                                                   std::string aggregatorPort,
                                                                   std::string compositeCentralStoreFileList,
                                                                   std::string masterIP, std::string availableFileList,
                                                                   int threadPriority);

    static std::string copyCentralStoreToAggregator(std::string aggregatorHostName, std::string aggregatorPort,
                                                    std::string aggregatorDataPort, int graphId, int partitionId,
                                                    std::string masterIP);

    static long aggregateCentralStoreTriangles(SQLiteDBInterface *sqlite, std::string graphId, std::string masterIP,
                                               int threadPriority,
                                               const std::map<string, std::vector<string>> &partitionMap);

    static string countCentralStoreTriangles(std::string aggregatorPort, std::string host, std::string partitionId,
                                             std::string partitionIdList, std::string graphId, std::string masterIP,
                                             int threadPriority, std::string traceContext);

    static bool proceedOrNot(std::set<string> partitionSet, int partitionId);

    static void updateMap(int partitionId);

 private:
    SQLiteDBInterface *sqlite;
    PerformanceSQLiteDBInterface *perfDB;
};

// Partition filtering and allocation helper functions (shared between executors)
void allocate(int p, std::string w, std::map<int, std::string> &alloc, std::set<int> &remain,
             std::map<int, std::vector<std::string>> &p_avail, std::map<std::string, int> &loads);

int alloc_plan(std::map<int, std::string> &alloc, std::set<int> &remain, 
               std::map<int, std::vector<std::string>> &p_avail, std::map<std::string, int> &loads);

std::vector<int> reallocate_parts(std::map<int, std::string> &alloc, std::set<int> &remain,
                                  const std::map<int, std::vector<std::string>> &P_AVAIL);

void scale_up(std::map<std::string, int> &loads, std::map<std::string, std::string> &workers, int copy_cnt);

int alloc_net_plan(std::map<int, std::string> &alloc, std::vector<int> &parts,
                   std::map<int, std::pair<std::string, std::string>> &transfer,
                   std::map<std::string, int> &net_loads, std::map<std::string, int> &loads,
                   const std::map<int, std::vector<std::string>> &P_AVAIL, int C);

void filter_partitions(std::map<std::string, std::vector<std::string>> &partitionMap, SQLiteDBInterface *sqlite,
                      std::string graphId);

#endif  // JASMINEGRAPH_TRIANGLECOUNTEXECUTOR_H
