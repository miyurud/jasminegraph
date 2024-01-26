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

    static long getTriangleCount(int graphId, std::string host, int port, int dataPort, int partitionId,
                                 std::string masterIP, int uniqueId, bool isCompositeAggregation, int threadPriority);

    static long aggregateCentralStoreTriangles(SQLiteDBInterface *sqlite, std::string graphId, std::string masterIP,
                                               int threadPriority);

    static string isFileAccessibleToWorker(std::string graphId, std::string partitionId, std::string aggregatorHostName,
                                           std::string aggregatorPort, std::string masterIP, std::string fileType,
                                           std::string fileName);

    static std::string copyCompositeCentralStoreToAggregator(std::string aggregatorHostName, std::string aggregatorPort,
                                                             std::string aggregatorDataPort, std::string fileName,
                                                             std::string masterIP);

    static string countCompositeCentralStoreTriangles(std::string aggregatorHostName, std::string aggregatorPort,
                                                      std::string compositeCentralStoreFileList, std::string masterIP,
                                                      std::string availableFileList, int threadPriority);

    static std::vector<std::vector<string>> getWorkerCombination(SQLiteDBInterface *sqlite, std::string graphId);

    static std::string copyCentralStoreToAggregator(std::string aggregatorHostName, std::string aggregatorPort,
                                                    std::string aggregatorDataPort, int graphId, int partitionId,
                                                    std::string masterIP);

    static string countCentralStoreTriangles(std::string aggregatorHostName, std::string aggregatorPort,
                                             std::string host, std::string partitionId, std::string partitionIdList,
                                             std::string graphId, std::string masterIP, int threadPriority);

    static bool proceedOrNot(std::set<string> partitionSet, int partitionId);

    static void updateMap(int partitionId);

    static int updateTriangleTreeAndGetTriangleCount(std::vector<std::string> triangles);

    static std::vector<std::vector<string>> fileCombinations;
    static std::map<std::string, std::string> combinationWorkerMap;
    static std::map<long, std::map<long, std::vector<long>>> triangleTree;

    static int collectPerformaceData(PerformanceSQLiteDBInterface *perDB, std::string graphId, std::string command,
                                     std::string category, int partitionCount, std::string masterIP,
                                     bool autoCalibrate);

 private:
    SQLiteDBInterface *sqlite;
    PerformanceSQLiteDBInterface *perfDB;
};

#endif  // JASMINEGRAPH_TRIANGLECOUNTEXECUTOR_H
