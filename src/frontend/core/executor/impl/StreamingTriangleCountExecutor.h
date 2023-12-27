//
// Created by ashokkumar on 27/12/23.
//

#ifndef JASMINEGRAPH_STREAMINGTRIANGLECOUNTEXECUTOR_H
#define JASMINEGRAPH_STREAMINGTRIANGLECOUNTEXECUTOR_H


#include <chrono>

#include "../../../../metadb/SQLiteDBInterface.h"
#include "../../../../performance/metrics/PerformanceUtil.h"
#include "../../../../performancedb/PerformanceSQLiteDBInterface.h"
#include "../../../../server/JasmineGraphInstanceProtocol.h"
#include "../../../../server/JasmineGraphServer.h"
#include "../../../JasmineGraphFrontEndProtocol.h"
#include "../../CoreConstants.h"
#include "../AbstractExecutor.h"


class StreamingTriangleCountExecutor : public AbstractExecutor{
public:
    StreamingTriangleCountExecutor();

    StreamingTriangleCountExecutor(SQLiteDBInterface db, PerformanceSQLiteDBInterface perfDb, JobRequest jobRequest);

    void execute();

    int getUid();

//    static std::vector<std::vector<string>> getCombinations(std::vector<string> inputVector);

    static long getTriangleCount(int graphId, std::string host, int port, int dataPort, int partitionId,
                                 std::string masterIP);

//    static long aggregateCentralStoreTriangles(SQLiteDBInterface sqlite, std::string graphId, std::string masterIP,
//                                               int threadPriority);

//    static string isFileAccessibleToWorker(std::string graphId, std::string partitionId, std::string aggregatorHostName,
//                                           std::string aggregatorPort, std::string masterIP, std::string fileType,
//                                           std::string fileName);
//
//    static std::string copyCompositeCentralStoreToAggregator(std::string aggregatorHostName, std::string aggregatorPort,
//                                                             std::string aggregatorDataPort, std::string fileName,
//                                                             std::string masterIP);
//
//    static string countCompositeCentralStoreTriangles(std::string aggregatorHostName, std::string aggregatorPort,
//                                                      std::string compositeCentralStoreFileList, std::string masterIP,
//                                                      std::string availableFileList, int threadPriority);

    static std::vector<std::vector<string>> getWorkerCombination(SQLiteDBInterface sqlite, std::string graphId);

//    static std::string copyCentralStoreToAggregator(std::string aggregatorHostName, std::string aggregatorPort,
//                                                    std::string aggregatorDataPort, int graphId, int partitionId,
//                                                    std::string masterIP);

//    static string countCentralStoreTriangles(std::string aggregatorHostName, std::string aggregatorPort,
//                                             std::string host, std::string partitionId, std::string partitionIdList,
//                                             std::string graphId, std::string masterIP, int threadPriority);

//    static bool proceedOrNot(std::set<string> partitionSet, int partitionId);
//
//    static void updateMap(int partitionId);
//
//    static int updateTriangleTreeAndGetTriangleCount(std::vector<std::string> triangles);

    static std::vector<std::vector<string>> fileCombinations;
    static std::map<std::string, std::string> combinationWorkerMap;
    //static std::map<long, std::map<long, std::vector<long>>> triangleTree;
//
//    static int collectPerformaceData(PerformanceSQLiteDBInterface perDB, std::string graphId, std::string command,
//                                     std::string category, int partitionCount, std::string masterIP,
//                                     bool autoCalibrate);

private:
    SQLiteDBInterface sqlite;
    PerformanceSQLiteDBInterface perfDB;
};


#endif //JASMINEGRAPH_STREAMINGTRIANGLECOUNTEXECUTOR_H
