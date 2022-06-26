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

#ifndef JASMINEGRAPH_JASMINEGRAPHINSTANCESERVICE_H
#define JASMINEGRAPH_JASMINEGRAPHINSTANCESERVICE_H

#include "JasmineGraphInstanceProtocol.h"
#include "../localstore/JasmineGraphLocalStore.h"
#include "../localstore/JasmineGraphHashMapLocalStore.h"
#include "../localstore/JasmineGraphLocalStoreFactory.h"
#include "../query/algorithms/triangles/Triangles.h"
#include "../util/Utils.h"
#include "../util/Conts.h"
#include "../localstore/incremental/JasmineGraphIncrementalLocalStore.h"
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <iostream>
#include <fstream>
#include <strings.h>
#include <stdlib.h>
#include <string>
#include <pthread.h>
#include <thread>
#include <map>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include "../util/performance/StatisticCollector.h"
#include <chrono>
#include <ctime>
#include <vector>

void *instanceservicesession(void *dummyPt);
void writeCatalogRecord(string record);
void deleteGraphPartition(std::string graphID, std::string partitionID);
void removeGraphFragments(std::string graphID);
long countLocalTriangles(std::string graphId, std::string partitionId,
                         std::map<std::string, JasmineGraphHashMapLocalStore> graphDBMapLocalStores,
                         std::map<std::string, JasmineGraphHashMapCentralStore> graphDBMapCentralStores,
                         std::map<std::string, JasmineGraphHashMapDuplicateCentralStore> graphDBMapDuplicateCentralStores,
                         int threadPriority);

map<long, long> calculateOutDegreeDist(string graphID, string partitionID, int serverPort,
                                       std::map<std::string, JasmineGraphHashMapLocalStore> graphDBMapLocalStores,
                                       std::map<std::string,JasmineGraphHashMapCentralStore> graphDBMapCentralStores,
                                       std::vector<string> workerSockets);

map<long, long> calculateLocalOutDegreeDist(string graphID, string partitionID,
                                            std::map<std::string, JasmineGraphHashMapLocalStore> graphDBMapLocalStores,
                                            std::map<std::string,JasmineGraphHashMapCentralStore> graphDBMapCentralStores);

map<long, long> calculateInDegreeDist(string graphID, string partitionID, int serverPort,
                                      std::map<std::string, JasmineGraphHashMapLocalStore> graphDBMapLocalStores,
                                      std::map<std::string,JasmineGraphHashMapCentralStore> graphDBMapCentralStores,
                                      std::vector<string> workerSockets);

map<long, long> calculateLocalInDegreeDist(string graphID, string partitionID,
                                           std::map<std::string, JasmineGraphHashMapLocalStore> graphDBMapLocalStores,
                                           std::map<std::string,JasmineGraphHashMapCentralStore> graphDBMapCentralStores);

map <long, map<long, unordered_set<long>>> calculateLocalEgoNet(string graphID, string partitionID, int serverPort,
                                                                JasmineGraphHashMapLocalStore localDB,
                                                                JasmineGraphHashMapCentralStore centralDB,
                                                                std::vector<string> workerSockets);

map<long, map<long, unordered_set<long>>> calculateEgoNet(string graphID, string partitionID,
                                                          int serverPort, JasmineGraphHashMapLocalStore localDB,
                                                          JasmineGraphHashMapCentralStore centralDB,
                                                          string workerList);

void calculateLocalPageRank(string graphID, double alpha, string partitionID, int serverPort, int top_k_page_rank_value,
                            string graphVertexCount, JasmineGraphHashMapLocalStore localDB,
                            JasmineGraphHashMapCentralStore centralDB,
                            std::vector<string> workerSockets, int iterations);

map<long, float> getAuthorityScoresWorldToLocal(string graphID, string partitionID, int serverPort,
                                                string graphVertexCount, JasmineGraphHashMapLocalStore localDB,
                                                JasmineGraphHashMapCentralStore centralDB,
                                                map<long, unordered_set<long>> graphVertexMap,
                                                std::vector<string> workerSockets, long worldOnlyVertexCount);

map<long, unordered_set<long>> getEdgesWorldToLocal(string graphID, string partitionID, int serverPort,
                                                    string graphVertexCount, JasmineGraphHashMapLocalStore localDB,
                                                    JasmineGraphHashMapCentralStore centralDB,
                                                    map<long, unordered_set<long>> graphVertexMap,
                                                    std::vector<string> workerSockets);

struct instanceservicesessionargs {
    string profile;
    string masterHost;
    string host;
    int connFd;
    int port;
    int dataPort;
    std::map<std::string,JasmineGraphHashMapLocalStore> graphDBMapLocalStores;
    std::map<std::string,JasmineGraphHashMapCentralStore> graphDBMapCentralStores;
    std::map<std::string,JasmineGraphHashMapDuplicateCentralStore> graphDBMapDuplicateCentralStores;
    std::map<std::string,JasmineGraphIncrementalLocalStore*> incrementalLocalStore;
};

class JasmineGraphInstanceService {

public:
    static const int MESSAGE_SIZE = 10;
    static const string END_OF_MESSAGE;
    JasmineGraphInstanceService();

    int run(string profile, string masterHost, string hostName, int serverPort, int serverDataPort);

    static bool isGraphDBExists(std::string graphId, std::string partitionId);
    static bool isInstanceCentralStoreExists(std::string graphId, std::string partitionId);
    static bool isInstanceDuplicateCentralStoreExists(std::string graphId, std::string partitionId);
    static void loadLocalStore(std::string graphId, std::string partitionId, std::map<std::string,JasmineGraphHashMapLocalStore>& graphDBMapLocalStores);
    static JasmineGraphIncrementalLocalStore* loadStreamingStore(
        std::string graphId, std::string partitionId,
        std::map<std::string, JasmineGraphIncrementalLocalStore*>& graphDBMapStreamingStores);
    static void loadInstanceCentralStore(std::string graphId, std::string partitionId, std::map<std::string,JasmineGraphHashMapCentralStore>& graphDBMapCentralStores);
    static void loadInstanceDuplicateCentralStore(std::string graphId, std::string partitionId, std::map<std::string,JasmineGraphHashMapDuplicateCentralStore>& graphDBMapDuplicateCentralStores);
    static JasmineGraphHashMapCentralStore loadCentralStore(std::string centralStoreFileName);
    static std::string copyCentralStoreToAggregator(std::string graphId, std::string partitionId, std::string aggregatorHost, std::string aggregatorPort, std::string host);
    static string aggregateCentralStoreTriangles(std::string graphId, std::string partitionId, std::string partitionIdList,
                                   int threadPriority);
    static string aggregateCompositeCentralStoreTriangles(std::string compositeFileList, std::string availableFileList,
                                                          int threadPriority);
    static map<long, long> getOutDegreeDistributionHashMap(map<long, unordered_set<long>> graphMap);
    static string requestPerformanceStatistics(std::string isVMStatManager, std::string isResourceAllocationRequested);

    struct workerPartitions {
        int port;
        int dataPort;
        std::vector<std::string> partitionID;
    };

    static void collectTrainedModels(instanceservicesessionargs *sessionargs, std::string graphID,
                                     std::map<std::string, JasmineGraphInstanceService::workerPartitions> graphPartitionedHosts,
                                     int totalPartitions);

    static int collectTrainedModelThreadFunction(instanceservicesessionargs *sessionargs, std::string host, int port, int dataPort,
                                                 std::string graphID, std::string partition);

    static void createPartitionFiles(std::string graphID, std::string partitionID, std::string fileType);

    static void collectExecutionData(std::string iteration, std::string trainArgs, std::string partCount);

    static void executeTrainingIterations(int maxThreads);

    static void trainPartition(std::string trainData);

    static std::map<int,std::vector<std::string>> iterationData;

    static map<long, long> calculateLocalOutDegreeDistribution(string graphID, string partitionID,
                                                               std::map<std::string,JasmineGraphHashMapLocalStore> graphDBMapLocalStores,
                                                               std::map<std::string,JasmineGraphHashMapCentralStore> graphDBMapCentralStores);

    static bool duplicateCentralStore(int thisWorkerPort, int graphID, int partitionID,
                               std::vector<string> workerSockets, std::string masterIP);

    static bool sendFileThroughService(std::string host, int dataPort, std::string fileName,
                                       std::string filePath, std::string masterIP);
    static int partitionCounter;
};




#endif //JASMINEGRAPH_JASMINEGRAPHINSTANCESERVICE_H
