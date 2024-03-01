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

#include <dirent.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <chrono>
#include <ctime>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include "../localstore/JasmineGraphHashMapLocalStore.h"
#include "../localstore/JasmineGraphLocalStore.h"
#include "../localstore/JasmineGraphLocalStoreFactory.h"
#include "../localstore/incremental/JasmineGraphIncrementalLocalStore.h"
#include "../performance/metrics/StatisticCollector.h"
#include "../query/algorithms/triangles/Triangles.h"
#include "../util/Conts.h"
#include "../util/Utils.h"
#include "JasmineGraphInstanceProtocol.h"

void *instanceservicesession(void *dummyPt);
void writeCatalogRecord(string record);
int deleteGraphPartition(std::string graphID, std::string partitionID);
void removeGraphFragments(std::string graphID);
long countLocalTriangles(
    std::string graphId, std::string partitionId,
    std::map<std::string, JasmineGraphHashMapLocalStore> &graphDBMapLocalStores,
    std::map<std::string, JasmineGraphHashMapCentralStore> &graphDBMapCentralStores,
    std::map<std::string, JasmineGraphHashMapDuplicateCentralStore> &graphDBMapDuplicateCentralStores,
    int threadPriority);

map<long, long> calculateOutDegreeDist(string graphID, string partitionID, int serverPort,
                                       std::map<std::string, JasmineGraphHashMapLocalStore> &graphDBMapLocalStores,
                                       std::map<std::string, JasmineGraphHashMapCentralStore> &graphDBMapCentralStores,
                                       std::vector<string> &workerSockets);

map<long, long> calculateLocalOutDegreeDist(
    string graphID, string partitionID, std::map<std::string, JasmineGraphHashMapLocalStore> &graphDBMapLocalStores,
    std::map<std::string, JasmineGraphHashMapCentralStore> &graphDBMapCentralStores);

map<long, long> calculateInDegreeDist(string graphID, string partitionID, int serverPort,
                                      std::map<std::string, JasmineGraphHashMapLocalStore> &graphDBMapLocalStores,
                                      std::map<std::string, JasmineGraphHashMapCentralStore> &graphDBMapCentralStores,
                                      std::vector<string> &workerSockets, string workerList);

map<long, long> calculateLocalInDegreeDist(
    string graphID, string partitionID, std::map<std::string, JasmineGraphHashMapLocalStore> &graphDBMapLocalStores,
    std::map<std::string, JasmineGraphHashMapCentralStore> &graphDBMapCentralStores);

map<long, map<long, unordered_set<long>>> calculateLocalEgoNet(string graphID, string partitionID, int serverPort,
                                                               JasmineGraphHashMapLocalStore localDB,
                                                               JasmineGraphHashMapCentralStore centralDB,
                                                               std::vector<string> &workerSockets);

void calculateEgoNet(string graphID, string partitionID, int serverPort, JasmineGraphHashMapLocalStore localDB,
                     JasmineGraphHashMapCentralStore centralDB, string workerList);

map<long, double> calculateLocalPageRank(string graphID, double alpha, string partitionID, int serverPort,
                                         int top_k_page_rank_value, string graphVertexCount,
                                         JasmineGraphHashMapLocalStore localDB,
                                         JasmineGraphHashMapCentralStore centralDB, std::vector<string> &workerSockets,
                                         int iterations);

map<long, double> getAuthorityScoresWorldToLocal(string graphID, string partitionID, int serverPort,
                                                 string graphVertexCount, JasmineGraphHashMapLocalStore localDB,
                                                 JasmineGraphHashMapCentralStore centralDB,
                                                 map<long, unordered_set<long>> &graphVertexMap,
                                                 std::vector<string> &workerSockets, long worldOnlyVertexCount);

map<long, unordered_set<long>> getEdgesWorldToLocal(string graphID, string partitionID, int serverPort,
                                                    string graphVertexCount, JasmineGraphHashMapLocalStore localDB,
                                                    JasmineGraphHashMapCentralStore centralDB,
                                                    map<long, unordered_set<long>> &graphVertexMap,
                                                    std::vector<string> &workerSockets);

struct instanceservicesessionargs {
    string masterHost;
    string host;
    int connFd;
    int port;
    int dataPort;
    std::map<std::string, JasmineGraphHashMapLocalStore> graphDBMapLocalStores;
    std::map<std::string, JasmineGraphHashMapCentralStore> graphDBMapCentralStores;
    std::map<std::string, JasmineGraphHashMapDuplicateCentralStore> graphDBMapDuplicateCentralStores;
    std::map<std::string, JasmineGraphIncrementalLocalStore *> incrementalLocalStore;
};

class JasmineGraphInstanceService {
 public:
    static const int MESSAGE_SIZE = 10;
    static const string END_OF_MESSAGE;
    JasmineGraphInstanceService();

    void run(string masterHost, string hostName, int serverPort, int serverDataPort);

    static bool isGraphDBExists(std::string graphId, std::string partitionId);
    static bool isInstanceCentralStoreExists(std::string graphId, std::string partitionId);
    static bool isInstanceDuplicateCentralStoreExists(std::string graphId, std::string partitionId);
    static void loadLocalStore(std::string graphId, std::string partitionId,
                               std::map<std::string, JasmineGraphHashMapLocalStore> &graphDBMapLocalStores);
    static JasmineGraphIncrementalLocalStore *loadStreamingStore(
        std::string graphId, std::string partitionId,
        std::map<std::string, JasmineGraphIncrementalLocalStore *> &graphDBMapStreamingStores, std::string openMode);
    static void loadInstanceCentralStore(
        std::string graphId, std::string partitionId,
        std::map<std::string, JasmineGraphHashMapCentralStore> &graphDBMapCentralStores);
    static void loadInstanceDuplicateCentralStore(
        std::string graphId, std::string partitionId,
        std::map<std::string, JasmineGraphHashMapDuplicateCentralStore> &graphDBMapDuplicateCentralStores);
    static JasmineGraphHashMapCentralStore loadCentralStore(std::string centralStoreFileName);
    static string aggregateCentralStoreTriangles(std::string graphId, std::string partitionId,
                                                 std::string partitionIdList, int threadPriority);
    static string aggregateCompositeCentralStoreTriangles(std::string compositeFileList, std::string availableFileList,
                                                          int threadPriority);
    static map<long, long> getOutDegreeDistributionHashMap(map<long, unordered_set<long>> &graphMap);

    struct workerPartitions {
        int port;
        int dataPort;
        std::vector<std::string> partitionID;
    };

    static void collectTrainedModels(
        instanceservicesessionargs *sessionargs, std::string graphID,
        std::map<std::string, JasmineGraphInstanceService::workerPartitions> &graphPartitionedHosts,
        int totalPartitions);

    static int collectTrainedModelThreadFunction(instanceservicesessionargs *sessionargs, std::string host, int port,
                                                 int dataPort, std::string graphID, std::string partition);

    static void createPartitionFiles(std::string graphID, std::string partitionID, std::string fileType);

    static void collectExecutionData(int iteration, std::string trainArgs, std::string partCount);

    static void executeTrainingIterations(int maxThreads);

    static void trainPartition(std::string trainData);

    static void startCollectingLoadAverage();

    static void initServer(string trainData);

    static void initOrgServer(string trainData);

    static void initAgg(string trainData);

    static void initClient(string trainData);

    static void mergeFiles(string trainData);

    static std::map<int, std::vector<std::string>> iterationData;

    static bool duplicateCentralStore(int thisWorkerPort, int graphID, int partitionID,
                                      std::vector<string> &workerSockets, std::string masterIP);

    static string aggregateStreamingCentralStoreTriangles(
        std::string graphId, std::string partitionId, std::string partitionIdString, std::string centralCountString,
        int threadPriority, std::map<std::string, JasmineGraphIncrementalLocalStore *> &incrementalLocalStores,
        std::string mode);

    static int partitionCounter;

    static std::thread workerThread;
};

#endif  // JASMINEGRAPH_JASMINEGRAPHINSTANCESERVICE_H
