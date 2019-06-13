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
long countLocalTriangles(std::string graphId, std::string partitionId, std::map<std::string,JasmineGraphHashMapLocalStore> graphDBMapLocalStores, std::map<std::string,JasmineGraphHashMapCentralStore> graphDBMapCentralStores);

struct instanceservicesessionargs {
    string host;
    int connFd;
    int port;
    int dataPort;
};

class JasmineGraphInstanceService {
public:
    JasmineGraphInstanceService();

    int run(string hostName, int serverPort, int serverDataPort);

    static bool isGraphDBExists(std::string graphId, std::string partitionId);
    static bool isInstanceCentralStoreExists(std::string graphId, std::string partitionId);
    static void loadLocalStore(std::string graphId, std::string partitionId, std::map<std::string,JasmineGraphHashMapLocalStore>& graphDBMapLocalStores);
    static void loadInstanceCentralStore(std::string graphId, std::string partitionId, std::map<std::string,JasmineGraphHashMapCentralStore>& graphDBMapCentralStores);
    static JasmineGraphHashMapCentralStore loadCentralStore(std::string centralStoreFileName);
    static std::string copyCentralStoreToAggregator(std::string graphId, std::string partitionId, std::string aggregatorHost, std::string aggregatorPort, std::string host);
    static long aggregateCentralStoreTriangles (std::string graphId, std::string partitionId);
    static map<long, long> getOutDegreeDistributionHashMap(map<long, unordered_set<long>> graphMap);
    static std::string requestPerformanceStatistics(std::string isVMStatManager);

};
    struct workerPartitions {
        int port;
        int dataPort;
        std::vector<std::string> partitionID;
    };

struct instanceservicesessionargs {
    int connFd;
    std::map<std::string,JasmineGraphHashMapLocalStore> graphDBMapLocalStores;
    std::map<std::string,JasmineGraphHashMapCentralStore> graphDBMapCentralStores;
    static void collectTrainedModels(instanceservicesessionargs *sessionargs, std::string graphID,
                                     std::map<std::string, JasmineGraphInstanceService::workerPartitions> graphPartitionedHosts,
                                     int totalPartitions);

    static int collectTrainedModelThreadFunction(instanceservicesessionargs *sessionargs, std::string host, int port, int dataPort,
                                      std::string graphID, std::string partition);
};


#endif //JASMINEGRAPH_JASMINEGRAPHINSTANCESERVICE_H
