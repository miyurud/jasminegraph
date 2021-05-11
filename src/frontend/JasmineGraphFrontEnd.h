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

#ifndef JASMINEGRAPH_JASMINEGRAPHFRONTEND_H
#define JASMINEGRAPH_JASMINEGRAPHFRONTEND_H


#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <iostream>
#include <fstream>
#include <strings.h>
#include <stdlib.h>
#include <string>
#include <pthread.h>
#include <chrono>
#include <thread>
#include <map>
#include <dirent.h>
#include "../metadb/SQLiteDBInterface.h"
#include "../util/PlacesToNodeMapper.h"
#include "../query/algorithms/triangles/Triangles.h"
#include "../server/JasmineGraphServer.h"

class JasmineGraphHashMapCentralStore;

void *frontendservicesesion(std::string masterIP, int connFd, SQLiteDBInterface sqlite);

class JasmineGraphFrontEnd {
public:
    JasmineGraphFrontEnd(SQLiteDBInterface db, std::string masterIP);

    int run();

    void setServer(JasmineGraphServer s);

    void initiateEntityResolution(std::string graphID, SQLiteDBInterface sqlite, std::string masterIP);

    static bool graphExists(std::string basic_string, SQLiteDBInterface sqlite);

    static bool graphExistsByID(std::string id, SQLiteDBInterface sqlite);

    static void removeGraph(std::string graphID, SQLiteDBInterface sqlite, std::string masterIP);

    static std::string copyCentralStoreToAggregator(std::string aggregatorHostName, std::string aggregatorPort, std::string aggregatorDataPort, int graphId, int partitionId, std::string masterIP);

    static std::string copyCompositeCentralStoreToAggregator(std::string aggregatorHostName, std::string aggregatorPort,
                                                             std::string aggregatorDataPort, std::string fileName,
                                                             std::string masterIP);

    static std::vector<std::vector<string>> getWorkerCombination (SQLiteDBInterface sqlite, std::string graphId);

    static std::vector<std::vector<string>> getCombinations (std::vector<string> inputVector);

    static long aggregateCentralStoreTriangles(SQLiteDBInterface sqlite, std::string graphId, std::string masterIP);

    static string countCentralStoreTriangles (std::string aggregatorHostName, std::string aggregatorPort, std::string host, std::string aggregatorPartitionId, std::string partitionIdList, std::string graphId, std::string masterIP);

    static string countCompositeCentralStoreTriangles(std::string aggregatorHostName, std::string aggregatorPort,
                                                      std::string compositeCentralStoreFileList,
                                                      std::string masterIP, std::string availableFileList);

    static long countTriangles(std::string graphId, SQLiteDBInterface sqlite, std::string masterIP);

    static string isFileAccessibleToWorker(std::string graphId, std::string partitionId,
                                           std::string aggregatorHostName, std::string aggregatorPort,
                                           std::string masterIP, std::string fileType,
                                           std::string fileName);

    static long getTriangleCount(int graphId, std::string host, int port, int dataPort, int partitionId,
                                 std::string masterIP, bool isCompositeAggregation);

    static void getAndUpdateUploadTime(std::string graphID, SQLiteDBInterface sqlite);

    static bool isGraphActiveAndTrained(std::string graphID, SQLiteDBInterface sqlite);

    static JasmineGraphHashMapCentralStore loadCentralStore(std::string centralStoreFileName);

    static map<long, long> getOutDegreeDistributionHashMap(map<long, unordered_set<long>> graphMap);

    static bool isGraphActive(string graphID, SQLiteDBInterface sqlite);


    static std::vector<std::vector<string>> fileCombinations;
    static std::map<std::string, std::string> combinationWorkerMap;
    static std::map<long, std::map<long, std::vector<long>>> triangleTree;

private:
    SQLiteDBInterface sqlite;
    std::string masterIP;
    JasmineGraphServer *server;


};

struct frontendservicesessionargs {
    SQLiteDBInterface sqlite;
    int connFd;
};

#endif //JASMINGRAPH_JASMINGRAPHFRONTEND_H
