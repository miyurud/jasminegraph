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
#include "../performancedb/PerformanceSQLiteDBInterface.h"
#include "../util/PlacesToNodeMapper.h"
#include "../query/algorithms/triangles/Triangles.h"
#include "core/scheduler/JobScheduler.h"
#include "../server/JasmineGraphServer.h"

class JasmineGraphHashMapCentralStore;

void *frontendservicesesion(std::string masterIP, int connFd, SQLiteDBInterface sqlite,
                            PerformanceSQLiteDBInterface perfSqlite, JobScheduler jobScheduler);

class JasmineGraphFrontEnd {
public:
    JasmineGraphFrontEnd(SQLiteDBInterface db, PerformanceSQLiteDBInterface perfDb, std::string masterIP, JobScheduler jobScheduler);

    int run();

    void setServer(JasmineGraphServer s);

    static void initiateEntityResolutionOrg(std::string graphID, SQLiteDBInterface sqlite, std::string masterIP,
                                            std::string designatedWorkerHost, std::string designatedWorkerPort);

    static void initiateEntityResolutionCoordinator(std::string graphID, SQLiteDBInterface sqlite, std::string masterIP,
                                                    std::string designatedWorkerHost, std::string designatedWorkerPort,
                                                    std::string designatedWorkerDataPort);

    static bool graphExists(std::string basic_string, SQLiteDBInterface sqlite);

    static bool graphExistsByID(std::string id, SQLiteDBInterface sqlite);

    static void removeGraph(std::string graphID, SQLiteDBInterface sqlite, std::string masterIP);


    static void getAndUpdateUploadTime(std::string graphID, SQLiteDBInterface sqlite);

    static bool isGraphActiveAndTrained(std::string graphID, SQLiteDBInterface sqlite);

    static JasmineGraphHashMapCentralStore loadCentralStore(std::string centralStoreFileName);

    static map<long, long> getOutDegreeDistributionHashMap(map<long, unordered_set<long>> graphMap);

    static bool isGraphActive(string graphID, SQLiteDBInterface sqlite);

    static int getUid();

    static bool isQueueTimeAcceptable(SQLiteDBInterface sqlite, PerformanceSQLiteDBInterface perfSqlite, std::string graphId,
            std::string command, std::string category);

    static int getRunningHighPriorityTaskCount();


    static std::vector<std::vector<string>> fileCombinations;
    static std::map<std::string, std::string> combinationWorkerMap;
    static std::map<long, std::map<long, std::vector<long>>> triangleTree;

private:
    SQLiteDBInterface sqlite;
    std::string masterIP;
    PerformanceSQLiteDBInterface perfSqlite;
    JobScheduler jobScheduler;
    JasmineGraphServer *server;


};

struct frontendservicesessionargs {
    SQLiteDBInterface sqlite;
    int connFd;
};

#endif //JASMINGRAPH_JASMINGRAPHFRONTEND_H
