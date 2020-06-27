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
#include "../metadb/SQLiteDBInterface.h"
#include "../util/PlacesToNodeMapper.h"
#include "../centralstore/JasmineGraphHashMapCentralStore.h"

void *frontendservicesesion(std::string masterIP, int connFd, SQLiteDBInterface sqlite);

class JasmineGraphFrontEnd {
public:
    JasmineGraphFrontEnd(SQLiteDBInterface db, std::string masterIP);

    int run();

    static bool graphExists(std::string basic_string, SQLiteDBInterface sqlite);

    static bool modelExists(std::string basic_string, SQLiteDBInterface sqlite);

    static bool graphExistsByID(std::string id, SQLiteDBInterface sqlite);

    static bool modelExistsByID(std::string id, SQLiteDBInterface sqlite);

    static void removeGraph(std::string graphID, SQLiteDBInterface sqlite, std::string masterIP);

    static std::string copyCentralStoreToAggregator(std::string aggregatorHostName, std::string aggregatorPort, std::string host, std::string port, int graphId, int partitionId, std::string masterIP);

    static long countCentralStoreTriangles (std::string aggregatorHostName, std::string aggregatorPort, std::string host, std::string partitionId, std::string graphId, std::string masterIP);

    static long countTriangles(std::string graphId, SQLiteDBInterface sqlite, std::string masterIP);

    static long getTriangleCount(int graphId, std::string host, int port, int partitionId, std::string masterIP);

    static void getAndUpdateUploadTime(std::string graphID, SQLiteDBInterface sqlite);

    static bool isGraphActiveAndTrained(std::string graphID, SQLiteDBInterface sqlite);

private:
    SQLiteDBInterface sqlite;
    std::string masterIP;

};

struct frontendservicesessionargs {
    SQLiteDBInterface sqlite;
    int connFd;
};

#endif //JASMINGRAPH_JASMINGRAPHFRONTEND_H
