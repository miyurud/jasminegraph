/**
Copyright 2019 JasmineGraph Team
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

#ifndef JASMINEGRAPH_JASMINEGRAPHSERVER_H
#define JASMINEGRAPH_JASMINEGRAPHSERVER_H

#include <map>
#include <thread>
#include <stdio.h>
#include <stdlib.h>
#include "../frontend/JasmineGraphFrontEnd.h"
#include "../backend/JasmineGraphBackend.h"
#include "../metadb/SQLiteDBInterface.h"
#include "../util/Conts.h"

using std::map;

class JasmineGraphServer {
private:
    map<std::string, long> hostPlaceMap;
    int numberOfWorkers;
    int serverPort;
    int serverDataPort;
    std::map<std::string, std::vector<int>> workerPortsMap;
    std::map<std::string, std::vector<int>> workerDataPortsMap;

    static void startRemoteWorkers(std::vector<int> workerPortsVector, std::vector<int> workerDataPortsVector, std::string host);

    static bool sendFileThroughService(std::string host, int dataPort, std::string fileName, std::string filePath);

public:
    ~JasmineGraphServer();

    JasmineGraphServer();

    void init();

    void start_workers();

    int run();

    bool isRunning();

    void uploadGraphLocally(int graphID);

    static bool batchUploadFile(std::string host, int port, int dataPort, int graphID, std::string filePath);

    JasmineGraphFrontEnd *frontend;
    SQLiteDBInterface sqlite;
    JasmineGraphBackend *backend;
    //pthread_t frontendthread;
};


#endif //JASMINEGRAPH_JASMINEGRAPHSERVER_H
