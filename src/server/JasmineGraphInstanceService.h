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

void *instanceservicesession(void *dummyPt);
void writeCatalogRecord(string record);
void deleteGraphPartition(std::string graphID, std::string partitionID);

class JasmineGraphInstanceService {
private:
    std::string dataFolder;
    std::vector<std::string> loadedGraphs;

    std::string countLocalTriangles(std::string graphId, std::string partitionId, std::map<std::string,JasmineGraphHashMapLocalStore> graphDBMapLocalStores);
    bool isGraphDBExists(std::string graphId, std::string partitionId);
    void loadLocalStore(std::string graphId, std::string partitionId, std::map<std::string,JasmineGraphHashMapLocalStore> graphDBMapLocalStores);
public:
    JasmineGraphInstanceService();

    int run(int serverPort);

};

struct instanceservicesessionargs {
    int connFd;
    std::map<std::string,JasmineGraphHashMapLocalStore> graphDBMapLocalStores;
    std::map<std::string,JasmineGraphHashMapCentralStore> graphDBMapCentralStores;
};

#endif //JASMINEGRAPH_JASMINEGRAPHINSTANCESERVICE_H


