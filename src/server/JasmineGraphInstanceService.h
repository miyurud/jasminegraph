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
#include <vector>
#include <map>

void *instanceservicesession(void *dummyPt);
void writeCatalogRecord(string record);
void deleteGraphPartition(std::string graphID, std::string partitionID);

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
};


#endif //JASMINEGRAPH_JASMINEGRAPHINSTANCESERVICE_H
