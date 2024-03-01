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

#ifndef JASMINEGRAPH_JASMINEGRAPHINSTANCE_H
#define JASMINEGRAPH_JASMINEGRAPHINSTANCE_H

#include <map>

#include "../localstore/JasmineGraphHashMapLocalStore.h"
#include "../localstore/JasmineGraphLocalStore.h"
#include "../metadb/SQLiteDBInterface.h"
#include "../performance/metrics/PerformanceUtil.h"
#include "JasmineGraphInstanceFileTransferService.h"
#include "JasmineGraphInstanceService.h"

using std::map;

class JasmineGraphInstance {
 private:
    map<std::string, JasmineGraphLocalStore> graphDBMapLocalStores;
    static const int BUFFER_SIZE = 128;

 public:
    int start_running(string hostName, string masterHost, int serverPort, int serverDataPort, string enableNmon);

    bool acknowledgeMaster(string masterHost, string workerIP, string workerPort);

    void startNmonAnalyzer(string enableNmon, int serverPort);

    void registerShutdownHook();

    void truncate();

    void shutdown();

    string hostName;
    string masterHostName;
    int serverPort;
    int serverDataPort;
    string enableNmon;

    JasmineGraphInstanceService *instanceService;
    JasmineGraphInstanceFileTransferService *ftpService;
};

#endif  // JASMINEGRAPH_JASMINEGRAPHINSTANCE_H
