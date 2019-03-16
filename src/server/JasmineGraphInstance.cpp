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

#include "JasmineGraphInstance.h"
#include "../util/logger/Logger.h"

void *runInstanceService(void *dummyPt) {
    JasmineGraphInstance *refToInstance = (JasmineGraphInstance *) dummyPt;
    refToInstance->instanceService = new JasmineGraphInstanceService();
    refToInstance->instanceService->run(refToInstance->serverPort);
}

void *runFileTransferService(void *dummyPt) {
    JasmineGraphInstance *refToInstance = (JasmineGraphInstance *) dummyPt;
    refToInstance->ftpService = new JasmineGraphInstanceFileTransferService();
    refToInstance->ftpService->run(refToInstance->serverDataPort);
}

int JasmineGraphInstance::start_running(int serverPort, int serverDataPort) {
    std::cout << "Worker started" << std::endl;
    std::cout << "Running the server..." << std::endl;

    this->sqlite = *new SQLiteDBInterface();
    this->sqlite.init();
    this->serverPort = serverPort;
    this->serverDataPort = serverDataPort;

    pthread_t instanceCommunicatorThread;
    pthread_t instanceFileTransferThread;
    pthread_create(&instanceCommunicatorThread, NULL, runInstanceService, this);
    pthread_create(&instanceFileTransferThread, NULL, runFileTransferService, this);

}

bool JasmineGraphInstance::isRunning() {
    return true;
}