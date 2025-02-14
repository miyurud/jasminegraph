/*
 * Copyright 2023 JasminGraph Team
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef INSTANCESTREAMHANDLER_H
#define INSTANCESTREAMHANDLER_H

#include <mutex>
#include <thread>
#include <condition_variable>
#include <queue>
#include <map>
#include <string>
#include <atomic>
#include "../../localstore/incremental/JasmineGraphIncrementalLocalStore.h"

class InstanceStreamHandler {
 public:
    InstanceStreamHandler(std::map<std::string, JasmineGraphIncrementalLocalStore*>& incrementalLocalStoreMap);
    ~InstanceStreamHandler();

    void handleRequest(const std::string& nodeString);
    void handleLocalEdge(const std::pair<std::string, std::string> &edge, std::string graphId,
                         std::string partitionId, std::string graphIdentifier);
    void handleCentralEdge(const std::pair<std::string, std::string> &edge, std::string graphId,
                           std::string partitionId, std::string graphIdentifier);

 private:
    std::map<std::string, JasmineGraphIncrementalLocalStore*>& incrementalLocalStoreMap;
    std::map<std::string, std::thread> threads;
    std::map<std::string, std::queue<std::string>> queues;
    std::map<std::string, std::condition_variable> cond_vars;
    std::map<std::string, std::mutex> queue_mutexes;
    std::atomic<bool> terminateThreads{false};

        void threadFunction(const std::string& nodeString);
        static std::string extractGraphIdentifier(const std::string& nodeString);
        static JasmineGraphIncrementalLocalStore *loadStreamingStore(
                std::string graphId, std::string partitionId, std::map<std::string,
                JasmineGraphIncrementalLocalStore *> &graphDBMapStreamingStores);
};
#endif  // INSTANCESTREAMHANDLER_H
