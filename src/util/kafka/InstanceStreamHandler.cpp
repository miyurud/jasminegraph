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

#include "InstanceStreamHandler.h"
#include "../../localstore/incremental/JasmineGraphIncrementalLocalStore.h"
#include "../Utils.h"
#include "../logger/Logger.h"

Logger instance_stream_logger;
InstanceStreamHandler::InstanceStreamHandler(std::map<std::string,
                                             JasmineGraphIncrementalLocalStore*>& incrementalLocalStoreMap)
        : incrementalLocalStoreMap(incrementalLocalStoreMap) { }

InstanceStreamHandler::~InstanceStreamHandler() { }

void InstanceStreamHandler::handleRequest(const std::string& nodeString) {
    if (nodeString == "-1") {
        terminateThreads = true;

        for (auto& cv : cond_vars) {
            cv.second.notify_all();
        }

         for (auto& thread : threads) {
             if (thread.second.joinable()) {
                 thread.second.join();
             }
         }

        return;
    }
    std::string graphIdentifier = extractGraphIdentifier(nodeString);

    std::unique_lock<std::mutex> lock(queue_mutexes[graphIdentifier]);  // Use specific mutex for the queue
    if (threads.find(graphIdentifier) == threads.end()) {
        threads[graphIdentifier] = std::thread(&InstanceStreamHandler::threadFunction, this, nodeString);
        queues[graphIdentifier] = std::queue<std::string>();
    }

    queues[graphIdentifier].push(nodeString);
    cond_vars[graphIdentifier].notify_one();
    instance_stream_logger.debug("Pushed into the Queue");
}

void InstanceStreamHandler::threadFunction(const std::string& nodeString) {
    std::string graphIdentifier = extractGraphIdentifier(nodeString);
    if (incrementalLocalStoreMap.find(graphIdentifier) == incrementalLocalStoreMap.end()) {
        auto graphIdPartitionId = JasmineGraphIncrementalLocalStore::getIDs(nodeString);
        std::string graphId = graphIdPartitionId.first;
        std::string partitionId = std::to_string(graphIdPartitionId.second);
        instance_stream_logger.info("[PROC THREAD] Creating store for " + graphIdentifier +
                                    " on thread (thread_local fstreams will be set here)");
        loadStreamingStore(graphId, partitionId, incrementalLocalStoreMap);
    }
    JasmineGraphIncrementalLocalStore* localStore = incrementalLocalStoreMap[graphIdentifier];
    instance_stream_logger.info("[PROC THREAD] Processing thread ready for " + graphIdentifier);

    uint64_t edgesProcessed = 0;
    while (!terminateThreads) {
        std::string nodeString;
        {
            std::unique_lock<std::mutex> lock(queue_mutexes[graphIdentifier]);
            cond_vars[graphIdentifier].wait(lock, [&]{
                return !queues[graphIdentifier].empty() || terminateThreads;
            });

            if (terminateThreads) {
                break;
            }
            nodeString = queues[graphIdentifier].front();
            queues[graphIdentifier].pop();
        }
        localStore->addEdgeFromString(nodeString);
        ++edgesProcessed;
        if (edgesProcessed % 50000 == 0 || (edgesProcessed <= 1000 && edgesProcessed % 100 == 0)) {
            instance_stream_logger.info("[PROC THREAD " + graphIdentifier + "] "
                                        + std::to_string(edgesProcessed) + " edges written to store");
        }
    }
    instance_stream_logger.info("[PROC THREAD " + graphIdentifier + "] Terminated after "
                                + std::to_string(edgesProcessed) + " edges");
}

std::string InstanceStreamHandler::extractGraphIdentifier(const std::string& nodeString) {
    auto graphIdPartitionId = JasmineGraphIncrementalLocalStore::getIDs(nodeString);
    std::string graphId = graphIdPartitionId.first;
    std::string partitionId = std::to_string(graphIdPartitionId.second);
    std::string graphIdentifier = graphId + "_" + partitionId;
    return graphIdentifier;
}

JasmineGraphIncrementalLocalStore *
InstanceStreamHandler::loadStreamingStore(std::string graphId, std::string partitionId, map<std::string,
                                          JasmineGraphIncrementalLocalStore *> &graphDBMapStreamingStores,
                                          std::string dbFilesOpenMode , bool isEmbed ) {
    std::string graphIdentifier = graphId + "_" + partitionId;
    instance_stream_logger.info("###INSTANCE### Loading streaming Store for" + graphIdentifier
                               + " : Started");
    std::string folderLocation = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    auto *jasmineGraphStreamingLocalStore = new JasmineGraphIncrementalLocalStore(
                                     stoi(graphId), stoi(partitionId), dbFilesOpenMode, isEmbed);
    graphDBMapStreamingStores.insert(std::make_pair(graphIdentifier, jasmineGraphStreamingLocalStore));
    instance_stream_logger.info("###INSTANCE### Loading Local Store : Completed");
    return jasmineGraphStreamingLocalStore;
}

void InstanceStreamHandler::preInitPartitions(int graphId, const std::vector<int>& partitions) {
    std::lock_guard<std::mutex> guard(mapLock_);
    for (int p : partitions) {
        std::string graphIdent = std::to_string(graphId) + "_" + std::to_string(p);
        // Pre-create mutex/queue/condvar entries for thread safety.
        // Do NOT create stores here — NodeManager uses static thread_local fstream
        // pointers, so the store MUST be created on the thread that will use it.
        // threadFunction() (spawned by handleRequest) will create the store lazily.
        queue_mutexes[graphIdent];
        queues[graphIdent] = std::queue<std::string>();
        cond_vars[graphIdent];
        instance_stream_logger.info("Pre-initialized mutex/queue for partition " + graphIdent);
    }
}

void InstanceStreamHandler::handleLocalEdge(std::string edge, std::string graphId,
                                            std::string partitionId, std::string graphIdentifier , bool isEmbed) {
    JasmineGraphIncrementalLocalStore* localStore;
    std::mutex* partMutex;
    {
        std::lock_guard<std::mutex> guard(mapLock_);
        if (incrementalLocalStoreMap.find(graphIdentifier) == incrementalLocalStoreMap.end()) {
            loadStreamingStore(graphId, partitionId, incrementalLocalStoreMap, NodeManager::FILE_MODE, isEmbed);
        }
        localStore = incrementalLocalStoreMap[graphIdentifier];
        partMutex = &queue_mutexes[graphIdentifier];
    }
    std::unique_lock<std::mutex> lock(*partMutex);
    localStore->addLocalEdge(edge);
}

void InstanceStreamHandler::handleCentralEdge(std::string edge, std::string graphId,
                                              std::string partitionId, std::string graphIdentifier, bool isEmbed) {
    JasmineGraphIncrementalLocalStore* localStore;
    std::mutex* partMutex;
    {
        std::lock_guard<std::mutex> guard(mapLock_);
        if (incrementalLocalStoreMap.find(graphIdentifier) == incrementalLocalStoreMap.end()) {
            loadStreamingStore(graphId, partitionId, incrementalLocalStoreMap, NodeManager::FILE_MODE,
                isEmbed);
        }
        localStore = incrementalLocalStoreMap[graphIdentifier];
        partMutex = &queue_mutexes[graphIdentifier];
    }
    std::unique_lock<std::mutex> lock(*partMutex);
    localStore->addCentralEdge(edge);
}
