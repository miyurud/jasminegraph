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
        : incrementalLocalStoreMap(incrementalLocalStoreMap) {
    // Per-partition in-memory queue cap to prevent unbounded RAM growth under ingest spikes.
    // If not set/invalid, keep a conservative default.
    std::string capProp = Utils::getJasmineGraphProperty(
        "org.jasminegraph.server.streaming.localstore.queue.max");
    if (!capProp.empty()) {
        try {
            size_t parsed = static_cast<size_t>(std::stoull(capProp));
            if (parsed > 0) {
                maxQueueSizePerPartition = parsed;
            }
        } catch (const std::exception&) {
            instance_stream_logger.warn("Invalid org.jasminegraph.server.streaming.localstore.queue.max='" +
                                       capProp + "'. Using default " +
                                       std::to_string(maxQueueSizePerPartition));
        }
    }
    instance_stream_logger.info("InstanceStreamHandler queue cap per partition = " +
                                std::to_string(maxQueueSizePerPartition));
}

InstanceStreamHandler::~InstanceStreamHandler() {
    terminateThreads = true;

    for (auto& cv : cond_vars) {
        cv.second.notify_all();
    }
    for (auto& cv : producer_cond_vars) {
        cv.second.notify_all();
    }

    for (auto& thread : threads) {
        if (thread.second.joinable()) {
            thread.second.join();
        }
    }

    std::lock_guard<std::mutex> guard(mapLock_);
    for (const auto& key : ownedStoreKeys_) {
        auto it = incrementalLocalStoreMap.find(key);
        if (it != incrementalLocalStoreMap.end()) {
            delete it->second;
            incrementalLocalStoreMap.erase(it);
        }
    }
    ownedStoreKeys_.clear();
}

void InstanceStreamHandler::handleRequest(const std::string& nodeString) {
    if (nodeString == "-1") {
        terminateThreads = true;

        for (auto& cv : cond_vars) {
            cv.second.notify_all();
        }
        for (auto& cv : producer_cond_vars) {
            cv.second.notify_all();
        }

         for (auto& thread : threads) {
             if (thread.second.joinable()) {
                 thread.second.join();
             }
         }

        return;
    }
    // Slow path: re-parse JSON to extract graphIdentifier (only used by the TCP/non-Kafka path).
    handleRequest(nodeString, extractGraphIdentifier(nodeString));
}

void InstanceStreamHandler::handleRequest(const std::string& nodeString,
                                          const std::string& graphIdentifier) {
    // String slow-path: used by TCP relay. Parse once here, then delegate to
    // the json overload so the writer thread never needs to parse again.
    nlohmann::json obj;
    try {
        obj = nlohmann::json::parse(nodeString);
    } catch (const std::exception& e) {
        instance_stream_logger.error("handleRequest parse error: " + std::string(e.what()));
        return;
    }
    handleRequest(std::move(obj), graphIdentifier);
}

void InstanceStreamHandler::handleRequest(nlohmann::json obj,
                                          const std::string& graphIdentifier) {
    // Fastest path: json already built by caller — no serialisation/deserialisation.
    std::unique_lock<std::mutex> lock(queue_mutexes[graphIdentifier]);
    if (threads.find(graphIdentifier) == threads.end()) {
        threads[graphIdentifier] = std::thread(
            &InstanceStreamHandler::threadFunction, this, graphIdentifier);
        queues[graphIdentifier] = std::queue<nlohmann::json>();
    }

    // Backpressure: block producers when per-partition queue reaches cap.
    // This slows ingestion instead of allowing unbounded RAM growth and OOM.
    producer_cond_vars[graphIdentifier].wait(lock, [&] {
        return terminateThreads || queues[graphIdentifier].size() < maxQueueSizePerPartition;
    });
    if (terminateThreads) {
        return;
    }

    queues[graphIdentifier].push(std::move(obj));
    cond_vars[graphIdentifier].notify_one();
    instance_stream_logger.debug("Pushed into the Queue");
}

void InstanceStreamHandler::threadFunction(const std::string& graphIdentifier) {
    {
        std::lock_guard<std::mutex> guard(mapLock_);
        if (incrementalLocalStoreMap.find(graphIdentifier) == incrementalLocalStoreMap.end()) {
            // Derive graphId and partitionId from "graphId_partitionId" key.
            auto sep = graphIdentifier.find('_');
            std::string graphId     = graphIdentifier.substr(0, sep);
            std::string partitionId = graphIdentifier.substr(sep + 1);
            instance_stream_logger.info("[PROC THREAD] Creating store for " + graphIdentifier +
                                        " on thread (thread_local fstreams will be set here)");
            loadStreamingStore(graphId, partitionId, incrementalLocalStoreMap);
            ownedStoreKeys_.insert(graphIdentifier);
        }
    }
    JasmineGraphIncrementalLocalStore* localStore = incrementalLocalStoreMap[graphIdentifier];
    instance_stream_logger.info("[PROC THREAD] Processing thread ready for " + graphIdentifier);

    uint64_t edgesProcessed = 0;
    while (!terminateThreads) {
        // Batch-drain: swap the full queue out under the lock in O(1), then
        // process every item outside the lock.  Reduces mutex acquisitions from
        // one-per-edge to one-per-batch, cutting producer/consumer contention.
        std::queue<nlohmann::json> localBatch;
        {
            std::unique_lock<std::mutex> lock(queue_mutexes[graphIdentifier]);
            cond_vars[graphIdentifier].wait(lock, [&]{
                return !queues[graphIdentifier].empty() || terminateThreads;
            });
            if (terminateThreads) break;
            std::swap(localBatch, queues[graphIdentifier]);
            // Queue is now empty (or smaller) after swap; wake blocked producers.
            producer_cond_vars[graphIdentifier].notify_all();
        }
        while (!localBatch.empty()) {
            // Exit the batch early on shutdown — the temporal store (updated on
            // consumer threads) is already complete; incremental local store
            // edges in this tail are acceptable losses vs. a multi-minute drain.
            if (terminateThreads) break;
            localStore->addEdgeFromJson(localBatch.front());
            localBatch.pop();
            ++edgesProcessed;
            if (edgesProcessed % 50000 == 0 || (edgesProcessed <= 1000 && edgesProcessed % 100 == 0)) {
                instance_stream_logger.info("[PROC THREAD " + graphIdentifier + "] "
                                            + std::to_string(edgesProcessed) + " edges written to store");
            }
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
    auto existing = graphDBMapStreamingStores.find(graphIdentifier);
    if (existing != graphDBMapStreamingStores.end()) {
        return existing->second;
    }

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
        queues[graphIdent] = std::queue<nlohmann::json>();
        cond_vars[graphIdent];
        producer_cond_vars[graphIdent];
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
            ownedStoreKeys_.insert(graphIdentifier);
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
            ownedStoreKeys_.insert(graphIdentifier);
        }
        localStore = incrementalLocalStoreMap[graphIdentifier];
        partMutex = &queue_mutexes[graphIdentifier];
    }
    std::unique_lock<std::mutex> lock(*partMutex);
    localStore->addCentralEdge(edge);
}
