/**
Copyright 2023 JasmineGraph Team
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**/

#include "StreamHandler.h"

#include <chrono>
#include <nlohmann/json.hpp>
#include <string>
#include <stdlib.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <sys/stat.h>

#include "../logger/Logger.h"
#include "../Utils.h"
#include "../../server/JasmineGraphServer.h"
#include "../../server/JasmineGraphInstanceProtocol.h"
#include "../../temporalstore/TemporalStorePersistence.h"
#include "../telemetry/OpenTelemetryUtil.h"
#include "WorkerKafkaConsumer.h"

using json = nlohmann::json;
using namespace std;
using namespace std::chrono;

namespace {
Logger& streamHandlerLogger() {
    static Logger logger;
    return logger;
}
}  // namespace


/// Queries the Kafka broker for the number of partitions in `topic`.
/// Returns the partition count on success, or -1 if the query fails.
static int getKafkaTopicPartitionCount(cppkafka::Consumer& consumer, const std::string& topic) {
    try {
        auto metadata = consumer.get_metadata(true);
        for (const auto& topicMeta : metadata.get_topics()) {
            if (topicMeta.get_name() == topic) {
                return static_cast<int>(topicMeta.get_partitions().size());
            }
        }
        streamHandlerLogger().warn("[KAFKA META] Topic '" + topic + "' not found in broker metadata");
    } catch (const std::exception& e) {
        streamHandlerLogger().warn("[KAFKA META] Failed to query partition count for topic '" +
                                   topic + "': " + e.what());
    }
    return -1;
}

StreamHandler::StreamHandler(KafkaConnector *kstream, int numberOfPartitions,
                             vector<DataPublisher *> &workerClients, SQLiteDBInterface* sqlite,
                             int graphId, bool isDirected, spt::Algorithms algorithms,
                             bool isNewGraph)
        : kstream(kstream),
          graphId(graphId),
          workerClients(workerClients),
          graphPartitioner(numberOfPartitions, graphId, algorithms, sqlite, isDirected),
          stream_topic_name("stream_topic_name"),
          currentSnapshot(0),
          centralTemporalStore(nullptr),
          numberOfPartitions(numberOfPartitions),
          stopPublishing(false),
          snapshotsFinalized(false),
          globalSnapshotId(0) {
    
    // Initialize batch buffers for each worker
    workerBatches.resize(workerClients.size());
    for (size_t i = 0; i < workerClients.size(); i++) {
        workerBatchMutexes.push_back(std::make_unique<std::mutex>());
    }
    
    // Start async publish thread pool
    startPublishThreads();
    
    streamHandlerLogger().info("Initialized StreamHandler with " + std::to_string(workerClients.size()) + 
                              " workers, batch size " + std::to_string(BATCH_SIZE) + 
                              ", " + std::to_string(PUBLISH_THREADS) + " publish threads");
    
    // When starting a fresh (non-existing) graph, remove any stale snapshot files
    // from a previous run with the same graph ID so workers start from a clean state.
    if (isNewGraph) {
        std::string snapshotDir = Utils::getJasmineGraphHome() + "/env/data/temporal_snapshots";
        streamHandlerLogger().info("[NEW GRAPH] Deleting stale snapshot files for graph " +
                                   std::to_string(graphId) + " in " + snapshotDir);
        // Delete all files matching graph<N>_part*_snap*.tgs
        std::string pattern = snapshotDir + "/graph" + std::to_string(graphId) + "_part";
        Utils::deleteAllMatchingFiles(pattern);
    }

    std::string temporalEnabled = Utils::getJasmineGraphProperty("org.jasminegraph.temporal.enabled");
    if (temporalEnabled == "true") {
        uint64_t timeThreshold = 60;
        uint64_t edgeThreshold = 10000;
        
        std::string timeStr = Utils::getJasmineGraphProperty("org.jasminegraph.temporal.snapshot.time.seconds");
        if (!timeStr.empty()) {
            timeThreshold = std::stoull(timeStr);
        }
        
        std::string edgeStr = Utils::getJasmineGraphProperty("org.jasminegraph.temporal.snapshot.edge.count");
        if (!edgeStr.empty()) {
            edgeThreshold = std::stoull(edgeStr);
        }
        
        for (int partitionId = 0; partitionId < numberOfPartitions; partitionId++) {
            localTemporalStores[partitionId] = std::make_unique<TemporalStore>(
                graphId, partitionId, timeThreshold, edgeThreshold, 
                SnapshotManager::SnapshotMode::HYBRID
            );
        }
        
        centralTemporalStore = std::make_unique<TemporalStore>(
            graphId, numberOfPartitions, timeThreshold, edgeThreshold,
            SnapshotManager::SnapshotMode::HYBRID
        );
        
        // Restore snapshot state from disk if snapshots exist
        std::string snapshotDir = Utils::getJasmineGraphHome() + "/env/data/temporal_snapshots";
        
        // Check if directory exists
        struct stat st;
        if (stat(snapshotDir.c_str(), &st) == 0 && S_ISDIR(st.st_mode)) {
            // Track the highest snapshot ID found across ALL partitions (for global synchronization)
            uint32_t maxGlobalSnapshotId = 0;
            bool foundAnySnapshot = false;
            
            // Restore local partition stores
            for (int partitionId = 0; partitionId < numberOfPartitions; partitionId++) {
                std::string metaPath = TemporalStorePersistence::generateMetaFilePath(
                    snapshotDir, graphId, partitionId);
                uint32_t maxSnapshotId = TemporalStorePersistence::readLatestSnapshotId(metaPath);

                if (maxSnapshotId != UINT32_MAX) {
                    foundAnySnapshot = true;
                    if (maxSnapshotId > maxGlobalSnapshotId) {
                        maxGlobalSnapshotId = maxSnapshotId;
                    }

                    std::string bitmapPath = TemporalStorePersistence::generateBitmapFilePath(
                        snapshotDir, graphId, partitionId);

                    if (localTemporalStores[partitionId]->loadBitmapIndexFromDisk(bitmapPath)) {
                        // loadBitmapIndexFromDisk restores snapshotId; open next snapshot
                        uint32_t newSnapshotId = localTemporalStores[partitionId]->openNewSnapshot();

                        streamHandlerLogger().info("Restored temporal state for graph " + std::to_string(graphId) +
                                                  " partition " + std::to_string(partitionId) +
                                                  " from snapshot " + std::to_string(maxSnapshotId) +
                                                  ", continuing with snapshot " + std::to_string(newSnapshotId));
                    } else {
                        streamHandlerLogger().error("Failed to load bitmap index for graph " + std::to_string(graphId) +
                                                   " partition " + std::to_string(partitionId));
                    }
                }
            }
            
            // Restore central store (partitionId = numberOfPartitions)
            {
                std::string centralMeta = TemporalStorePersistence::generateMetaFilePath(
                    snapshotDir, graphId, numberOfPartitions);
                uint32_t maxCentralSnapshotId = TemporalStorePersistence::readLatestSnapshotId(centralMeta);

                if (maxCentralSnapshotId != UINT32_MAX) {
                    foundAnySnapshot = true;
                    if (maxCentralSnapshotId > maxGlobalSnapshotId) {
                        maxGlobalSnapshotId = maxCentralSnapshotId;
                    }

                    std::string centralBitmapPath = TemporalStorePersistence::generateBitmapFilePath(
                        snapshotDir, graphId, numberOfPartitions);

                    if (centralTemporalStore->loadBitmapIndexFromDisk(centralBitmapPath)) {
                        uint32_t newCentralSnapshotId = centralTemporalStore->openNewSnapshot();

                        streamHandlerLogger().info("Restored central temporal state for graph " + std::to_string(graphId) +
                                                  " from snapshot " + std::to_string(maxCentralSnapshotId) +
                                                  ", continuing with snapshot " + std::to_string(newCentralSnapshotId));
                    } else {
                        streamHandlerLogger().error("Failed to load central bitmap index for graph " + std::to_string(graphId));
                    }
                }
            }
            
            // Set global snapshot ID to continue from the highest found (synchronized across all partitions)
            if (foundAnySnapshot) {
                globalSnapshotId = maxGlobalSnapshotId + 1;
                streamHandlerLogger().info("Global snapshot ID restored to " + std::to_string(globalSnapshotId) + 
                                          " (continuing from highest snapshot " + std::to_string(maxGlobalSnapshotId) + ")");
            }
        }
        
        streamHandlerLogger().info("Temporal storage enabled for graph " + std::to_string(graphId) + 
                                  " with " + std::to_string(numberOfPartitions) + " partitions");
    }
}

// Destructor: Clean up temporal stores to prevent memory leaks
StreamHandler::~StreamHandler() noexcept {
    try {
        // Stop publish threads
        stopPublishThreads();

        // Flush all remaining batches
        for (size_t i = 0; i < workerBatches.size(); i++) {
            flushWorkerBatch(i, true);
        }

        // Save final snapshots if not already done
        finalizeAllSnapshots();
    } catch (const std::exception& e) {
        streamHandlerLogger().error(std::string("Exception in StreamHandler destructor: ") + e.what());
    } catch (...) {
        streamHandlerLogger().error("Unknown exception in StreamHandler destructor");
    }
}

// Finalize all snapshots: Save all open snapshots even if below threshold
// This prevents data loss when consumer stops mid-stream
void StreamHandler::finalizeAllSnapshots() {
    // Check if already finalized (prevent double-save)
    bool expected = false;
    if (!snapshotsFinalized.compare_exchange_strong(expected, true)) {
        // Already finalized by another thread or normal completion
        return;
    }
    
    if (localTemporalStores.empty() && centralTemporalStore == nullptr) {
        return;  // No temporal stores to save
    }
    
    streamHandlerLogger().info("Finalizing all temporal snapshots (saving open snapshots)");
    
    std::string snapshotDir = Utils::getJasmineGraphHome() + "/env/data/temporal_snapshots";
    Utils::createDirectory(snapshotDir);
    
    // Save all local partition snapshots (even if below threshold)
    for (auto& [partitionId, store] : localTemporalStores) {
        if (store != nullptr) {
            try {
                // Use globalSnapshotId — the same counter passed to addEdge() — so the
                // snapshot meta records match the bitmap bit positions exactly.
                bool saved = store->saveBitmapIndexToDisk(snapshotDir, globalSnapshotId);
                store->appendSnapshotMetaToDisk(snapshotDir, globalSnapshotId);
                if (saved) {
                    streamHandlerLogger().info("Saved final bitmap index for partition " +
                                              std::to_string(partitionId) + " snapId=" + std::to_string(globalSnapshotId));
                } else {
                    streamHandlerLogger().error("Failed to save final bitmap index for partition " +
                                               std::to_string(partitionId));
                }
            } catch (const std::exception& e) {
                streamHandlerLogger().error("Exception saving partition " +
                                           std::to_string(partitionId) + ": " + e.what());
            }
        }
    }
    
    // Save central store snapshot
    if (centralTemporalStore != nullptr) {
        try {
            bool saved = centralTemporalStore->saveBitmapIndexToDisk(snapshotDir, globalSnapshotId);
            centralTemporalStore->appendSnapshotMetaToDisk(snapshotDir, globalSnapshotId);
            if (saved) {
                streamHandlerLogger().info("Saved final central bitmap index snapId=" + std::to_string(globalSnapshotId));
            } else {
                streamHandlerLogger().error("Failed to save final central bitmap index");
            }
        } catch (const std::exception& e) {
            streamHandlerLogger().error("Exception saving central bitmap index: " + std::string(e.what()));
        }
    }
}

// Create global snapshot: Save ALL partitions with the SAME snapshot ID
// This ensures snapshot IDs represent consistent graph states across all partitions
void StreamHandler::createGlobalSnapshot() {
    std::string snapshotDir = Utils::getJasmineGraphHome() + "/env/data/temporal_snapshots";
    Utils::createDirectory(snapshotDir);
    
    streamHandlerLogger().info("Creating GLOBAL snapshot " + std::to_string(globalSnapshotId) + 
                              " for ALL partitions");
    
    // Save ALL local partition stores with the SAME snapshot ID
    int partitionsSaved = 0;
    for (auto& [partitionId, store] : localTemporalStores) {
        if (store != nullptr) {
            try {
                bool saved = store->saveBitmapIndexToDisk(snapshotDir, globalSnapshotId);
                store->appendSnapshotMetaToDisk(snapshotDir, globalSnapshotId);
                if (saved) {
                    streamHandlerLogger().info("Saved global snapshot " + std::to_string(globalSnapshotId) +
                                              " for partition " + std::to_string(partitionId));
                    partitionsSaved++;
                } else {
                    streamHandlerLogger().error("Failed to save global snapshot " +
                                               std::to_string(globalSnapshotId) +
                                               " for partition " + std::to_string(partitionId));
                }
            } catch (const std::exception& e) {
                streamHandlerLogger().error("Exception saving partition " + std::to_string(partitionId) +
                                           " snapshot " + std::to_string(globalSnapshotId) + ": " + e.what());
            }
        }
    }
    
    // Save central store with the same snapshot ID
    if (centralTemporalStore != nullptr) {
        try {
            bool saved = centralTemporalStore->saveBitmapIndexToDisk(snapshotDir, globalSnapshotId);
            centralTemporalStore->appendSnapshotMetaToDisk(snapshotDir, globalSnapshotId);
            if (saved) {
                streamHandlerLogger().info("Saved global snapshot " + std::to_string(globalSnapshotId) +
                                          " for central store");
            } else {
                streamHandlerLogger().error("Failed to save central store global snapshot " +
                                           std::to_string(globalSnapshotId));
            }
        } catch (const std::exception& e) {
            streamHandlerLogger().error("Exception saving central store bitmap index " +
                                       std::to_string(globalSnapshotId) + ": " + e.what());
        }
    }
    
    // Open new snapshots for ALL stores (synchronized)
    for (auto& [partitionId, store] : localTemporalStores) {
        if (store != nullptr) {
            store->openNewSnapshot();
        }
    }
    if (centralTemporalStore != nullptr) {
        centralTemporalStore->openNewSnapshot();
    }
    
    // Increment global snapshot counter
    globalSnapshotId++;
    
    streamHandlerLogger().info("Global snapshot created across " + std::to_string(partitionsSaved) + 
                              " partitions. Next snapshot ID: " + std::to_string(globalSnapshotId));
}


// Polls kafka for a message.
cppkafka::Message StreamHandler::pollMessage() { return kstream->consumer.poll(std::chrono::milliseconds(1000)); }

// Poll batch of messages at once for much better throughput
std::vector<cppkafka::Message> StreamHandler::pollMessageBatch(size_t maxMessages) {
    return kstream->consumer.poll_batch(maxMessages, std::chrono::milliseconds(1000));
}

// Checks if there's an error in Kafka's message.
bool StreamHandler::isErrorInMessage(const cppkafka::Message &msg) {
    if (!msg) {
        // Empty message (timeout) - not an error, just no data available yet
        return true;
    }
    if (msg.get_error()) {
        auto errorCode = msg.get_error().get_error();
        if (errorCode == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
            // Reached end of partition - this is expected, not an error
            return true;
        } else if (errorCode == RD_KAFKA_RESP_ERR__TIMED_OUT) {
            // Poll timeout - normal, just no messages available
            return true;
        } else {
            streamHandlerLogger().error("Kafka message error: " + msg.get_error().to_string() + 
                                       " (code: " + std::to_string(errorCode) + ")");
        }
        return true;
    }
    return false;
}

// Ends the stream if the end message("-1") has been received.
bool StreamHandler::isEndOfStream(const cppkafka::Message &msg) {
    if (!msg || !msg.get_payload()) {
        return false;  // Can't be end of stream if no message
    }
    std::string data(msg.get_payload());
    if (data == "-1") {
        frontend_logger.info("Received the end of `" + stream_topic_name + "` input kafka stream");
        return true;
    }
    return false;
}

// ============================================================================
// OPTIMIZATION METHODS: Batch Publishing + Async Thread Pool + Partition Cache
// ============================================================================

// Start async publish thread pool
void StreamHandler::startPublishThreads() {
    for (int i = 0; i < PUBLISH_THREADS; i++) {
        publishThreads.emplace_back(&StreamHandler::publishWorker, this);
    }
}

// Stop async publish thread pool
void StreamHandler::stopPublishThreads() {
    stopPublishing = true;
    queueCV.notify_all();
    for (auto& thread : publishThreads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

// Worker thread for async publishing
void StreamHandler::publishWorker() {
    while (!stopPublishing) {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(queueMutex);
            queueCV.wait(lock, [this]{ return !publishQueue.empty() || stopPublishing; });
            if (stopPublishing && publishQueue.empty()) return;
            if (!publishQueue.empty()) {
                task = std::move(publishQueue.front());
                publishQueue.pop();
            }
        }
        if (task) {
            task();
        }
    }
}

// Enqueue publish task to async thread pool
void StreamHandler::enqueuePublish(std::function<void()> task) {
    std::unique_lock<std::mutex> lock(queueMutex);
    publishQueue.push(std::move(task));
    queueCV.notify_one();
}

// Flush worker batch - send accumulated edges to worker
void StreamHandler::flushWorkerBatch(int workerId, bool force) {
    std::unique_lock<std::mutex> lock(*workerBatchMutexes[workerId]);
    
    if (workerBatches[workerId].empty()) return;
    if (!force && workerBatches[workerId].size() < BATCH_SIZE) return;
    
    // Send all edges individually but asynchronously
    std::vector<std::string> edges = std::move(workerBatches[workerId]);
    workerBatches[workerId].clear();
    lock.unlock();
    
    // Enqueue ONE task that publishes the entire batch — avoids 1000 heap allocs
    // and 1000 string copies per flush that the per-edge lambda approach incurred.
    enqueuePublish([this, workerId, edges = std::move(edges)]() mutable {
        for (const auto& edgeData : edges) {
            try {
                workerClients[workerId]->publish(edgeData);
            } catch (const std::exception& e) {
                streamHandlerLogger().error("Failed to publish edge to worker " +
                                           std::to_string(workerId) + ": " + e.what());
            }
        }
    });
}

// Get cached partition for a node (with cache hit tracking)
long StreamHandler::getCachedPartition(const std::string& nodeId, bool* cacheHit) {
    std::lock_guard<std::mutex> lock(cacheMutex);
    auto it = partitionCache.find(nodeId);
    if (it != partitionCache.end()) {
        *cacheHit = true;
        return it->second;
    }
    *cacheHit = false;
    return -1;
}

// Cache partition for a node
void StreamHandler::cachePartition(const std::string& nodeId, long partition) {
    std::lock_guard<std::mutex> lock(cacheMutex);
    // Limit cache size to prevent unbounded memory growth
    if (partitionCache.size() < 100000) {  // Cache up to 100K nodes
        partitionCache[nodeId] = partition;
    }
}

// ============================================================================
// PARALLEL CONSUMER THREAD FUNCTION
// Each thread owns a subset of Kafka topic partitions (Kafka assigns them
// automatically when multiple consumers join the same consumer group).
// Shared mutable state is protected by partitionerMutex_, temporalMutex_,
// and snapshotMutex_.  Per-worker batch vectors are already protected by
// workerBatchMutexes.
// ============================================================================
void StreamHandler::consumerThreadFunc(int threadId,
                                       cppkafka::Consumer* consumer,
                                       const std::string& topic,
                                       int n_workers,
                                       std::atomic<uint64_t>& totalMessages,
                                       std::atomic<uint64_t>& totalLocal,
                                       std::atomic<uint64_t>& totalCentral,
                                       std::atomic<bool>& endSignalReceived) {
    consumer->subscribe({topic});
    streamHandlerLogger().info("Consumer thread " + std::to_string(threadId) +
                               " subscribed to topic " + topic);

    const size_t KAFKA_BATCH_SIZE = 2000;
    const uint64_t MAX_EMPTY_POLLS = 60;
    uint64_t emptyPolls = 0;

    // Per-thread timing accumulators
    auto total_parse_time      = std::chrono::microseconds(0);
    auto total_partition_time  = std::chrono::microseconds(0);
    auto total_temporal_time   = std::chrono::microseconds(0);
    auto total_publish_time    = std::chrono::microseconds(0);
    uint64_t threadMessages    = 0;

    while (!endSignalReceived) {
        auto messageBatch = consumer->poll_batch(KAFKA_BATCH_SIZE, std::chrono::milliseconds(1000));

        if (messageBatch.empty()) {
            emptyPolls++;
            uint64_t processed = totalMessages.load();
            if (processed > 0 && emptyPolls >= MAX_EMPTY_POLLS) {
                streamHandlerLogger().warn("Thread " + std::to_string(threadId) +
                                           ": no messages for " + std::to_string(MAX_EMPTY_POLLS) +
                                           "s — exiting");
                break;
            }
            continue;
        }
        emptyPolls = 0;

        for (auto& msg : messageBatch) {
            if (endSignalReceived) break;

            if (isEndOfStream(msg)) {
                streamHandlerLogger().info("Thread " + std::to_string(threadId) +
                                           " received termination signal (-1)");
                endSignalReceived = true;
                break;
            }
            if (isErrorInMessage(msg)) continue;

            // ---- 1. JSON PARSE ----
            auto parse_start = std::chrono::high_resolution_clock::now();
            string data(msg.get_payload());
            auto edgeJson = json::parse(data);

            auto prop = edgeJson["properties"];
            prop["graphId"] = to_string(this->graphId);
            auto sourceJson = edgeJson["source"];
            auto destinationJson = edgeJson["destination"];
            string sId = std::string(sourceJson["id"]);
            string dId = std::string(destinationJson["id"]);
            total_parse_time += std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now() - parse_start);

            // ---- 2. PARTITION ---- 
            // HASH partitioning is lock-free: the destination partition is purely
            // deterministic (hash % n) and each Partition protects its own state
            // with an internal per-partition mutex.  FENNEL/LDG touch shared
            // totalVertices/totalEdges counters so still need the global lock.
            auto partition_start = std::chrono::high_resolution_clock::now();
            partitionedEdge partEdge;
            if (graphPartitioner.isHashAlgorithm()) {
                partEdge = graphPartitioner.addEdge({sId, dId});
            } else {
                std::lock_guard<std::mutex> lock(partitionerMutex_);
                partEdge = graphPartitioner.addEdge({sId, dId});
            }
            long part_s = partEdge[0].second;
            long part_d = partEdge[1].second;
            total_partition_time += std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now() - partition_start);

            sourceJson["pid"] = part_s;
            destinationJson["pid"] = part_d;
            json obj;
            obj["source"] = sourceJson;
            obj["destination"] = destinationJson;
            obj["properties"] = prop;

            long temp_s = part_s % n_workers;
            long temp_d = part_d % n_workers;

            // ---- 3. TEMPORAL STORE (mutex-protected) ----
            auto temporal_start = std::chrono::high_resolution_clock::now();
            bool shouldCreateGlobalSnapshot = false;

            if (!localTemporalStores.empty()) {
                try {
                    std::lock_guard<std::mutex> tlock(temporalMutex_);
                    if (part_s == part_d) {
                        if (localTemporalStores.find(part_s) == localTemporalStores.end()) {
                            streamHandlerLogger().error("Invalid partition " + std::to_string(part_s) +
                                                        " for edge " + sId + "-" + dId);
                            continue;
                        }
                        localTemporalStores[part_s]->addEdge(sId, dId, globalSnapshotId);
                        totalLocal++;
                        if (localTemporalStores[part_s]->shouldCreateSnapshot())
                            shouldCreateGlobalSnapshot = true;
                    } else {
                        if (centralTemporalStore == nullptr) {
                            streamHandlerLogger().error("Central store null for edge " + sId + "-" + dId);
                            continue;
                        }
                        centralTemporalStore->addEdge(sId, dId, globalSnapshotId);
                        totalCentral++;
                        if (centralTemporalStore->shouldCreateSnapshot())
                            shouldCreateGlobalSnapshot = true;
                    }
                } catch (const std::exception& e) {
                    streamHandlerLogger().error("Temporal error for edge " + sId + "-" + dId + ": " + e.what());
                }

                if (shouldCreateGlobalSnapshot) {
                    std::lock_guard<std::mutex> slock(snapshotMutex_);
                    // Re-check inside lock: another thread may have already snapshotted
                    if (localTemporalStores.begin()->second->shouldCreateSnapshot() ||
                        (centralTemporalStore && centralTemporalStore->shouldCreateSnapshot())) {
                        createGlobalSnapshot();
                    }
                }
            }
            total_temporal_time += std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now() - temporal_start);

            // ---- 4. BATCH PUBLISH (workerBatchMutexes already per-worker) ----
            auto publish_start = std::chrono::high_resolution_clock::now();
            if (part_s == part_d) {
                // Local edge — only one dump() needed
                obj["EdgeType"] = "Local";
                obj["PID"] = part_s;
                std::string localEdgeData = obj.dump();

                {
                    std::unique_lock<std::mutex> lock(*workerBatchMutexes[temp_s]);
                    workerBatches[temp_s].push_back(localEdgeData);
                    size_t batchSize = workerBatches[temp_s].size();
                    lock.unlock();
                    if (batchSize >= BATCH_SIZE) flushWorkerBatch(temp_s, false);
                }
            } else {
                // Central edge — serialize once, patch PID in the string for dst worker
                obj["EdgeType"] = "Central";
                obj["PID"] = part_s;
                std::string centralEdgeData_s = obj.dump();

                // Build dst variant by replacing "PID":part_s with "PID":part_d
                // (nlohmann dumps without spaces, e.g. "PID":3)
                std::string centralEdgeData_d = centralEdgeData_s;
                {
                    std::string pidOld = "\"PID\":" + std::to_string(part_s);
                    std::string pidNew = "\"PID\":" + std::to_string(part_d);
                    auto pos = centralEdgeData_d.find(pidOld);
                    if (pos != std::string::npos)
                        centralEdgeData_d.replace(pos, pidOld.size(), pidNew);
                }

                {
                    std::unique_lock<std::mutex> lock(*workerBatchMutexes[temp_s]);
                    workerBatches[temp_s].push_back(centralEdgeData_s);
                    size_t batchSize = workerBatches[temp_s].size();
                    lock.unlock();
                    if (batchSize >= BATCH_SIZE) flushWorkerBatch(temp_s, false);
                }
                {
                    std::unique_lock<std::mutex> lock(*workerBatchMutexes[temp_d]);
                    workerBatches[temp_d].push_back(centralEdgeData_d);
                    size_t batchSize = workerBatches[temp_d].size();
                    lock.unlock();
                    if (batchSize >= BATCH_SIZE) flushWorkerBatch(temp_d, false);
                }
            }
            total_publish_time += std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now() - publish_start);

            threadMessages++;
            totalMessages++;

            // Periodic partial flush to ensure timely delivery
            if (threadMessages % 1000 == 0) {
                for (size_t i = 0; i < workerBatches.size(); i++) flushWorkerBatch(i, false);
            }

            // Per-thread performance log every 10 000 messages
            if (threadMessages % 10000 == 0 && threadMessages > 0) {
                auto avg_parse     = total_parse_time.count()     / threadMessages;
                auto avg_partition = total_partition_time.count() / threadMessages;
                auto avg_temporal  = total_temporal_time.count()  / threadMessages;
                auto avg_publish   = total_publish_time.count()   / threadMessages;
                auto avg_total     = avg_parse + avg_partition + avg_temporal + avg_publish;
                if (avg_total > 0) {
                    streamHandlerLogger().info(
                        "[T" + std::to_string(threadId) + "] Performance @ " +
                        std::to_string(threadMessages) + " msgs: " +
                        "parse=" + std::to_string(avg_parse) + "us (" +
                        std::to_string(100 * avg_parse / avg_total) + "%), " +
                        "partition=" + std::to_string(avg_partition) + "us (" +
                        std::to_string(100 * avg_partition / avg_total) + "%), " +
                        "temporal=" + std::to_string(avg_temporal) + "us (" +
                        std::to_string(100 * avg_temporal / avg_total) + "%), " +
                        "publish=" + std::to_string(avg_publish) + "us (" +
                        std::to_string(100 * avg_publish / avg_total) + "%)");
                }
            }
        }  // end message batch loop
    }  // end while

    streamHandlerLogger().info("Consumer thread " + std::to_string(threadId) +
                               " finished. Processed " + std::to_string(threadMessages) + " messages.");
}

// ============================================================================
// COORDINATOR: spawns N parallel consumer threads then handles teardown
// ============================================================================
void StreamHandler::listen_to_kafka_topic() {
    OTEL_TRACE_FUNCTION();
    OpenTelemetryUtil::addSpanAttribute("graph.id", std::to_string(graphId));
    OpenTelemetryUtil::addSpanAttribute("partitions", std::to_string(numberOfPartitions));
    OpenTelemetryUtil::addSpanAttribute("temporal.enabled", !localTemporalStores.empty() ? "true" : "false");

    // Assign partitions to workers
    JasmineGraphServer *server = JasmineGraphServer::getInstance();
    std::vector<JasmineGraphServer::worker> workers = server->workers(workerClients.size());
    for (int i = 0; i < (int)workerClients.size(); i++) {
        Utils::assignPartitionToWorker(graphId, i, workers.at(i).hostname, workers.at(i).port);
    }

    // Read n_workers ONCE (fix: was read inside per-message loop previously)
    int n_workers = atoi(Utils::getJasmineGraphProperty("org.jasminegraph.server.nworkers").c_str());

    // Get the topic that the original consumer already subscribed to
    std::vector<std::string> subscription = kstream->consumer.get_subscription();
    if (subscription.empty()) {
        streamHandlerLogger().error("KafkaConnector has no topic subscription — cannot start consumers");
        return;
    }
    const std::string topic = subscription[0];

    // Adaptive: use actual Kafka topic partition count as the thread count so
    // each thread can own exactly one Kafka partition.
    int numConsumerThreads = DEFAULT_CONSUMER_THREADS;
    int detectedPartitions = getKafkaTopicPartitionCount(kstream->consumer, topic);
    if (detectedPartitions > 0) {
        numConsumerThreads = detectedPartitions;
        streamHandlerLogger().info("[THREAD CFG] Auto-detected " + std::to_string(numConsumerThreads) +
                                   " Kafka partitions for topic '" + topic + "' — using as thread count");
    } else {
        streamHandlerLogger().warn("[THREAD CFG] Could not detect Kafka partition count for topic '" +
                                   topic + "' — using default: " + std::to_string(numConsumerThreads));
    }
    // Allow explicit override via property (useful for testing or tuning)
    std::string threadsProp = Utils::getJasmineGraphProperty("org.jasminegraph.kafka.consumer.threads");
    if (!threadsProp.empty()) {
        int val = std::atoi(threadsProp.c_str());
        if (val > 0) {
            numConsumerThreads = val;
            streamHandlerLogger().info("[THREAD CFG] Thread count overridden to " +
                                       std::to_string(numConsumerThreads) +
                                       " via org.jasminegraph.kafka.consumer.threads");
        }
    }

    streamHandlerLogger().info("Starting " + std::to_string(numConsumerThreads) +
                               " parallel Kafka consumer threads for topic '" + topic + "'");

    // For HASH partitioning: workers consume directly from Kafka (eliminates master relay bottleneck)
    if (graphPartitioner.isHashAlgorithm()) {
        streamHandlerLogger().info("Hash partitioning detected: dispatching direct Kafka consuming to workers");
        listenViaDirectWorkers(topic, workers);
        return;
    }

    // Shared state for all consumer threads
    std::atomic<uint64_t> totalMessages{0};
    std::atomic<uint64_t> totalLocal{0};
    std::atomic<uint64_t> totalCentral{0};
    std::atomic<bool>     endSignalReceived{false};

    // Spawn N consumer threads; each creates its own cppkafka::Consumer so that
    // Kafka can distribute topic partitions across the group automatically.
    // Use a lambda to avoid std::thread's rvalue-conversion issues with
    // reference parameters and move-only types (cppkafka::Consumer).
    std::vector<std::thread> consumerThreads;
    consumerThreads.reserve(numConsumerThreads);
    for (int i = 0; i < numConsumerThreads; i++) {
        auto consumerPtr = std::make_unique<cppkafka::Consumer>(kstream->getConfig());
        consumerThreads.emplace_back(
            [this, threadId = i, c = std::move(consumerPtr), topic, n_workers,
             &totalMessages, &totalLocal, &totalCentral, &endSignalReceived]() mutable {
                this->consumerThreadFunc(threadId, c.get(), topic, n_workers,
                                         totalMessages, totalLocal, totalCentral,
                                         endSignalReceived);
            }
        );
    }

    // Wait for all consumer threads to finish
    for (auto& t : consumerThreads) {
        if (t.joinable()) t.join();
    }

    streamHandlerLogger().info("All consumer threads finished. Total messages: " +
                               std::to_string(totalMessages.load()) +
                               " (local=" + std::to_string(totalLocal.load()) +
                               ", central=" + std::to_string(totalCentral.load()) + ")");

    // Flush all remaining worker batches
    streamHandlerLogger().info("Flushing remaining worker batches...");
    for (size_t i = 0; i < workerBatches.size(); i++) flushWorkerBatch(i, true);

    // Wait for async publish queue to drain (up to 5 s)
    int waitCount = 0;
    while (waitCount < 50) {
        std::unique_lock<std::mutex> lock(queueMutex);
        if (publishQueue.empty()) break;
        lock.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        waitCount++;
    }

    // Signal workers that the stream is done
    for (auto& workerClient : workerClients) {
        if (workerClient != nullptr) workerClient->publish("-1");
    }
    streamHandlerLogger().info("Termination signal sent to all workers");

    graphPartitioner.updateMetaDB();
    graphPartitioner.printStats();
    finalizeAllSnapshots();

    // Unsubscribe the original (unused) consumer cleanly
    kstream->Unsubscribe();

}

void StreamHandler::listenViaDirectWorkers(
        const std::string& topic,
        const std::vector<JasmineGraphServer::worker>& workers) {

    const int n_workers = static_cast<int>(workers.size());

    // Kafka broker address
    std::string brokers = Utils::getJasmineGraphProperty("org.jasminegraph.server.streaming.kafka.host");

    // Master IP for worker handshake verification
    std::string masterIP = Utils::getJasmineGraphProperty("org.jasminegraph.server.host");

    // Each worker is assigned its own independent Kafka consumer group, so every
    // worker reads ALL Kafka topic partitions (fan-out model).  The thread count
    // must therefore equal the total number of Kafka topic partitions — NOT the
    // number of graph partitions the worker owns.  With 8 Kafka partitions and
    // 2 workers: each worker needs 8 threads so Kafka assigns 1 partition per thread.
    // Using owned-graph-partition count (4) would give each thread 2 Kafka partitions
    // and halve throughput, which is the regression that was observed.
    int kafkaTopicPartitions = getKafkaTopicPartitionCount(kstream->consumer, topic);
    int defaultThreadCount = (kafkaTopicPartitions > 0) ? kafkaTopicPartitions : DEFAULT_CONSUMER_THREADS;
    if (kafkaTopicPartitions > 0) {
        streamHandlerLogger().info("[THREAD CFG] Detected " + std::to_string(kafkaTopicPartitions) +
                                   " Kafka partitions for topic '" + topic +
                                   "' — each worker will use " + std::to_string(defaultThreadCount) +
                                   " consumer threads (1 per Kafka partition)");
    } else {
        streamHandlerLogger().warn("[THREAD CFG] Could not detect Kafka partition count for topic '" +
                                   topic + "' — falling back to default: " +
                                   std::to_string(defaultThreadCount));
    }

    // Property override applies uniformly across all workers if set.
    std::string threadsProp = Utils::getJasmineGraphProperty("org.jasminegraph.kafka.consumer.threads");
    int threadOverride = -1;
    if (!threadsProp.empty()) {
        int val = std::atoi(threadsProp.c_str());
        if (val > 0) {
            threadOverride = val;
            streamHandlerLogger().info("[THREAD CFG] Consumer thread count globally overridden to " +
                                       std::to_string(threadOverride) +
                                       " via org.jasminegraph.kafka.consumer.threads");
        }
    }

    // Temporal config
    bool temporalEnabled = !localTemporalStores.empty();
    uint64_t timeThreshold = 0, edgeThreshold = 0;
    uint32_t initialSnapshotId = globalSnapshotId;
    if (temporalEnabled) {
        std::string tProp = Utils::getJasmineGraphProperty(
            "org.jasminegraph.server.streaming.temporal.time.threshold");
        std::string eProp = Utils::getJasmineGraphProperty(
            "org.jasminegraph.server.streaming.temporal.edge.threshold");
        if (!tProp.empty()) timeThreshold = std::stoull(tProp);
        if (!eProp.empty()) edgeThreshold = std::stoull(eProp);
        streamHandlerLogger().info("[TEMPORAL CFG] listenViaDirectWorkers:"
            " temporalEnabled=true edgeThreshold=" + std::to_string(edgeThreshold) +
            " timeThreshold=" + std::to_string(timeThreshold) +
            " initialSnapshotId=" + std::to_string(initialSnapshotId) +
            " (rawEdgeProp='" + eProp + "' rawTimeProp='" + tProp + "')");
        if (edgeThreshold == 0 && timeThreshold == 0) {
            streamHandlerLogger().warn("[TEMPORAL CFG] BOTH thresholds are 0 — workers will never"
                " auto-snapshot. Check org.jasminegraph.server.streaming.temporal.edge.threshold"
                " and org.jasminegraph.server.streaming.temporal.time.threshold in properties.");
        }
    } else {
        streamHandlerLogger().warn("[TEMPORAL CFG] listenViaDirectWorkers: temporalEnabled=false"
            " (localTemporalStores empty — was org.jasminegraph.temporal.enabled=true?)");
    }

    // Unique timestamp suffix so each run initializes fresh consumer groups
    std::string tsuffix = std::to_string(
        std::chrono::system_clock::now().time_since_epoch().count());

    // Build owned-partition list per worker: worker i owns partitions p where (p % n_workers) == i
    std::vector<std::vector<int>> workerOwnedPartitions(n_workers);
    for (int p = 0; p < numberOfPartitions; p++) {
        workerOwnedPartitions[p % n_workers].push_back(p);
    }

    streamHandlerLogger().info("Dispatching direct Kafka stream to " +
                               std::to_string(n_workers) + " workers for topic '" + topic + "'");

    // ── Two-phase dispatch ────────────────────────────────────────────────
    //
    // Phase 1 (synchronous): connect to every worker, send Kafka config,
    //   confirm the command was accepted.
    //
    // Phase 2 (blocking): one thread per worker reads the
    //   WORKER_DIRECT_KAFKA_DONE reply when the worker finishes consuming.
    //   We join all threads before returning so "done" only reaches the user
    //   after all workers are truly done.

    struct DoneStats {
        std::atomic<uint64_t> totalMessages{0}, totalLocal{0}, totalCentral{0};
        std::atomic<int> successCount{0};
    };
    auto doneStats = std::make_shared<DoneStats>();
    int n_started = 0;
    std::vector<std::thread> doneThreads;

    for (int wi = 0; wi < n_workers; wi++) {
        const JasmineGraphServer::worker& w = workers[wi];

        // Build unique consumer group per worker (fan-out: each reads ALL messages)
        std::string groupId = "jasminegraph_direct_g" + std::to_string(graphId) +
                              "_w" + std::to_string(wi) + "_" + tsuffix;

        // Build JSON config for this worker
        nlohmann::json configJson;
        configJson["brokers"]            = brokers;
        configJson["topic"]              = topic;
        configJson["groupId"]            = groupId;
        configJson["graphId"]            = graphId;
        configJson["numberOfPartitions"] = numberOfPartitions;
        configJson["ownedPartitions"]    = workerOwnedPartitions[wi];
        configJson["temporalEnabled"]    = temporalEnabled;
        configJson["timeThreshold"]      = timeThreshold;
        configJson["edgeThreshold"]      = edgeThreshold;
        configJson["initialSnapshotId"]  = initialSnapshotId;
        // Thread count = total Kafka topic partitions (worker reads ALL of them in its own group).
        // Owned graph partitions are a separate concept — they control which EDGES are stored,
        // not how many Kafka partitions this worker's consumer group is assigned.
        int numConsumerThreads = (threadOverride > 0) ? threadOverride : defaultThreadCount;
        streamHandlerLogger().info("[THREAD CFG] Worker " + std::to_string(wi) + ": " +
                                   std::to_string(numConsumerThreads) + " consumer threads (" +
                                   std::to_string(kafkaTopicPartitions) + " Kafka partitions, " +
                                   std::to_string(workerOwnedPartitions[wi].size()) +
                                   " owned graph partitions)");
        configJson["numConsumerThreads"] = numConsumerThreads;
        configJson["workerIndex"]        = wi;
        std::string configStr = configJson.dump();

        // ── Phase 1: open TCP connection and deliver config ───────────────
        int sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd < 0) {
            streamHandlerLogger().error("Worker " + std::to_string(wi) + ": cannot create socket");
            continue;
        }

        std::string host = w.hostname;
        if (host.find('@') != std::string::npos) host = Utils::split(host, '@')[1];

        struct hostent hostEntry;
        struct hostent *server = nullptr;
        int hErrno = 0;
        char hostBuffer[8192];
        if (gethostbyname_r(host.c_str(), &hostEntry, hostBuffer, sizeof(hostBuffer), &server, &hErrno) != 0 ||
            !server) {
            streamHandlerLogger().error("Worker " + std::to_string(wi) + ": unknown host " + host);
            close(sockfd);
            continue;
        }

        struct sockaddr_in serv_addr;
        bzero((char*)&serv_addr, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        bcopy((char*)server->h_addr, (char*)&serv_addr.sin_addr.s_addr, server->h_length);
        serv_addr.sin_port = htons(w.port);

        if (Utils::connect_wrapper(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
            streamHandlerLogger().error("Worker " + std::to_string(wi) +
                                        ": connect failed to " + host + ":" + std::to_string(w.port));
            close(sockfd);
            continue;
        }

        char data[FED_DATA_LENGTH + 1];
        if (!Utils::performHandshake(sockfd, data, FED_DATA_LENGTH, masterIP)) {
            streamHandlerLogger().error("Worker " + std::to_string(wi) + ": handshake failed");
            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
            close(sockfd);
            continue;
        }

        if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH,
                    JasmineGraphInstanceProtocol::WORKER_DIRECT_KAFKA_STREAM,
                    JasmineGraphInstanceProtocol::WORKER_DIRECT_KAFKA_STREAM_ACK)) {
            streamHandlerLogger().error("Worker " + std::to_string(wi) +
                                        ": failed to get ACK for direct kafka stream command");
            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
            close(sockfd);
            continue;
        }

        streamHandlerLogger().info("[WORKER CFG] Sending to worker " + std::to_string(wi) +
                                   ": " + configStr);
        if (!Utils::send_str_wrapper(sockfd, configStr)) {
            streamHandlerLogger().error("Worker " + std::to_string(wi) + ": failed to send config JSON");
            close(sockfd);
            continue;
        }

        streamHandlerLogger().info("Worker " + std::to_string(wi) +
                                   " config delivered. Handing socket to background thread.");
        n_started++;

        // ── Phase 2: background thread waits for DONE from this worker ────
        //   We join these threads below so "done" returns only AFTER all
        //   workers have finished consuming (matches listen_to_kafka_topic
        //   behavior).
        doneThreads.emplace_back([doneStats, sockfd, wi]() {
            char longData[INSTANCE_LONG_DATA_LENGTH + 1];
            std::string doneMsg = Utils::read_str_trim_wrapper(sockfd, longData, INSTANCE_LONG_DATA_LENGTH);

            // Log raw bytes for diagnostics
            std::string rawHex;
            for (unsigned char c : doneMsg.substr(0, std::min(doneMsg.size(), (size_t)80)))
                rawHex += (c < 32 ? "[" + std::to_string((int)c) + "]" : std::string(1, c));
            streamHandlerLogger().info("[DONE MSG] Worker " + std::to_string(wi) +
                                       " rawBytes=" + rawHex +
                                       " len=" + std::to_string(doneMsg.size()));

            const std::string& donePrefix = JasmineGraphInstanceProtocol::WORKER_DIRECT_KAFKA_DONE;
            if (doneMsg.size() >= donePrefix.size() &&
                doneMsg.substr(0, donePrefix.size()) == donePrefix) {
                auto parts = Utils::split(doneMsg, '|');
                if (parts.size() >= 4) {
                    doneStats->totalMessages.fetch_add(std::stoull(parts[1]));
                    doneStats->totalLocal.fetch_add(std::stoull(parts[2]));
                    doneStats->totalCentral.fetch_add(std::stoull(parts[3]));
                } else {
                    streamHandlerLogger().warn("[DONE MSG] Worker " + std::to_string(wi) +
                                               " done msg had only " + std::to_string(parts.size()) +
                                               " parts: '" + doneMsg + "'");
                }
                doneStats->successCount.fetch_add(1);
                streamHandlerLogger().info("[DONE MSG] Worker " + std::to_string(wi) +
                                           " finished. Cumulative total=" +
                                           std::to_string(doneStats->totalMessages.load()));
            } else {
                streamHandlerLogger().error("[DONE MSG] Worker " + std::to_string(wi) +
                                            ": unexpected done message: " + doneMsg);
            }

            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
            close(sockfd);
        });
    }

    // Block until ALL workers finish consuming (idle-timeout or end-of-stream).
    // This matches the behavior of listen_to_kafka_topic() — "done" only
    // returns after consumption is complete.
    if (n_started > 0) {
        streamHandlerLogger().info("[READY] Config delivered to " + std::to_string(n_started) +
                                   "/" + std::to_string(n_workers) +
                                   " workers. Waiting for all workers to finish consuming...");
        for (auto& t : doneThreads) {
            if (t.joinable()) t.join();
        }
        streamHandlerLogger().info("[DONE] All workers finished. total=" +
                                   std::to_string(doneStats->totalMessages.load()) +
                                   " local=" + std::to_string(doneStats->totalLocal.load()) +
                                   " central=" + std::to_string(doneStats->totalCentral.load()) +
                                   " successCount=" + std::to_string(doneStats->successCount.load()) +
                                   "/" + std::to_string(n_started));
    } else {
        streamHandlerLogger().warn("[READY] No workers started successfully for topic '" + topic + "'.");
    }

    // Update partition metadata DB now (does not require streaming to be complete).
    graphPartitioner.updateMetaDB();
    graphPartitioner.printStats();
    kstream->Unsubscribe();
}
