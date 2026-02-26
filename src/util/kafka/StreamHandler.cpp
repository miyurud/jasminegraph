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
#include <sys/stat.h>

#include "../logger/Logger.h"
#include "../Utils.h"
#include "../../server/JasmineGraphServer.h"
#include "../../temporalstore/TemporalStorePersistence.h"
#include "../telemetry/OpenTelemetryUtil.h"

using json = nlohmann::json;
using namespace std;
using namespace std::chrono;
Logger stream_handler_logger;

StreamHandler::StreamHandler(KafkaConnector *kstream, int numberOfPartitions,
                             vector<DataPublisher *> &workerClients, SQLiteDBInterface* sqlite,
                             int graphId, bool isDirected, spt::Algorithms algorithms)
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
    
    stream_handler_logger.info("Initialized StreamHandler with " + std::to_string(workerClients.size()) + 
                              " workers, batch size " + std::to_string(BATCH_SIZE) + 
                              ", " + std::to_string(PUBLISH_THREADS) + " publish threads");
    
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
            localTemporalStores[partitionId] = new TemporalStore(
                graphId, partitionId, timeThreshold, edgeThreshold, 
                SnapshotManager::SnapshotMode::HYBRID
            );
        }
        
        centralTemporalStore = new TemporalStore(
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
                uint32_t maxSnapshotId = TemporalStorePersistence::findHighestSnapshotId(
                    snapshotDir, graphId, partitionId);
                
                // Continue from the NEXT snapshot after the highest one found
                // Note: Snapshot 0 is valid! We check for UINT32_MAX (not found) instead
                if (maxSnapshotId != UINT32_MAX) {
                    foundAnySnapshot = true;
                    if (maxSnapshotId > maxGlobalSnapshotId) {
                        maxGlobalSnapshotId = maxSnapshotId;
                    }
                    
                    // Load the snapshot data from disk
                    std::string snapshotFilePath = TemporalStorePersistence::generateFilePath(
                        snapshotDir, graphId, partitionId, maxSnapshotId);
                    
                    if (localTemporalStores[partitionId]->loadSnapshotFromDisk(snapshotFilePath)) {
                        // Set to the loaded snapshot ID first
                        localTemporalStores[partitionId]->getSnapshotManager()->setCurrentSnapshotId(maxSnapshotId);
                        
                        // Open new snapshot to mark all loaded edges as active in next snapshot (cumulative semantics)
                        uint32_t newSnapshotId = localTemporalStores[partitionId]->openNewSnapshot();
                        
                        stream_handler_logger.info("Restored temporal state for graph " + std::to_string(graphId) + 
                                                  " partition " + std::to_string(partitionId) + 
                                                  " from snapshot " + std::to_string(maxSnapshotId) +
                                                  ", continuing with snapshot " + std::to_string(newSnapshotId));
                    } else {
                        stream_handler_logger.error("Failed to load snapshot for graph " + std::to_string(graphId) + 
                                                   " partition " + std::to_string(partitionId) + 
                                                   " from snapshot " + std::to_string(maxSnapshotId));
                    }
                }
            }
            
            // Restore central store (partitionId = numberOfPartitions)
            uint32_t maxCentralSnapshotId = TemporalStorePersistence::findHighestSnapshotId(
                snapshotDir, graphId, numberOfPartitions);
            
            if (maxCentralSnapshotId != UINT32_MAX) {
                foundAnySnapshot = true;
                if (maxCentralSnapshotId > maxGlobalSnapshotId) {
                    maxGlobalSnapshotId = maxCentralSnapshotId;
                }
                
                // Load the central snapshot data from disk
                std::string centralSnapshotFilePath = TemporalStorePersistence::generateFilePath(
                    snapshotDir, graphId, numberOfPartitions, maxCentralSnapshotId);
                
                if (centralTemporalStore->loadSnapshotFromDisk(centralSnapshotFilePath)) {
                    // Set to the loaded snapshot ID first
                    centralTemporalStore->getSnapshotManager()->setCurrentSnapshotId(maxCentralSnapshotId);
                    
                    // Open new snapshot to mark all loaded edges as active in next snapshot (cumulative semantics)
                    uint32_t newCentralSnapshotId = centralTemporalStore->openNewSnapshot();
                    
                    stream_handler_logger.info("Restored central temporal state for graph " + std::to_string(graphId) + 
                                              " from snapshot " + std::to_string(maxCentralSnapshotId) +
                                              ", continuing with snapshot " + std::to_string(newCentralSnapshotId));
                } else {
                    stream_handler_logger.error("Failed to load central snapshot for graph " + std::to_string(graphId) + 
                                               " from snapshot " + std::to_string(maxCentralSnapshotId));
                }
            }
            
            // Set global snapshot ID to continue from the highest found (synchronized across all partitions)
            if (foundAnySnapshot) {
                globalSnapshotId = maxGlobalSnapshotId + 1;
                stream_handler_logger.info("Global snapshot ID restored to " + std::to_string(globalSnapshotId) + 
                                          " (continuing from highest snapshot " + std::to_string(maxGlobalSnapshotId) + ")");
            }
        }
        
        stream_handler_logger.info("Temporal storage enabled for graph " + std::to_string(graphId) + 
                                  " with " + std::to_string(numberOfPartitions) + " partitions");
    }
}

// Destructor: Clean up temporal stores to prevent memory leaks
StreamHandler::~StreamHandler() {
    // Stop publish threads
    stopPublishThreads();
    
    // Flush all remaining batches
    for (size_t i = 0; i < workerBatches.size(); i++) {
        flushWorkerBatch(i, true);
    }
    
    // Save final snapshots if not already done
    finalizeAllSnapshots();
    
    // Clean up local partition stores
    for (auto& [partitionId, store] : localTemporalStores) {
        if (store != nullptr) {
            delete store;
        }
    }
    localTemporalStores.clear();
    
    // Clean up central store
    if (centralTemporalStore != nullptr) {
        delete centralTemporalStore;
        centralTemporalStore = nullptr;
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
    
    stream_handler_logger.info("Finalizing all temporal snapshots (saving open snapshots)");
    
    std::string snapshotDir = Utils::getJasmineGraphHome() + "/env/data/temporal_snapshots";
    Utils::createDirectory(snapshotDir);
    
    // Save all local partition snapshots (even if below threshold)
    for (auto& [partitionId, store] : localTemporalStores) {
        if (store != nullptr) {
            try {
                if (store->saveSnapshotToDisk(snapshotDir, false)) {
                    stream_handler_logger.info("Saved final snapshot for partition " + 
                                              std::to_string(partitionId));
                } else {
                    stream_handler_logger.error("Failed to save final snapshot for partition " + 
                                               std::to_string(partitionId));
                }
            } catch (const std::exception& e) {
                stream_handler_logger.error("Exception saving partition " + 
                                           std::to_string(partitionId) + ": " + e.what());
            }
        }
    }
    
    // Save central store snapshot
    if (centralTemporalStore != nullptr) {
        try {
            if (centralTemporalStore->saveSnapshotToDisk(snapshotDir, false)) {
                stream_handler_logger.info("Saved final central snapshot");
            } else {
                stream_handler_logger.error("Failed to save final central snapshot");
            }
        } catch (const std::exception& e) {
            stream_handler_logger.error("Exception saving central snapshot: " + std::string(e.what()));
        }
    }
}

// Create global snapshot: Save ALL partitions with the SAME snapshot ID
// This ensures snapshot IDs represent consistent graph states across all partitions
void StreamHandler::createGlobalSnapshot() {
    std::string snapshotDir = Utils::getJasmineGraphHome() + "/env/data/temporal_snapshots";
    Utils::createDirectory(snapshotDir);
    
    stream_handler_logger.info("Creating GLOBAL snapshot " + std::to_string(globalSnapshotId) + 
                              " for ALL partitions");
    
    // Save ALL local partition stores with the SAME snapshot ID
    int partitionsSaved = 0;
    for (auto& [partitionId, store] : localTemporalStores) {
        if (store != nullptr) {
            try {
                if (store->saveSnapshotToDisk(snapshotDir, false)) {
                    stream_handler_logger.info("Saved global snapshot " + std::to_string(globalSnapshotId) + 
                                              " for partition " + std::to_string(partitionId));
                    partitionsSaved++;
                } else {
                    stream_handler_logger.error("Failed to save global snapshot " + 
                                               std::to_string(globalSnapshotId) + 
                                               " for partition " + std::to_string(partitionId));
                }
            } catch (const std::exception& e) {
                stream_handler_logger.error("Exception saving partition " + std::to_string(partitionId) + 
                                           " snapshot " + std::to_string(globalSnapshotId) + ": " + e.what());
            }
        }
    }
    
    // Save central store with the same snapshot ID
    if (centralTemporalStore != nullptr) {
        try {
            if (centralTemporalStore->saveSnapshotToDisk(snapshotDir, false)) {
                stream_handler_logger.info("Saved global snapshot " + std::to_string(globalSnapshotId) + 
                                          " for central store");
            } else {
                stream_handler_logger.error("Failed to save central store global snapshot " + 
                                           std::to_string(globalSnapshotId));
            }
        } catch (const std::exception& e) {
            stream_handler_logger.error("Exception saving central store snapshot " + 
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
    
    stream_handler_logger.info("Global snapshot created across " + std::to_string(partitionsSaved) + 
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
            stream_handler_logger.error("Kafka message error: " + msg.get_error().to_string() + 
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
    
    // Enqueue each edge for async publishing
    for (const auto& edgeData : edges) {
        enqueuePublish([this, workerId, edgeData]() {
            try {
                workerClients[workerId]->publish(edgeData);
            } catch (const std::exception& e) {
                stream_handler_logger.error("Failed to publish edge to worker " + 
                                           std::to_string(workerId) + ": " + e.what());
            }
        });
    }
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
    stream_handler_logger.info("Consumer thread " + std::to_string(threadId) +
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
                stream_handler_logger.warn("Thread " + std::to_string(threadId) +
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
                stream_handler_logger.info("Thread " + std::to_string(threadId) +
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

            // ---- 2. PARTITION (mutex-protected) ----
            auto partition_start = std::chrono::high_resolution_clock::now();
            partitionedEdge partEdge;
            {
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
                            stream_handler_logger.error("Invalid partition " + std::to_string(part_s) +
                                                        " for edge " + sId + "-" + dId);
                            continue;
                        }
                        localTemporalStores[part_s]->addEdge(sId, dId, globalSnapshotId);
                        totalLocal++;
                        if (localTemporalStores[part_s]->shouldCreateSnapshot())
                            shouldCreateGlobalSnapshot = true;
                    } else {
                        if (centralTemporalStore == nullptr) {
                            stream_handler_logger.error("Central store null for edge " + sId + "-" + dId);
                            continue;
                        }
                        centralTemporalStore->addEdge(sId, dId, globalSnapshotId);
                        totalCentral++;
                        if (centralTemporalStore->shouldCreateSnapshot())
                            shouldCreateGlobalSnapshot = true;
                    }
                } catch (const std::exception& e) {
                    stream_handler_logger.error("Temporal error for edge " + sId + "-" + dId + ": " + e.what());
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
                // Central edge — two dump() calls (src worker, dst worker)
                obj["EdgeType"] = "Central";
                obj["PID"] = part_s;
                std::string centralEdgeData_s = obj.dump();

                obj["PID"] = part_d;
                std::string centralEdgeData_d = obj.dump();

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
                    stream_handler_logger.info(
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

    stream_handler_logger.info("Consumer thread " + std::to_string(threadId) +
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

    // Determine number of consumer threads from property, fallback to default
    int numConsumerThreads = DEFAULT_CONSUMER_THREADS;
    std::string threadsProp = Utils::getJasmineGraphProperty("org.jasminegraph.kafka.consumer.threads");
    if (!threadsProp.empty()) {
        int val = std::atoi(threadsProp.c_str());
        if (val > 0) numConsumerThreads = val;
    }

    // Get the topic that the original consumer already subscribed to
    std::vector<std::string> subscription = kstream->consumer.get_subscription();
    if (subscription.empty()) {
        stream_handler_logger.error("KafkaConnector has no topic subscription — cannot start consumers");
        return;
    }
    const std::string topic = subscription[0];

    stream_handler_logger.info("Starting " + std::to_string(numConsumerThreads) +
                               " parallel Kafka consumer threads for topic '" + topic + "'");

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

    stream_handler_logger.info("All consumer threads finished. Total messages: " +
                               std::to_string(totalMessages.load()) +
                               " (local=" + std::to_string(totalLocal.load()) +
                               ", central=" + std::to_string(totalCentral.load()) + ")");

    // Flush all remaining worker batches
    stream_handler_logger.info("Flushing remaining worker batches...");
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
    stream_handler_logger.info("Termination signal sent to all workers");

    graphPartitioner.updateMetaDB();
    graphPartitioner.printStats();
    finalizeAllSnapshots();

    // Unsubscribe the original (unused) consumer cleanly
    kstream->Unsubscribe();

}

