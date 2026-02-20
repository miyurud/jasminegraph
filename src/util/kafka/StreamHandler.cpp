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

void StreamHandler::listen_to_kafka_topic() {
    // Start automatic OpenTelemetry tracing for entire streaming session
    OTEL_TRACE_FUNCTION();
    
    // Add attributes to identify this streaming session
    OpenTelemetryUtil::addSpanAttribute("graph.id", std::to_string(graphId));
    OpenTelemetryUtil::addSpanAttribute("partitions", std::to_string(numberOfPartitions));
    OpenTelemetryUtil::addSpanAttribute("temporal.enabled", !localTemporalStores.empty() ? "true" : "false");
    
    // get workers
    JasmineGraphServer *server = JasmineGraphServer::getInstance();
    std::vector<JasmineGraphServer::worker> workers = server->workers(workerClients.size());

    // assign partitions to workers
    for (int i = 0; i < workerClients.size(); i++) {
        Utils::assignPartitionToWorker(graphId, i, workers.at(i).hostname, workers.at(i).port);
    }

    uint64_t messagesProcessed = 0;
    uint64_t emptyPolls = 0;
    uint64_t localEdgesAdded = 0;
    uint64_t centralEdgesAdded = 0;
    uint64_t lastProgressCheck = 0;
    const uint64_t MAX_CONSECUTIVE_EMPTY_POLLS = 60;
    
    // Timing accumulators for performance analysis
    auto total_parse_time = std::chrono::microseconds(0);
    auto total_partition_time = std::chrono::microseconds(0);
    auto total_temporal_time = std::chrono::microseconds(0);
    auto total_publish_time = std::chrono::microseconds(0);
    
    stream_handler_logger.info("Starting Kafka consumer loop for graph " + std::to_string(graphId));
    
    // Batch processing mode: consume multiple messages at once
    const size_t KAFKA_BATCH_SIZE = 2000;  // Process up to 2000 messages per batch (optimized for high throughput)
    bool useTerminationSignal = true;

    while (true) {
        // Poll batch of messages instead of single message
        std::vector<cppkafka::Message> messageBatch = this->pollMessageBatch(KAFKA_BATCH_SIZE);
        
        // Check for empty batch (timeout)
        if (messageBatch.empty()) {
            emptyPolls++;
            
            if (messagesProcessed > 0 && emptyPolls >= MAX_CONSECUTIVE_EMPTY_POLLS) {
                stream_handler_logger.info("Stream timeout: No messages for " + 
                                          std::to_string(MAX_CONSECUTIVE_EMPTY_POLLS) + " seconds");
                stream_handler_logger.info("Edges added: " + std::to_string(localEdgesAdded) + " local, " + 
                                          std::to_string(centralEdgesAdded) + " central");
                stream_handler_logger.warn("Did not receive termination signal (-1). Exiting due to timeout.");
                break;
            }
            continue;
        }
        
        emptyPolls = 0;
        
        // Process all messages in batch
        for (auto& msg : messageBatch) {
            // Check for termination or error
            if (this->isEndOfStream(msg)) {
                stream_handler_logger.info("Received termination signal (-1) from Kafka");
                stream_handler_logger.info("Total messages processed: " + std::to_string(messagesProcessed));
                stream_handler_logger.info("Edges added: " + std::to_string(localEdgesAdded) + " local, " + 
                                          std::to_string(centralEdgesAdded) + " central");
                
                // Flush all batches before sending termination signal
                stream_handler_logger.info("Flushing all batches before termination...");
                for (size_t i = 0; i < workerBatches.size(); i++) {
                    flushWorkerBatch(i, true);
                }
                
                // Wait for async publish queue to drain
                int waitCount = 0;
                while (waitCount < 50) {
                    std::unique_lock<std::mutex> lock(queueMutex);
                    if (publishQueue.empty()) break;
                    lock.unlock();
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    waitCount++;
                }
                
                // Send termination signal to all workers
                for (auto &workerClient : workerClients) {
                    if (workerClient != nullptr) {
                        workerClient->publish("-1");
                    }
                }
                useTerminationSignal = false;
                break;
            }
            
            if (this->isErrorInMessage(msg)) {
                continue;  // Skip error messages, continue with batch
            }
            
            // Increment message counter
            messagesProcessed++;
            
            if (messagesProcessed > lastProgressCheck) {
                lastProgressCheck = messagesProcessed;
            }
            
            // Reduce tracing overhead: only trace every 1000th message in batch mode
            bool shouldTrace = (messagesProcessed % 1000 == 0);
            ScopedTracer* message_tracer = nullptr;
            if (shouldTrace) {
                message_tracer = new ScopedTracer("process_kafka_batch", {
                    {"batch.size", std::to_string(messageBatch.size())},
                    {"messages.processed", std::to_string(messagesProcessed)},
                    {"graph.id", std::to_string(graphId)}
                });
            }
            
            // 1. JSON PARSING
            auto parse_start = std::chrono::high_resolution_clock::now();
            string data(msg.get_payload());
            auto edgeJson = json::parse(data);

            auto prop = edgeJson["properties"];
            prop["graphId"] = to_string(this->graphId);
            auto sourceJson = edgeJson["source"];
            auto destinationJson = edgeJson["destination"];
            string sId = std::string(sourceJson["id"]);
            string dId = std::string(destinationJson["id"]);
            auto parse_duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now() - parse_start);
            total_parse_time += parse_duration;
            
            // 2. PARTITIONING
            auto partition_start = std::chrono::high_resolution_clock::now();
            partitionedEdge partEdge = graphPartitioner.addEdge({sId, dId});
            long part_s = partEdge[0].second;
            long part_d = partEdge[1].second;
            auto partition_duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now() - partition_start);
            total_partition_time += partition_duration;            
            sourceJson["pid"] = part_s;
            destinationJson["pid"] = part_d;
            json obj;
            obj["source"] = sourceJson;
            obj["destination"] = destinationJson;
            obj["properties"] = prop;
            
            int n_workers = atoi((Utils::getJasmineGraphProperty("org.jasminegraph.server.nworkers")).c_str());
            long temp_s = part_s % n_workers;
            long temp_d = part_d % n_workers;

            // 3. TEMPORAL STORE OPERATIONS
            auto temporal_start = std::chrono::high_resolution_clock::now();
            bool shouldCreateGlobalSnapshot = false;
            
            if (!localTemporalStores.empty()) {
                try {
                    if (part_s == part_d) {
                        // Local edge: both nodes in same partition
                        if (localTemporalStores.find(part_s) == localTemporalStores.end()) {
                            stream_handler_logger.error("Invalid partition ID " + std::to_string(part_s) + 
                                                       " for edge " + sId + "-" + dId);
                            if (message_tracer) delete message_tracer;
                            continue;
                        }
                        // Use global snapshot ID for consistency across all partitions
                        localTemporalStores[part_s]->addEdge(sId, dId, globalSnapshotId);
                        localEdgesAdded++;
                        
                        // Check if THIS partition reached threshold (trigger global snapshot for ALL)
                        if (localTemporalStores[part_s]->shouldCreateSnapshot()) {
                            shouldCreateGlobalSnapshot = true;
                        }
                    } else {
                        // Central edge: cross-partition
                        if (centralTemporalStore == nullptr) {
                            stream_handler_logger.error("Central temporal store is null for edge " + sId + "-" + dId);
                            if (message_tracer) delete message_tracer;
                            continue;
                        }
                        centralTemporalStore->addEdge(sId, dId, globalSnapshotId);
                        centralEdgesAdded++;
                    
                    // Check if central store reached threshold (trigger global snapshot for ALL)
                    if (centralTemporalStore->shouldCreateSnapshot()) {
                        shouldCreateGlobalSnapshot = true;
                    }
                }
                
                // GLOBAL SNAPSHOT COORDINATION: When ANY partition/central store reaches threshold,
                // create snapshot for ALL partitions simultaneously with the same snapshot ID
                if (shouldCreateGlobalSnapshot) {
                    // Trace snapshot save operation
                    OTEL_TRACE_OPERATION("save_global_snapshot");
                    OpenTelemetryUtil::addSpanAttribute("snapshot.id", std::to_string(globalSnapshotId));
                    OpenTelemetryUtil::addSpanAttribute("trigger.partition", std::to_string(part_s));
                    
                    createGlobalSnapshot();
                }
            } catch (const std::bad_alloc& e) {
                // Critical: Memory exhaustion - try emergency snapshot save
                stream_handler_logger.error("CRITICAL: Memory allocation failed at edge " + 
                                          std::to_string(messagesProcessed) + 
                                          ". Attempting emergency snapshot save...");
                
                if (message_tracer) {
                    message_tracer->setStatus(trace_api::StatusCode::kError, "Memory allocation failed");
                    delete message_tracer;
                }
                
                std::string snapshotDir = Utils::getJasmineGraphHome() + "/env/data/temporal_snapshots";
                Utils::createDirectory(snapshotDir);
                
                // Try to save all snapshots before crashing
                bool savedAny = false;
                for (auto& [partitionId, store] : localTemporalStores) {
                    if (store != nullptr && store->saveSnapshotToDisk(snapshotDir, false)) {
                        stream_handler_logger.info("Emergency saved partition " + std::to_string(partitionId));
                        savedAny = true;
                    }
                }
                if (centralTemporalStore != nullptr && centralTemporalStore->saveSnapshotToDisk(snapshotDir, false)) {
                    stream_handler_logger.info("Emergency saved central store");
                    savedAny = true;
                }
                
                if (savedAny) {
                    stream_handler_logger.error("Emergency snapshots saved. Terminating stream processing.");
                } else {
                    stream_handler_logger.error("Failed to save emergency snapshots. Data may be lost.");
                }
                
                // Re-throw to terminate processing gracefully
                throw;
            } catch (const std::exception& e) {
                stream_handler_logger.error("Exception while adding edge " + sId + "-" + dId + ": " + e.what());
                if (message_tracer) {
                    message_tracer->setStatus(trace_api::StatusCode::kError, e.what());
                }
                // Continue processing other edges
            }
        }
        auto temporal_duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now() - temporal_start);
        total_temporal_time += temporal_duration;

        // 4. WORKERPublishing - batch publishing (minimal overhead)
        auto publish_start = std::chrono::high_resolution_clock::now();
        
        // Add to batch instead of immediate publish
        std::string edgeData = obj.dump();
        if (part_s == part_d) {
            // Local edge
            obj["EdgeType"] = "Local";
            obj["PID"] = part_s;
            std::string localEdgeData = obj.dump();
            
            std::unique_lock<std::mutex> lock(*workerBatchMutexes[temp_s]);
            workerBatches[temp_s].push_back(localEdgeData);
            size_t batchSize = workerBatches[temp_s].size();
            lock.unlock();
            
            // Flush if batch is full
            if (batchSize >= BATCH_SIZE) {
                flushWorkerBatch(temp_s, false);
            }
        } else {
            // Central edge - send to both workers
            obj["EdgeType"] = "Central";
            obj["PID"] = part_s;
            std::string centralEdgeData_s = obj.dump();
            
            obj["PID"] = part_d;
            std::string centralEdgeData_d = obj.dump();
            
            // Add to both worker batches
            {
                std::unique_lock<std::mutex> lock(*workerBatchMutexes[temp_s]);
                workerBatches[temp_s].push_back(centralEdgeData_s);
                size_t batchSize = workerBatches[temp_s].size();
                lock.unlock();
                if (batchSize >= BATCH_SIZE) {
                    flushWorkerBatch(temp_s, false);
                }
            }
            {
                std::unique_lock<std::mutex> lock(*workerBatchMutexes[temp_d]);
                workerBatches[temp_d].push_back(centralEdgeData_d);
                size_t batchSize = workerBatches[temp_d].size();
                lock.unlock();
                if (batchSize >= BATCH_SIZE) {
                    flushWorkerBatch(temp_d, false);
                }
            }
        }
        
        auto publish_duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now() - publish_start);
        total_publish_time += publish_duration;
        if (message_tracer) {
            delete message_tracer;  // Clean up tracer
        }
        }  // End of message batch processing (for loop)
        
        // Check if we should break from main loop (termination signal received)
        if (!useTerminationSignal) {
            break;
        }
        
        // Periodically flush all batches to ensure timely delivery
        if (messagesProcessed % 1000 == 0) {
            for (size_t i = 0; i < workerBatches.size(); i++) {
                flushWorkerBatch(i, false);
            }
        }
        
        // Log performance statistics every 10000 messages
        if (messagesProcessed % 10000 == 0) {
            OTEL_TRACE_OPERATION("streaming_performance_checkpoint");
            
            auto avg_parse = total_parse_time.count() / messagesProcessed;
            auto avg_partition = total_partition_time.count() / messagesProcessed;
            auto avg_temporal = total_temporal_time.count() / messagesProcessed;
            auto avg_publish = total_publish_time.count() / messagesProcessed;
            auto avg_total = avg_parse + avg_partition + avg_temporal + avg_publish;
            
            // Add performance metrics as span attributes
            OpenTelemetryUtil::addSpanAttribute("messages.processed", std::to_string(messagesProcessed));
            OpenTelemetryUtil::addSpanAttribute("avg.parse_us", std::to_string(avg_parse));
            OpenTelemetryUtil::addSpanAttribute("avg.partition_us", std::to_string(avg_partition));
            OpenTelemetryUtil::addSpanAttribute("avg.temporal_us", std::to_string(avg_temporal));
            OpenTelemetryUtil::addSpanAttribute("avg.publish_us", std::to_string(avg_publish));
            OpenTelemetryUtil::addSpanAttribute("avg.total_us", std::to_string(avg_total));
            
            // Calculate percentages
            if (avg_total > 0) {
                OpenTelemetryUtil::addSpanAttribute("pct.parse", std::to_string(100 * avg_parse / avg_total));
                OpenTelemetryUtil::addSpanAttribute("pct.partition", std::to_string(100 * avg_partition / avg_total));
                OpenTelemetryUtil::addSpanAttribute("pct.temporal", std::to_string(100 * avg_temporal / avg_total));
                OpenTelemetryUtil::addSpanAttribute("pct.publish", std::to_string(100 * avg_publish / avg_total));
            }
            
            stream_handler_logger.info("Performance @ " + std::to_string(messagesProcessed) + " messages: " +
                "parse=" + std::to_string(avg_parse) + "us (" + std::to_string(100 * avg_parse / avg_total) + "%), " +
                "partition=" + std::to_string(avg_partition) + "us (" + std::to_string(100 * avg_partition / avg_total) + "%), " +
                "temporal=" + std::to_string(avg_temporal) + "us (" + std::to_string(100 * avg_temporal / avg_total) + "%), " +
                "publish=" + std::to_string(avg_publish) + "us (" + std::to_string(100 * avg_publish / avg_total) + "%)");;
        }
    }
    
    stream_handler_logger.info("Edges added: " + std::to_string(localEdgesAdded) + " local, " + 
                              std::to_string(centralEdgesAdded) + " central, " +
                              std::to_string(localEdgesAdded + centralEdgesAdded) + " total");
    stream_handler_logger.info("Kafka consumption completed. Total messages processed: " + std::to_string(messagesProcessed));
    
    // Flush all remaining batches before finishing
    stream_handler_logger.info("Flushing remaining worker batches...");
    for (size_t i = 0; i < workerBatches.size(); i++) {
        flushWorkerBatch(i, true);
    }
    
    // Wait for async publish queue to drain
    int waitCount = 0;
    while (waitCount < 50) {  // Wait up to 5 seconds
        std::unique_lock<std::mutex> lock(queueMutex);
        if (publishQueue.empty()) break;
        lock.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        waitCount++;
    }
    stream_handler_logger.info("All batches flushed and published");
    
    graphPartitioner.updateMetaDB();
    graphPartitioner.printStats();
    
    // Save final snapshots for all partitions (even if below threshold)
    finalizeAllSnapshots();
}
