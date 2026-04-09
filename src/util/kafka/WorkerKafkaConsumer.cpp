/**
Copyright 2025 JasmineGraph Team
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

#include "WorkerKafkaConsumer.h"

#include <chrono>
#include <sys/stat.h>
#include <nlohmann/json.hpp>

#include "../Utils.h"
#include "../../temporalstore/TemporalStorePersistence.h"
#include "../../server/JasmineGraphInstanceService.h"

using json = nlohmann::json;

namespace {
Logger& workerKafkaLogger() {
    static Logger logger;
    return logger;
}

std::string getTemporalSnapshotDir() {
    std::string configuredPath =
        Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.temporalsnapshotfolder");
    if (!configuredPath.empty()) {
        return configuredPath;
    }

    return Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") +
           "/temporal_snapshots";
}
}  // namespace


// ─────────────────────────────────────────────────────────────────────────────
// Constructor / Destructor
// ─────────────────────────────────────────────────────────────────────────────

WorkerKafkaConsumer::WorkerKafkaConsumer(
        const WorkerKafkaConfig& config,
        std::map<std::string, JasmineGraphIncrementalLocalStore*>& incrementalLocalStoreMap)
    : cfg(config),
      localStoreMap(incrementalLocalStoreMap),
      streamHandler(incrementalLocalStoreMap),
      globalSnapshotId(config.initialSnapshotId) {
    for (int p : config.ownedPartitions) {
        ownedPartitionSet.insert(p);
    }

    // Worker-specific central store ID avoids file collision between workers
    centralPartitionId = cfg.numberOfPartitions + cfg.workerIndex;

    // Pre-initialize per-partition temporal mutexes
    for (int p : config.ownedPartitions) {
        partitionTemporalMutexes_[p];  // default-construct mutex
    }

    initTemporalStores();

    // Pre-initialize InstanceStreamHandler stores for all owned partitions
    // BEFORE consumer threads start — eliminates data races on map insertions
    streamHandler.preInitPartitions(cfg.graphId, cfg.ownedPartitions);

    workerKafkaLogger().info(
        "WorkerKafkaConsumer ready: graph=" + std::to_string(cfg.graphId) +
        " worker=" + std::to_string(cfg.workerIndex) +
        " centralPartitionId=" + std::to_string(centralPartitionId) +
        " ownedPartitions=[" + [&] {
            std::string s;
            for (int p : cfg.ownedPartitions) s += std::to_string(p) + ",";
            if (!s.empty()) s.pop_back();
            return s;
        }() + "]" +
        " threads=" + std::to_string(cfg.numConsumerThreads));
}

WorkerKafkaConsumer::~WorkerKafkaConsumer() noexcept {
    try {
        finalizeAllSnapshots();
    } catch (const std::exception& e) {
        workerKafkaLogger().error(std::string("[FINALIZE] Exception in destructor: ") + e.what());
    } catch (...) {
        workerKafkaLogger().error("[FINALIZE] Unknown exception in destructor");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Temporal store lifecycle (mirrors StreamHandler constructor temporal block)
// ─────────────────────────────────────────────────────────────────────────────

void WorkerKafkaConsumer::initTemporalStores() {
    if (!cfg.temporalEnabled) {
        workerKafkaLogger().warn("[TEMPORAL INIT] temporalEnabled=false for graph=" +
                                 std::to_string(cfg.graphId) + " — no snapshots will be created");
        return;
    }

    std::string snapshotDir = getTemporalSnapshotDir();
    workerKafkaLogger().info("[TEMPORAL INIT] graph=" + std::to_string(cfg.graphId) +
                             " edgeThreshold=" + std::to_string(cfg.edgeThreshold) +
                             " timeThreshold=" + std::to_string(cfg.timeThreshold) +
                             " ownedPartitions=" + std::to_string(cfg.ownedPartitions.size()) +
                             " numberOfPartitions=" + std::to_string(cfg.numberOfPartitions) +
                             " snapshotDir=" + snapshotDir);
    if (cfg.edgeThreshold == 0 && cfg.timeThreshold == 0) {
        workerKafkaLogger().warn("[TEMPORAL INIT] BOTH thresholds are 0 — snapshots will NEVER be"
                                 " auto-triggered during streaming for graph=" +
                                 std::to_string(cfg.graphId));
    }
    // Verify snapshot directory is accessible (shared volume check)
    struct stat stDir;
    if (stat(snapshotDir.c_str(), &stDir) != 0 || !S_ISDIR(stDir.st_mode)) {
        workerKafkaLogger().warn("[TEMPORAL INIT] snapshotDir not accessible: " + snapshotDir +
                                 " — if running in Docker, check the volume mount!");
    } else {
        workerKafkaLogger().info("[TEMPORAL INIT] snapshotDir accessible: " + snapshotDir);
    }

    for (int p : cfg.ownedPartitions) {
        localTemporalStores[p] = std::make_unique<TemporalStore>(
            cfg.graphId, p,
            cfg.timeThreshold, cfg.edgeThreshold,
            SnapshotManager::SnapshotMode::HYBRID);
    }

    // Central store uses worker-specific partition ID to avoid collisions
    // between workers writing to the same snapshot directory.
    // Worker 0 → centralPartitionId = numberOfPartitions + 0
    // Worker 1 → centralPartitionId = numberOfPartitions + 1
    centralTemporalStore = std::make_unique<TemporalStore>(
        cfg.graphId, centralPartitionId,
        cfg.timeThreshold, cfg.edgeThreshold,
        SnapshotManager::SnapshotMode::HYBRID);

    for (int p : cfg.ownedPartitions) {
        std::string metaPath = TemporalStorePersistence::generateMetaFilePath(
            snapshotDir, cfg.graphId, p);
        uint32_t maxId = TemporalStorePersistence::readLatestSnapshotId(metaPath);
        if (maxId != UINT32_MAX) {
            std::string bitmapPath = TemporalStorePersistence::generateBitmapFilePath(
                snapshotDir, cfg.graphId, p);
            if (localTemporalStores[p]->loadBitmapIndexFromDisk(bitmapPath)) {
                // loadBitmapIndexFromDisk already restores snapshotId via setCurrentSnapshotId
                localTemporalStores[p]->openNewSnapshot();
                workerKafkaLogger().info(
                    "Restored temporal snapshot for graph=" + std::to_string(cfg.graphId) +
                    " partition=" + std::to_string(p) +
                    " snapshotId=" + std::to_string(maxId));
            }
        }
    }

    {
        std::string metaPath = TemporalStorePersistence::generateMetaFilePath(
            snapshotDir, cfg.graphId, centralPartitionId);
        uint32_t maxCentral = TemporalStorePersistence::readLatestSnapshotId(metaPath);
        if (maxCentral != UINT32_MAX) {
            std::string bitmapPath = TemporalStorePersistence::generateBitmapFilePath(
                snapshotDir, cfg.graphId, centralPartitionId);
            workerKafkaLogger().info("[TEMPORAL INIT] Restoring central bitmap index: " + bitmapPath);
            if (centralTemporalStore->loadBitmapIndexFromDisk(bitmapPath)) {
                centralTemporalStore->openNewSnapshot();
                workerKafkaLogger().info(
                    "Restored central temporal snapshot for graph=" + std::to_string(cfg.graphId) +
                    " snapshotId=" + std::to_string(maxCentral));
            } else {
                workerKafkaLogger().error("[TEMPORAL INIT] Failed to load central bitmap index: " + bitmapPath);
            }
        } else {
            workerKafkaLogger().info("[TEMPORAL INIT] No existing central bitmap index for graph=" +
                                     std::to_string(cfg.graphId) + " — starting fresh");
        }
    }
}

void WorkerKafkaConsumer::finalizeAllSnapshots() {
    bool expected = false;
    if (!snapshotsFinalized.compare_exchange_strong(expected, true)) return;

    if (localTemporalStores.empty() && !centralTemporalStore) {
        workerKafkaLogger().info("[FINALIZE] No temporal stores to save for graph=" +
                                 std::to_string(cfg.graphId));
        return;
    }

    std::string snapshotDir = getTemporalSnapshotDir();
    Utils::createDirectory(snapshotDir);
    workerKafkaLogger().info("[FINALIZE] Saving final snapshots for graph=" +
                             std::to_string(cfg.graphId) + " dir=" + snapshotDir);

    uint32_t finalSnapId = globalSnapshotId.load();  // use the same global ID used by addEdge
    for (auto& [pid, store] : localTemporalStores) {
        if (store) {
            bool ok = store->saveBitmapIndexToDisk(snapshotDir, finalSnapId);
            store->appendSnapshotMetaToDisk(snapshotDir, finalSnapId);
            std::string bitmapPath = TemporalStorePersistence::generateBitmapFilePath(
                snapshotDir, cfg.graphId, pid);
            workerKafkaLogger().info("[FINALIZE] partition=" + std::to_string(pid) +
                                     " snapId=" + std::to_string(finalSnapId) +
                                     " path=" + bitmapPath +
                                     (ok ? " SAVED OK" : " SAVE FAILED"));
        }
    }
    if (centralTemporalStore) {
        bool ok = centralTemporalStore->saveBitmapIndexToDisk(snapshotDir, finalSnapId);
        centralTemporalStore->appendSnapshotMetaToDisk(snapshotDir, finalSnapId);
        std::string bitmapPath = TemporalStorePersistence::generateBitmapFilePath(
            snapshotDir, cfg.graphId, centralPartitionId);
        workerKafkaLogger().info("[FINALIZE] central path=" + bitmapPath + " snapId=" + std::to_string(finalSnapId) +
                                 (ok ? " SAVED OK" : " SAVE FAILED"));
    }
}

void WorkerKafkaConsumer::createGlobalSnapshot() {
    std::string snapshotDir = getTemporalSnapshotDir();
    Utils::createDirectory(snapshotDir);

    uint32_t snapId = globalSnapshotId.load();
    workerKafkaLogger().info("[SNAPSHOT] Creating global snapshot " + std::to_string(snapId) +
                             " for graph=" + std::to_string(cfg.graphId) +
                             " dir=" + snapshotDir);

    for (auto& [pid, store] : localTemporalStores) {
        if (store) {
            bool ok = store->saveBitmapIndexToDisk(snapshotDir, snapId);
            store->appendSnapshotMetaToDisk(snapshotDir, snapId);
            workerKafkaLogger().info("[SNAPSHOT] partition=" + std::to_string(pid) +
                                     (ok ? " SAVED OK" : " SAVE FAILED"));
        }
    }
    if (centralTemporalStore) {
        bool ok = centralTemporalStore->saveBitmapIndexToDisk(snapshotDir, snapId);
        centralTemporalStore->appendSnapshotMetaToDisk(snapshotDir, snapId);
        workerKafkaLogger().info("[SNAPSHOT] central" +
                                 std::string(ok ? " SAVED OK" : " SAVE FAILED"));
    }

    // Open next snapshot epoch on all stores
    for (auto& [pid, store] : localTemporalStores) {
        if (store) store->openNewSnapshot();
    }
    if (centralTemporalStore) centralTemporalStore->openNewSnapshot();

    globalSnapshotId++;
    workerKafkaLogger().info("[SNAPSHOT] Global snapshot " + std::to_string(snapId) + " created.");
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

bool WorkerKafkaConsumer::isOwnedPartition(int part) const {
    return ownedPartitionSet.count(part) > 0;
}

bool WorkerKafkaConsumer::isEndOfStream(const cppkafka::Message& msg) {
    if (!msg || !msg.get_payload()) return false;
    return std::string(msg.get_payload()) == "-1";
}

bool WorkerKafkaConsumer::isErrorInMessage(const cppkafka::Message& msg) {
    if (!msg) return true;
    if (msg.get_error()) {
        auto code = msg.get_error().get_error();
        if (code == RD_KAFKA_RESP_ERR__PARTITION_EOF) return true;
        if (code == RD_KAFKA_RESP_ERR__TIMED_OUT)    return true;
        workerKafkaLogger().error("Kafka error: " + msg.get_error().to_string());
        return true;
    }
    return false;
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-thread consumer loop
// ─────────────────────────────────────────────────────────────────────────────

void WorkerKafkaConsumer::consumerThreadFunc(
        int                    threadId,
        cppkafka::Consumer*    consumer,
        std::atomic<uint64_t>& totalMessages,
        std::atomic<uint64_t>& totalLocal,
        std::atomic<uint64_t>& totalCentral,
        std::atomic<bool>&     endSignalReceived) {
    try {
    consumer->subscribe({cfg.topic});
    workerKafkaLogger().info("Worker consumer thread " + std::to_string(threadId) +
                             " subscribed to " + cfg.topic);

    const size_t   KAFKA_BATCH   = 2000;
    const uint64_t MAX_IDLE_POLLS = 30;  // seconds with zero real messages before giving up
                                           // (primary stop = endSignalReceived via -1 sentinel)
    uint64_t       idleSeconds    = 0;  // counts seconds with zero valid messages
    uint64_t       threadMsgs    = 0;
    uint64_t       errorMsgs     = 0;  // total error/EOF messages seen

    // Thread-local done flag: this thread exits only when IT receives -1
    // from its own assigned Kafka partition.  The shared endSignalReceived
    // is kept only as an emergency stop (exceptions / crashes).
    bool myDone = false;

    // Hash functor used for deterministic partition assignment (HASH algo)
    std::hash<std::string> hasher;

    // Pre-computed once: graphId as string for fast identifier building
    const std::string graphIdStr = std::to_string(cfg.graphId);

    workerKafkaLogger().info("Worker thread " + std::to_string(threadId) +
                             " entering consume loop (MAX_IDLE=" +
                             std::to_string(MAX_IDLE_POLLS) + "s)");

    while (!endSignalReceived && !myDone) {
        auto batch = consumer->poll_batch(KAFKA_BATCH, std::chrono::milliseconds(1000));

        // Count how many messages in this batch are actual data (not errors)
        size_t validInBatch = 0;
        for (auto& m : batch) {
            if (!m || m.get_error()) {
                ++errorMsgs;
                continue;
            }
            ++validInBatch;
        }

        if (validInBatch == 0) {
            // No real messages — increment idle counter
            ++idleSeconds;
            if (idleSeconds % 30 == 0) {
                workerKafkaLogger().info("Worker thread " + std::to_string(threadId) +
                    ": idle " + std::to_string(idleSeconds) + "s" +
                    " (batchSize=" + std::to_string(batch.size()) +
                    " errors=" + std::to_string(errorMsgs) +
                    " processed=" + std::to_string(threadMsgs) + ")");
            }
            if (idleSeconds >= MAX_IDLE_POLLS) {
                workerKafkaLogger().warn("Worker thread " + std::to_string(threadId) +
                                         ": idle timeout (" +
                                         std::to_string(idleSeconds) +
                                         "s, errors=" + std::to_string(errorMsgs) +
                                         ") — exiting");
                break;
            }
            continue;
        }
        idleSeconds = 0;  // reset only when we get REAL messages
        if (threadMsgs == 0 && validInBatch > 0) {
            workerKafkaLogger().info("Worker thread " + std::to_string(threadId) +
                                     ": first valid batch! validInBatch=" +
                                     std::to_string(validInBatch) +
                                     " batchSize=" + std::to_string(batch.size()));
        }

        for (auto& msg : batch) {
            if (myDone) break;

            if (isEndOfStream(msg)) {
                workerKafkaLogger().info("Worker thread " + std::to_string(threadId) +
                                         " received end-of-stream signal on its Kafka partition"
                                         " — signalling all threads to stop");
                // Set BOTH flags:
                //   myDone          → this thread exits its inner batch loop immediately
                //   endSignalReceived → all other threads exit their outer while loop
                //                      before their cppkafka::Consumer is destroyed.
                //
                // Why both? When this thread exits and its Consumer goes out of scope,
                // librdkafka triggers a Kafka consumer-group REBALANCE which revokes
                // partition assignments from every other thread in the group.  After
                // rebalance those threads get no new messages (committed offset is already
                // at end-of-topic) and they would idle for MAX_IDLE_POLLS seconds before
                // timing out.  Setting endSignalReceived=true here lets all threads exit
                // cleanly in the CURRENT poll iteration — before the rebalance fires.
                //
                // Safety: -1 is only published AFTER the Python producer has flushed and
                // closed all connections (all edges are in Kafka before -1 arrives), so it
                // is always safe to stop every thread the moment any thread sees -1.
                myDone = true;
                endSignalReceived = true;
                break;
            }
            if (isErrorInMessage(msg)) continue;

            // ── 1. JSON PARSE ──────────────────────────────────────────────
            std::string raw(msg.get_payload());
            json edgeJson;
            try {
                edgeJson = json::parse(raw);
            } catch (const std::exception& e) {
                workerKafkaLogger().error("T" + std::to_string(threadId) +
                                          " JSON parse error: " + e.what());
                continue;
            }

            auto prop    = edgeJson["properties"];
            prop["graphId"] = graphIdStr;
            auto srcJson = edgeJson["source"];
            auto dstJson = edgeJson["destination"];
            std::string sId = std::string(srcJson["id"]);
            std::string dId = std::string(dstJson["id"]);

            // ── 2. DETERMINISTIC HASH PARTITION ───────────────────────────
            // Identical formula to Partitioner::hashPartitioning so assignments
            // are consistent with what the master stores in the meta DB.
            int part_s = static_cast<int>(hasher(sId) % cfg.numberOfPartitions);
            int part_d = static_cast<int>(hasher(dId) % cfg.numberOfPartitions);

            bool srcOwned = isOwnedPartition(part_s);
            bool dstOwned = isOwnedPartition(part_d);

            // Ignore edges that don't touch this worker's partitions at all
            if (!srcOwned && !dstOwned) continue;

            // ── 3. BUILD EDGE OBJECT (same JSON schema the TCP path produces) ──
            srcJson["pid"] = part_s;
            dstJson["pid"] = part_d;
            json obj;
            obj["source"]      = srcJson;
            obj["destination"] = dstJson;
            obj["properties"]  = prop;

            // ── 4. TEMPORAL STORE (per-partition locks for reduced contention) ──
            if (cfg.temporalEnabled) {
                bool shouldSnapshot = false;
                if (part_s == part_d) {
                    // Local edge: lock only the specific partition's mutex
                    auto it = localTemporalStores.find(part_s);
                    if (it != localTemporalStores.end()) {
                        std::lock_guard<std::mutex> tlock(partitionTemporalMutexes_[part_s]);
                        it->second->addEdge(sId, dId, globalSnapshotId.load());
                        ++totalLocal;
                        if (it->second->shouldCreateSnapshot())
                            shouldSnapshot = true;
                    }
                } else {
                    // Central edge: lock the central mutex
                    if (centralTemporalStore) {
                        std::lock_guard<std::mutex> tlock(centralTemporalMutex_);
                        centralTemporalStore->addEdge(sId, dId, globalSnapshotId.load());
                        ++totalCentral;
                        if (centralTemporalStore->shouldCreateSnapshot())
                            shouldSnapshot = true;
                    }
                }
                if (shouldSnapshot) {
                    // CAS: exactly one thread triggers the snapshot; others see the flag set and skip.
                    // Eliminates the O(partitions) re-scan that the old mutex+loop approach needed.
                    bool expected = false;
                    if (snapshotInProgress_.compare_exchange_strong(
                            expected, true,
                            std::memory_order_acq_rel,
                            std::memory_order_relaxed)) {
                        createGlobalSnapshot();
                        snapshotInProgress_.store(false, std::memory_order_release);
                    }
                }
            }

            // ── 5. WRITE TO INCREMENTAL LOCAL STORE ───────────────────────
            // Use handleRequest() which queues the edge and delegates to a
            // dedicated per-partition processing thread.  This is critical
            // because NodeManager uses static thread_local fstream pointers –
            // the store MUST be created and used on the same thread.
            if (part_s == part_d) {
                // Local edge — both endpoints live in the same owned partition
                obj["EdgeType"] = "Local";
                obj["PID"]      = part_s;
                streamHandler.handleRequest(std::move(obj),
                                            graphIdStr + "_" + std::to_string(part_s));
            } else {
                // Central edge — may need to write to src's partition, dst's partition, or both
                obj["EdgeType"] = "Central";

                if (srcOwned && dstOwned) {
                    // Both partitions owned: copy for src, move into dst
                    json objSrc = obj;
                    objSrc["PID"] = part_s;
                    obj["PID"]    = part_d;
                    streamHandler.handleRequest(std::move(objSrc),
                                                graphIdStr + "_" + std::to_string(part_s));
                    streamHandler.handleRequest(std::move(obj),
                                                graphIdStr + "_" + std::to_string(part_d));
                } else if (srcOwned) {
                    obj["PID"] = part_s;
                    streamHandler.handleRequest(std::move(obj),
                                                graphIdStr + "_" + std::to_string(part_s));
                } else {
                    obj["PID"] = part_d;
                    streamHandler.handleRequest(std::move(obj),
                                                graphIdStr + "_" + std::to_string(part_d));
                }
            }

            ++threadMsgs;
            ++totalMessages;
            if (threadMsgs % 50000 == 0 || (threadMsgs <= 1000 && threadMsgs % 100 == 0)) {
                workerKafkaLogger().info("Worker thread " + std::to_string(threadId) +
                                         ": " + std::to_string(threadMsgs) + " msgs queued" +
                                         " (total=" + std::to_string(totalMessages.load()) + ")");
            }
        }  // end message loop
    }  // end while

    workerKafkaLogger().info("Worker consumer thread " + std::to_string(threadId) +
                             " finished. Messages processed: " + std::to_string(threadMsgs));
    } catch (const std::exception& e) {
        workerKafkaLogger().error("[CRASH] consumerThreadFunc thread " + std::to_string(threadId) +
                                  " caught exception: " + e.what());
        endSignalReceived = true;
    } catch (...) {
        workerKafkaLogger().error("[CRASH] consumerThreadFunc thread " + std::to_string(threadId) +
                                  " caught unknown exception");
        endSignalReceived = true;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// run() — public entry point
// ─────────────────────────────────────────────────────────────────────────────

WorkerKafkaStats WorkerKafkaConsumer::run() {
    std::atomic<uint64_t> totalMessages{0};
    std::atomic<uint64_t> totalLocal   {0};
    std::atomic<uint64_t> totalCentral {0};
    std::atomic<bool>     endSignalReceived{false};

    // Build Kafka consumer configuration.
    // Each worker uses a DISTINCT consumer group so it gets ALL messages
    // (fan-out read), not only a share of the topic partitions.
    // auto.offset.reset=earliest — workers consume ALL messages in the topic from offset 0.
    // This matches the old master-based behavior: publish to Kafka first, then call adstrmk.
    cppkafka::Configuration kafkaCfg = {
        {"metadata.broker.list", cfg.brokers},
        {"group.id",             cfg.groupId},
        {"auto.offset.reset",    "earliest"},
        {"enable.auto.commit",   "true"},
        {"session.timeout.ms",   "10000"},
        {"log.connection.close",  "false"}};

    // Error callback surfaces librdkafka-internal problems (broker disconnect, auth, etc.)
    kafkaCfg.set_error_callback([](cppkafka::KafkaHandleBase& /*handle*/, int error,
                                   const std::string& reason) {
        workerKafkaLogger().error("[librdkafka] error=" + std::to_string(error) + " reason=" + reason);
    });

    workerKafkaLogger().info("[KAFKA CFG] graph=" + std::to_string(cfg.graphId) +
                             " broker=" + cfg.brokers +
                             " topic=" + cfg.topic +
                             " groupId=" + cfg.groupId +
                             " offset=earliest threads=" + std::to_string(cfg.numConsumerThreads) +
                             " (consuming ALL messages from beginning of topic)");

    // Spawn N parallel consumer threads
    std::vector<std::thread> threads;
    threads.reserve(cfg.numConsumerThreads);

    for (int i = 0; i < cfg.numConsumerThreads; ++i) {
        auto consumer = std::make_unique<cppkafka::Consumer>(kafkaCfg);
        threads.emplace_back(
            [this, i, c = std::move(consumer),
             &totalMessages, &totalLocal, &totalCentral, &endSignalReceived]() mutable {
                consumerThreadFunc(i, c.get(),
                                   totalMessages, totalLocal, totalCentral,
                                   endSignalReceived);
            });
    }

    for (auto& t : threads) {
        if (t.joinable()) t.join();
    }

    // Persist temporal snapshots NOW — consumer threads are all done so the
    // temporal stores are fully populated.  Do this BEFORE terminating the
    // InstanceStreamHandler writer threads: those threads drain the incremental
    // local store (NodeManager disk I/O) and can block for minutes under heavy
    // load.  Snapshot creation must not wait for that drain.
    finalizeAllSnapshots();

    // Terminate the InstanceStreamHandler per-partition processing threads.
    // handleRequest("-1") sets terminateThreads=true, notifies condition
    // variables, and joins all processing threads before returning.
    streamHandler.handleRequest("-1");

    WorkerKafkaStats stats{totalMessages.load(), totalLocal.load(), totalCentral.load()};
    workerKafkaLogger().info(
        "WorkerKafkaConsumer done: total=" + std::to_string(stats.totalMessages) +
        " local="   + std::to_string(stats.totalLocal)   +
        " central=" + std::to_string(stats.totalCentral));
    return stats;
}
