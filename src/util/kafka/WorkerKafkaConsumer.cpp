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

static Logger worker_kafka_logger;

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
        partitionTemporalMutexes_[p]; // default-construct mutex
    }

    initTemporalStores();

    // Pre-initialize InstanceStreamHandler stores for all owned partitions
    // BEFORE consumer threads start — eliminates data races on map insertions
    streamHandler.preInitPartitions(cfg.graphId, cfg.ownedPartitions);

    worker_kafka_logger.info(
        "WorkerKafkaConsumer ready: graph=" + std::to_string(cfg.graphId) +
        " worker=" + std::to_string(cfg.workerIndex) +
        " centralPartitionId=" + std::to_string(centralPartitionId) +
        " ownedPartitions=[" + [&]{
            std::string s;
            for (int p : cfg.ownedPartitions) s += std::to_string(p) + ",";
            if (!s.empty()) s.pop_back();
            return s;
        }() + "]" +
        " threads=" + std::to_string(cfg.numConsumerThreads));
}

WorkerKafkaConsumer::~WorkerKafkaConsumer() {
    finalizeAllSnapshots();
    for (auto& [pid, store] : localTemporalStores) {
        delete store;
    }
    localTemporalStores.clear();
    if (centralTemporalStore) {
        delete centralTemporalStore;
        centralTemporalStore = nullptr;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Temporal store lifecycle (mirrors StreamHandler constructor temporal block)
// ─────────────────────────────────────────────────────────────────────────────

void WorkerKafkaConsumer::initTemporalStores() {
    if (!cfg.temporalEnabled) {
        worker_kafka_logger.warn("[TEMPORAL INIT] temporalEnabled=false for graph=" +
                                 std::to_string(cfg.graphId) + " — no snapshots will be created");
        return;
    }

    std::string snapshotDir = Utils::getJasmineGraphHome() + "/env/data/temporal_snapshots";
    worker_kafka_logger.info("[TEMPORAL INIT] graph=" + std::to_string(cfg.graphId) +
                             " edgeThreshold=" + std::to_string(cfg.edgeThreshold) +
                             " timeThreshold=" + std::to_string(cfg.timeThreshold) +
                             " ownedPartitions=" + std::to_string(cfg.ownedPartitions.size()) +
                             " numberOfPartitions=" + std::to_string(cfg.numberOfPartitions) +
                             " snapshotDir=" + snapshotDir);
    if (cfg.edgeThreshold == 0 && cfg.timeThreshold == 0) {
        worker_kafka_logger.warn("[TEMPORAL INIT] BOTH thresholds are 0 — snapshots will NEVER be"
                                 " auto-triggered during streaming for graph=" +
                                 std::to_string(cfg.graphId));
    }
    // Verify snapshot directory is accessible (shared volume check)
    struct stat stDir;
    if (stat(snapshotDir.c_str(), &stDir) != 0 || !S_ISDIR(stDir.st_mode)) {
        worker_kafka_logger.warn("[TEMPORAL INIT] snapshotDir not accessible: " + snapshotDir +
                                 " — if running in Docker, check the volume mount!");
    } else {
        worker_kafka_logger.info("[TEMPORAL INIT] snapshotDir accessible: " + snapshotDir);
    }

    for (int p : cfg.ownedPartitions) {
        localTemporalStores[p] = new TemporalStore(
            cfg.graphId, p,
            cfg.timeThreshold, cfg.edgeThreshold,
            SnapshotManager::SnapshotMode::HYBRID);
    }

    // Central store uses worker-specific partition ID to avoid collisions
    // between workers writing to the same snapshot directory.
    // Worker 0 → centralPartitionId = numberOfPartitions + 0
    // Worker 1 → centralPartitionId = numberOfPartitions + 1
    centralTemporalStore = new TemporalStore(
        cfg.graphId, centralPartitionId,
        cfg.timeThreshold, cfg.edgeThreshold,
        SnapshotManager::SnapshotMode::HYBRID);

    for (int p : cfg.ownedPartitions) {
        uint32_t maxId = TemporalStorePersistence::findHighestSnapshotId(
            snapshotDir, cfg.graphId, p);
        if (maxId != UINT32_MAX) {
            std::string path = TemporalStorePersistence::generateFilePath(
                snapshotDir, cfg.graphId, p, maxId);
            if (localTemporalStores[p]->loadSnapshotFromDisk(path)) {
                localTemporalStores[p]->getSnapshotManager()->setCurrentSnapshotId(maxId);
                localTemporalStores[p]->openNewSnapshot();
                worker_kafka_logger.info(
                    "Restored temporal snapshot for graph=" + std::to_string(cfg.graphId) +
                    " partition=" + std::to_string(p) +
                    " snapshotId=" + std::to_string(maxId));
            }
        }
    }

    uint32_t maxCentral = TemporalStorePersistence::findHighestSnapshotId(
        snapshotDir, cfg.graphId, centralPartitionId);
    if (maxCentral != UINT32_MAX) {
        std::string path = TemporalStorePersistence::generateFilePath(
            snapshotDir, cfg.graphId, centralPartitionId, maxCentral);
        worker_kafka_logger.info("[TEMPORAL INIT] Restoring central snapshot: " + path);
        if (centralTemporalStore->loadSnapshotFromDisk(path)) {
            centralTemporalStore->getSnapshotManager()->setCurrentSnapshotId(maxCentral);
            centralTemporalStore->openNewSnapshot();
            worker_kafka_logger.info(
                "Restored central temporal snapshot for graph=" + std::to_string(cfg.graphId) +
                " snapshotId=" + std::to_string(maxCentral));
        } else {
            worker_kafka_logger.error("[TEMPORAL INIT] Failed to load central snapshot: " + path);
        }
    } else {
        worker_kafka_logger.info("[TEMPORAL INIT] No existing central snapshot for graph=" +
                                 std::to_string(cfg.graphId) + " — starting fresh");
    }
}

void WorkerKafkaConsumer::finalizeAllSnapshots() {
    bool expected = false;
    if (!snapshotsFinalized.compare_exchange_strong(expected, true)) return;

    if (localTemporalStores.empty() && !centralTemporalStore) {
        worker_kafka_logger.info("[FINALIZE] No temporal stores to save for graph=" +
                                 std::to_string(cfg.graphId));
        return;
    }

    std::string snapshotDir = Utils::getJasmineGraphHome() + "/env/data/temporal_snapshots";
    Utils::createDirectory(snapshotDir);
    worker_kafka_logger.info("[FINALIZE] Saving final snapshots for graph=" +
                             std::to_string(cfg.graphId) + " dir=" + snapshotDir);

    for (auto& [pid, store] : localTemporalStores) {
        if (store) {
            std::string expectedPath = TemporalStorePersistence::generateFilePath(
                snapshotDir, cfg.graphId, pid,
                store->getSnapshotManager()->getCurrentSnapshotId());
            bool ok = store->saveSnapshotToDisk(snapshotDir, false);
            worker_kafka_logger.info("[FINALIZE] partition=" + std::to_string(pid) +
                                     " path=" + expectedPath +
                                     (ok ? " SAVED OK" : " SAVE FAILED"));
        }
    }
    if (centralTemporalStore) {
        std::string expectedPath = TemporalStorePersistence::generateFilePath(
            snapshotDir, cfg.graphId, centralPartitionId,
            centralTemporalStore->getSnapshotManager()->getCurrentSnapshotId());
        bool ok = centralTemporalStore->saveSnapshotToDisk(snapshotDir, false);
        worker_kafka_logger.info("[FINALIZE] central path=" + expectedPath +
                                 (ok ? " SAVED OK" : " SAVE FAILED"));
    }
}

void WorkerKafkaConsumer::createGlobalSnapshot() {
    std::string snapshotDir = Utils::getJasmineGraphHome() + "/env/data/temporal_snapshots";
    Utils::createDirectory(snapshotDir);

    uint32_t snapId = globalSnapshotId.load();
    worker_kafka_logger.info("[SNAPSHOT] Creating global snapshot " + std::to_string(snapId) +
                             " for graph=" + std::to_string(cfg.graphId) +
                             " dir=" + snapshotDir);

    for (auto& [pid, store] : localTemporalStores) {
        if (store) {
            std::string expectedPath = TemporalStorePersistence::generateFilePath(
                snapshotDir, cfg.graphId, pid, snapId);
            bool ok = store->saveSnapshotToDisk(snapshotDir, false);
            worker_kafka_logger.info("[SNAPSHOT] partition=" + std::to_string(pid) +
                                     " path=" + expectedPath +
                                     (ok ? " SAVED OK" : " SAVE FAILED"));
        }
    }
    if (centralTemporalStore) {
        std::string expectedPath = TemporalStorePersistence::generateFilePath(
            snapshotDir, cfg.graphId, centralPartitionId, snapId);
        bool ok = centralTemporalStore->saveSnapshotToDisk(snapshotDir, false);
        worker_kafka_logger.info("[SNAPSHOT] central path=" + expectedPath +
                                 (ok ? " SAVED OK" : " SAVE FAILED"));
    }

    // Open next snapshot epoch on all stores
    for (auto& [pid, store] : localTemporalStores) {
        if (store) store->openNewSnapshot();
    }
    if (centralTemporalStore) centralTemporalStore->openNewSnapshot();

    globalSnapshotId++;
    worker_kafka_logger.info("[SNAPSHOT] Global snapshot " + std::to_string(snapId) + " created.");
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
        worker_kafka_logger.error("Kafka error: " + msg.get_error().to_string());
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
    worker_kafka_logger.info("Worker consumer thread " + std::to_string(threadId) +
                             " subscribed to " + cfg.topic);

    const size_t   KAFKA_BATCH   = 2000;
    const uint64_t MAX_IDLE_POLLS = 600;   // seconds of no REAL messages before giving up
    uint64_t       idleSeconds    = 0;     // counts seconds with zero valid messages
    uint64_t       threadMsgs    = 0;
    uint64_t       errorMsgs     = 0;      // total error/EOF messages seen

    // Hash functor used for deterministic partition assignment (HASH algo)
    std::hash<std::string> hasher;

    worker_kafka_logger.info("Worker thread " + std::to_string(threadId) +
                             " entering consume loop (MAX_IDLE=" +
                             std::to_string(MAX_IDLE_POLLS) + "s)");

    while (!endSignalReceived) {
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
                worker_kafka_logger.info("Worker thread " + std::to_string(threadId) +
                    ": idle " + std::to_string(idleSeconds) + "s" +
                    " (batchSize=" + std::to_string(batch.size()) +
                    " errors=" + std::to_string(errorMsgs) +
                    " processed=" + std::to_string(threadMsgs) + ")");
            }
            if (idleSeconds >= MAX_IDLE_POLLS) {
                worker_kafka_logger.warn("Worker thread " + std::to_string(threadId) +
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
            worker_kafka_logger.info("Worker thread " + std::to_string(threadId) +
                                     ": first valid batch! validInBatch=" +
                                     std::to_string(validInBatch) +
                                     " batchSize=" + std::to_string(batch.size()));
        }

        for (auto& msg : batch) {
            if (endSignalReceived) break;

            if (isEndOfStream(msg)) {
                worker_kafka_logger.info("Worker thread " + std::to_string(threadId) +
                                         " received end-of-stream signal");
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
                worker_kafka_logger.error("T" + std::to_string(threadId) +
                                          " JSON parse error: " + e.what());
                continue;
            }

            auto prop    = edgeJson["properties"];
            prop["graphId"] = std::to_string(cfg.graphId);
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
                    std::lock_guard<std::mutex> slock(snapshotMutex_);
                    // Re-check inside lock; another thread may have already triggered it
                    bool needsSnap = false;
                    for (auto& [pid, store] : localTemporalStores) {
                        if (store && store->shouldCreateSnapshot()) { needsSnap = true; break; }
                    }
                    if (!needsSnap && centralTemporalStore &&
                        centralTemporalStore->shouldCreateSnapshot()) {
                        needsSnap = true;
                    }
                    if (needsSnap) createGlobalSnapshot();
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
                std::string edgeStr = obj.dump();
                streamHandler.handleRequest(edgeStr);
            } else {
                // Central edge — may need to write to src's partition, dst's partition, or both
                obj["EdgeType"] = "Central";

                if (srcOwned) {
                    obj["PID"]       = part_s;
                    std::string edgeStr = obj.dump();
                    streamHandler.handleRequest(edgeStr);
                }

                if (dstOwned) {
                    obj["PID"]       = part_d;
                    std::string edgeStr = obj.dump();
                    streamHandler.handleRequest(edgeStr);
                }
            }

            ++threadMsgs;
            ++totalMessages;
            if (threadMsgs % 50000 == 0 || (threadMsgs <= 1000 && threadMsgs % 100 == 0)) {
                worker_kafka_logger.info("Worker thread " + std::to_string(threadId) +
                                         ": " + std::to_string(threadMsgs) + " msgs queued" +
                                         " (total=" + std::to_string(totalMessages.load()) + ")");
            }
        }  // end message loop
    }  // end while

    worker_kafka_logger.info("Worker consumer thread " + std::to_string(threadId) +
                             " finished. Messages processed: " + std::to_string(threadMsgs));
    } catch (const std::exception& e) {
        worker_kafka_logger.error("[CRASH] consumerThreadFunc thread " + std::to_string(threadId) +
                                  " caught exception: " + e.what());
        endSignalReceived = true;
    } catch (...) {
        worker_kafka_logger.error("[CRASH] consumerThreadFunc thread " + std::to_string(threadId) +
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
        worker_kafka_logger.error("[librdkafka] error=" + std::to_string(error) + " reason=" + reason);
    });

    worker_kafka_logger.info("[KAFKA CFG] graph=" + std::to_string(cfg.graphId) +
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

    // Terminate the InstanceStreamHandler per-partition processing threads.
    // handleRequest("-1") sets terminateThreads=true, notifies condition
    // variables, and joins all processing threads before returning.
    streamHandler.handleRequest("-1");

    finalizeAllSnapshots();

    WorkerKafkaStats stats{totalMessages.load(), totalLocal.load(), totalCentral.load()};
    worker_kafka_logger.info(
        "WorkerKafkaConsumer done: total=" + std::to_string(stats.totalMessages) +
        " local="   + std::to_string(stats.totalLocal)   +
        " central=" + std::to_string(stats.totalCentral));
    return stats;
}
