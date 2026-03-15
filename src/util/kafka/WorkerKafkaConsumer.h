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

WorkerKafkaConsumer — direct Kafka consumer for JasmineGraph worker (instance) nodes.

ARCHITECTURE (new — this file):
  Kafka topic → each worker owns an independent consumer group
  Each worker reads ALL Kafka messages and keeps only the edges where
  at least one vertex belongs to its assigned graph partitions.
  This removes the master-relay bottleneck entirely.

  OLD path:  Kafka ─▶ Master (N consumer threads)
                           └─TCP relay─▶ Worker 0
                           └─TCP relay─▶ Worker 1  ...
  NEW path:  Kafka ─▶ Worker 0 (N consumer threads, independent group)
             Kafka ─▶ Worker 1 (N consumer threads, independent group)
             Master only coordinates (assignPartitions, wait, finalizeMetaDB)

LIMITATION: Currently applies to HASH partitioning only.
  FENNEL / LDG require shared adaptive state; those still use the master-relay
  path as a fallback (see StreamHandler::listen_to_kafka_topic).
**/

#pragma once

#include <atomic>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include <cppkafka/cppkafka.h>

#include "../../localstore/incremental/JasmineGraphIncrementalLocalStore.h"
#include "../../temporalstore/TemporalStore.h"
#include "../logger/Logger.h"
#include "InstanceStreamHandler.h"

// ─────────────────────────────────────────────────────────────────────────────
// Configuration bundle serialized as JSON and sent from master → worker
// ─────────────────────────────────────────────────────────────────────────────
struct WorkerKafkaConfig {
    std::string brokers;             // e.g. "kafka:9092"
    std::string topic;               // Kafka topic to subscribe to
    std::string groupId;             // unique consumer-group ID per (session, worker)
    int         graphId         = 0;
    int         numberOfPartitions = 1;
    std::vector<int> ownedPartitions; // graph-partitions owned by THIS worker
    bool        temporalEnabled = false;
    uint64_t    timeThreshold   = 60;    // snapshot time threshold (seconds)
    uint64_t    edgeThreshold   = 10000; // snapshot edge-count threshold
    uint32_t    initialSnapshotId = 0;
    int         numConsumerThreads = 3;  // parallel Kafka consumer threads
    int         workerIndex     = 0;     // index of this worker (0, 1, 2, ...)
};

// ─────────────────────────────────────────────────────────────────────────────
// Per-stream statistics reported back to master when consumption completes
// ─────────────────────────────────────────────────────────────────────────────
struct WorkerKafkaStats {
    uint64_t totalMessages = 0;
    uint64_t totalLocal    = 0;
    uint64_t totalCentral  = 0;
};

// ─────────────────────────────────────────────────────────────────────────────
// WorkerKafkaConsumer
// ─────────────────────────────────────────────────────────────────────────────
class WorkerKafkaConsumer {
 public:
    /**
     * @param config               Parameters sent from the master.
     * @param incrementalLocalStoreMap  Per-worker in-memory graph store map
     *                             (keyed by "graphId_partitionId").
     */
    WorkerKafkaConsumer(
        const WorkerKafkaConfig& config,
        std::map<std::string, JasmineGraphIncrementalLocalStore*>& incrementalLocalStoreMap);

    ~WorkerKafkaConsumer();

    /**
     * Blocking.  Spawns cfg.numConsumerThreads parallel Kafka consumer
     * threads, each in the same consumer group so Kafka auto-balances topic
     * partitions among them.  Returns when the end-of-stream marker ("-1")
     * has been received or the idle timeout is reached.
     */
    WorkerKafkaStats run();

 private:
    WorkerKafkaConfig   cfg;
    std::map<std::string, JasmineGraphIncrementalLocalStore*>& localStoreMap;
    InstanceStreamHandler streamHandler;   // wraps localStoreMap for edge writes

    // Temporal stores: per owned-partition + one shared central store
    std::map<int, TemporalStore*> localTemporalStores;
    TemporalStore*                centralTemporalStore = nullptr;
    int centralPartitionId = -1;  // worker-specific central store partition ID

    std::atomic<uint32_t> globalSnapshotId{0};
    std::atomic<bool>     snapshotsFinalized{false};
    std::atomic<bool>     snapshotInProgress_{false};  // CAS guard: only one thread triggers snapshot

    // Per-partition mutexes: reduce contention vs. a single global lock
    std::map<int, std::mutex> partitionTemporalMutexes_;
    std::mutex centralTemporalMutex_;
    std::mutex snapshotMutex_;   // serialises createGlobalSnapshot() calls

    // Fast O(1) ownership lookup
    std::unordered_set<int> ownedPartitionSet;

    // ── helpers ──────────────────────────────────────────────────────────────
    bool isOwnedPartition(int part) const;

    void consumerThreadFunc(
        int                    threadId,
        cppkafka::Consumer*    consumer,
        std::atomic<uint64_t>& totalMessages,
        std::atomic<uint64_t>& totalLocal,
        std::atomic<uint64_t>& totalCentral,
        std::atomic<bool>&     endSignalReceived);

    void initTemporalStores();
    void finalizeAllSnapshots();
    void createGlobalSnapshot();

    static bool isEndOfStream(const cppkafka::Message& msg);
    static bool isErrorInMessage(const cppkafka::Message& msg);
};
