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

#include <cppkafka/cppkafka.h>

#include <string>
#include <vector>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <unordered_map>
#include <memory>

#include "../../nativestore/DataPublisher.h"
#include "../../partitioner/stream/Partitioner.h"
#include "../logger/Logger.h"
#include "KafkaCC.h"
#include "../../metadb/SQLiteDBInterface.h"
#include "../../temporalstore/TemporalStore.h"

class StreamHandler {
 public:
    StreamHandler(KafkaConnector *kstream, int numberOfPartitions,
                  std::vector<DataPublisher *> &workerClients, SQLiteDBInterface* sqlite,
                  int graphId, bool isDirected, spt::Algorithms algo = spt::Algorithms::HASH);
    ~StreamHandler();
    void listen_to_kafka_topic();
    cppkafka::Message pollMessage();
    std::vector<cppkafka::Message> pollMessageBatch(size_t maxMessages = 500);
    bool isErrorInMessage(const cppkafka::Message &msg);
    bool isEndOfStream(const cppkafka::Message &msg);
    Partitioner graphPartitioner;
    int  graphId;
    uint32_t currentSnapshot;
    
    // Temporal storage: one store per partition + central store for cross-partition edges
    std::map<int, TemporalStore*> localTemporalStores;  // partitionId -> TemporalStore
    TemporalStore* centralTemporalStore;                // For cross-partition edges
    uint32_t globalSnapshotId;                          // Global snapshot counter (synchronized across all partitions)
    
 private:
    KafkaConnector *kstream;
    Logger frontend_logger;
    std::string stream_topic_name;
    std::vector<DataPublisher *> &workerClients;
    int numberOfPartitions;
    
    // Batch publishing optimization
    static constexpr size_t BATCH_SIZE = 200;  // Flush after 200 edges per worker
    std::vector<std::vector<std::string>> workerBatches;  // Per-worker edge batches
    std::vector<std::unique_ptr<std::mutex>> workerBatchMutexes;  // Protect batch access
    void flushWorkerBatch(int workerId, bool force = false);
    
    // Async publishing with thread pool
    static constexpr int PUBLISH_THREADS = 4;
    std::vector<std::thread> publishThreads;
    std::queue<std::function<void()>> publishQueue;
    std::mutex queueMutex;
    std::condition_variable queueCV;
    std::atomic<bool> stopPublishing;
    std::atomic<bool> snapshotsFinalized;  // Track if final snapshots already saved
    void startPublishThreads();
    void stopPublishThreads();
    void publishWorker();
    void enqueuePublish(std::function<void()> task);
    void finalizeAllSnapshots();  // Save all open snapshots
    void createGlobalSnapshot();  // Create synchronized snapshot across ALL partitions
    
    // Partition caching optimization
    std::unordered_map<std::string, long> partitionCache;  // nodeId -> partitionId
    std::mutex cacheMutex;
    long getCachedPartition(const std::string& nodeId, bool* cacheHit);
    void cachePartition(const std::string& nodeId, long partition);
};
