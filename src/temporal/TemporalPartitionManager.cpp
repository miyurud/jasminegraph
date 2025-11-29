#include "TemporalPartitionManager.h"

#include <sstream>
#include <optional>
#include "../util/logger/Logger.h"

Logger temporal_partition_logger;

namespace jasminegraph {

struct TemporalPartitionManager::Impl {
    Config cfg;
    std::unique_ptr<SnapshotManager> snapshotMgr;
    
    // Auto-flush threading
    std::atomic<bool> running{false};
    std::thread autoFlushThread;
    mutable std::mutex recordQueueMutex;
    std::queue<StreamEdgeRecord> pendingRecords;
    
    // Statistics
    std::atomic<size_t> recordCount{0};
    std::atomic<SnapshotID> currentSnapshot{0};
    
    Impl(const Config& c) : cfg(c) {
        // Initialize snapshot manager for this partition
        SnapshotManager::Config smCfg;
        smCfg.baseDir = cfg.baseDir + "/p" + std::to_string(cfg.partitionId);
        smCfg.snapshotInterval = cfg.snapshotInterval;
        
        snapshotMgr = std::make_unique<SnapshotManager>(smCfg);
        
        temporal_partition_logger.info("Temporal partition manager initialized for graph " + 
                                     std::to_string(cfg.graphId) + ", partition " + 
                                     std::to_string(cfg.partitionId));
    }
};

TemporalPartitionManager::TemporalPartitionManager(const Config& cfg) 
    : impl(std::make_unique<Impl>(cfg)) {
    
    if (impl->cfg.autoFlush) {
        startAutoFlush();
    }
}

TemporalPartitionManager::~TemporalPartitionManager() {
    stopAutoFlush();
}

void TemporalPartitionManager::ingestEdgeRecord(const StreamEdgeRecord& record) {
    {
        std::lock_guard<std::mutex> lock(impl->recordQueueMutex);
        impl->pendingRecords.push(record);
        impl->recordCount++;
    }
    
    temporal_partition_logger.debug("Ingested edge record: " + record.source.id + " -> " + 
                                  record.destination.id + " (op: " + 
                                  std::to_string(static_cast<int>(record.op)) + ")");
}

SnapshotID TemporalPartitionManager::flushSnapshot() {
    std::queue<StreamEdgeRecord> recordsToProcess;
    
    // Extract pending records
    {
        std::lock_guard<std::mutex> lock(impl->recordQueueMutex);
        recordsToProcess = std::move(impl->pendingRecords);
        impl->pendingRecords = std::queue<StreamEdgeRecord>(); // Clear original queue
    }
    
    // Process records through snapshot manager
    size_t processedCount = 0;
    while (!recordsToProcess.empty()) {
        const auto& record = recordsToProcess.front();
        impl->snapshotMgr->ingest(record);
        recordsToProcess.pop();
        processedCount++;
    }
    
    // Flush snapshot manager to create S_t
    SnapshotID snapshotId = impl->snapshotMgr->flush();
    impl->currentSnapshot = snapshotId;
    impl->recordCount = 0;
    
    temporal_partition_logger.info("Flushed snapshot " + std::to_string(snapshotId) + 
                                 " for partition " + std::to_string(impl->cfg.partitionId) + 
                                 " with " + std::to_string(processedCount) + " records");
    
    return snapshotId;
}

std::vector<EdgeRecordOnDisk> TemporalPartitionManager::getActiveEdgesFor(VertexID v, SnapshotID s) {
    auto adjacency = impl->snapshotMgr->buildAdjacency(s);
    std::vector<EdgeRecordOnDisk> result;
    
    auto it = adjacency.find(v);
    if (it != adjacency.end()) {
        result.reserve(it->second.size());
        for (const auto& ref : it->second) {
            result.push_back(EdgeRecordOnDisk{ref.edgeId, v, 0, 0});
        }
    }
    
    return result;
}

PropertyResult<std::string> TemporalPartitionManager::getEdgeProperty(EdgeID e, SnapshotID s, const std::string& key) {
    return impl->snapshotMgr->getEdgeProperty(e, s, key);
}

void TemporalPartitionManager::startAutoFlush() {
    if (impl->running.exchange(true)) {
        return; // Already running
    }
    
    impl->autoFlushThread = std::thread(&TemporalPartitionManager::autoFlushWorker, this);
    temporal_partition_logger.info("Started auto-flush worker for partition " + 
                                 std::to_string(impl->cfg.partitionId) + 
                                 " with interval " + std::to_string(impl->cfg.snapshotInterval.count()) + "s");
}

void TemporalPartitionManager::stopAutoFlush() {
    if (!impl->running.exchange(false)) {
        return; // Already stopped
    }

    if (impl->autoFlushThread.joinable()) {
        impl->autoFlushThread.join();
    }
    
    // Final flush before stopping
    if (getPendingRecordCount() > 0) {
        flushSnapshot();
    }
    
    temporal_partition_logger.info("Stopped auto-flush worker for partition " + 
                                 std::to_string(impl->cfg.partitionId));
}

size_t TemporalPartitionManager::getPendingRecordCount() const {
    return impl->recordCount.load();
}

SnapshotID TemporalPartitionManager::getCurrentSnapshotId() const {
    return impl->currentSnapshot.load();
}

void TemporalPartitionManager::autoFlushWorker() {
    while (impl->running.load()) {
        std::this_thread::sleep_for(impl->cfg.snapshotInterval);
        
        if (!impl->running.load()) {
            break;
        }
        
        size_t pendingCount = getPendingRecordCount();
        if (pendingCount > 0) {
            temporal_partition_logger.debug("Auto-flush triggered with " + std::to_string(pendingCount) + " pending records");
            flushSnapshot();
        }
    }
}

} // namespace jasminegraph