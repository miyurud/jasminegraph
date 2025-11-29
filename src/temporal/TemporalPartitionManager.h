/**
Partition-level temporal manager that integrates with worker-side processing.
Manages snapshot creation and bitmap updates for a specific graph partition.
*/

#pragma once

#include <memory>
#include <string>
#include <chrono>
#include <thread>
#include <atomic>
#include <mutex>
#include <queue>
#include <vector>

#include "SnapshotManager.h"
#include "TemporalTypes.h"
#include "PropertyStores.h"

namespace jasminegraph {

class TemporalPartitionManager {
public:
    struct Config {
        unsigned int graphId;
        unsigned int partitionId;
        std::string baseDir;
        std::chrono::seconds snapshotInterval{60}; // Δt interval
        bool autoFlush{true}; // Automatically flush snapshots on timer
    };

    explicit TemporalPartitionManager(const Config& cfg);
    ~TemporalPartitionManager();

    // Ingest streaming edge record for this partition
    void ingestEdgeRecord(const StreamEdgeRecord& record);

    // Force flush current buffer to create snapshot
    SnapshotID flushSnapshot();

    // Query interface for partition-level queries
    std::vector<EdgeRecordOnDisk> getActiveEdgesFor(VertexID v, SnapshotID s);
    PropertyResult<std::string> getEdgeProperty(EdgeID e, SnapshotID s, const std::string& key);

    // Start/stop background snapshot flushing
    void startAutoFlush();
    void stopAutoFlush();

    // Statistics
    size_t getPendingRecordCount() const;
    SnapshotID getCurrentSnapshotId() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl;
    
    void autoFlushWorker();
};

} // namespace jasminegraph