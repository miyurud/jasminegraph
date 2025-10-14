/**
Snapshot assembly, buffering, and compaction coordinator.
*/

#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "BitmapManager.h"
#include "EdgeStorage.h"
#include "PropertyStores.h"
#include "TemporalTypes.h"

namespace jasminegraph {

class SnapshotManager {
public:
    struct Config {
        std::string baseDir;
        std::chrono::seconds snapshotInterval{60}; // Δt
    };

    explicit SnapshotManager(const Config& cfg);
    ~SnapshotManager();

    // Ingest a stream record; performs timestamp normalization and buffering per Δt.
    void ingest(const StreamEdgeRecord& rec);

    // Force flush of current buffer into on-disk blocks creating snapshot S_t.
    SnapshotID flush();

    // Reconstruct active edges at S_t for queries; returns adjacency list structure.
    std::unordered_map<VertexID, std::vector<TemporalEdgeRef>> buildAdjacency(SnapshotID snapshotId);

    // Compaction orchestrator; merges NEDBs into FEDB and aligns bitmaps.
    void compact(SnapshotID upTo);

    // Property accessors used at query time
    std::optional<std::string> getEdgeProperty(EdgeID e, SnapshotID s, const std::string& key) const;
    std::optional<std::string> getVertexProperty(VertexID v, SnapshotID s, const std::string& key) const;

private:
    struct Impl; std::unique_ptr<Impl> impl;
};

} // namespace jasminegraph


