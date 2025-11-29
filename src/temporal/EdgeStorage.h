/**
Temporal edge storage handling for FEDB/NEDB binary blocks.
This scaffolding defines interfaces and I/O considerations. Implementation is provided in EdgeStorage.cpp.
*/

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "TemporalTypes.h"

namespace jasminegraph {

// Manages append-only fundamental and new edge data blocks.
class EdgeStorage {
public:
    struct Paths {
        std::string baseDir; // directory containing FEDB/NEDB versions
    };

    explicit EdgeStorage(const Paths& paths);
    ~EdgeStorage();

    // Resolve or create a stable EdgeID for (src,dst). Uses a persistent counter in SQLite metadata.
    EdgeID getOrCreateEdgeId(const std::string& srcExtId, const std::string& dstExtId);

    // Append a new edge tuple into the active NEDB for the current snapshot interval.
    void appendNewEdge(EdgeID edgeId, VertexID src, VertexID dst, std::uint64_t propertiesPtr);

    // Iterate edges from FEDB and all NEDBs up to and including snapshotId. Used during snapshot assembly.
    void forEachEdgeUpTo(SnapshotID snapshotId, const std::function<void(const EdgeRecordOnDisk&)>& visitor) const;

    // Compaction: merge existing NEDBs into a new FEDB. Updates internal versioning state and removes merged NEDBs.
    void compactUpTo(SnapshotID snapshotId);

private:
    struct Impl;
    std::unique_ptr<Impl> impl;
};

} // namespace jasminegraph


