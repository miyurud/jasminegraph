/**
Compressed bitmap manager for edge activity across snapshots.
*/

#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "TemporalTypes.h"

namespace jasminegraph {

class BitmapManager {
public:
    struct Paths {
        std::string baseDir; // location to persist bitmaps
    };

    explicit BitmapManager(const Paths& paths);
    ~BitmapManager();

    // Append activity bit for edgeId in the current snapshot (1 active, 0 inactive).
    void setActive(EdgeID edgeId, SnapshotID snapshotId, bool active);

    // Returns whether an edge is active at a given snapshot. Uses compressed representation if available.
    bool isActive(EdgeID edgeId, SnapshotID snapshotId) const;

    // Align timelines when compacting; optionally purge obsolete ranges.
    void compactTimelinesUpTo(SnapshotID snapshotId);

private:
    struct Impl;
    std::unique_ptr<Impl> impl;
};

} // namespace jasminegraph


