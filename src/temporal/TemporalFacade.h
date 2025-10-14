/**
Facade integrating temporal storage with NodeManager and query engine.
*/

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "SnapshotManager.h"

namespace jasminegraph {

class TemporalFacade {
public:
    explicit TemporalFacade(const std::string& baseDir, std::chrono::seconds snapshotInterval);

    // Query API to be used by existing algorithms without modification
    std::vector<EdgeRecordOnDisk> getActiveEdgesFor(VertexID v, SnapshotID s);
    std::optional<std::string> getEdgeProperty(EdgeID e, SnapshotID s, const std::string& key);
    std::optional<std::string> getVertexProperty(VertexID v, SnapshotID s, const std::string& key);

private:
    std::unique_ptr<SnapshotManager> snapshotMgr;
};

} // namespace jasminegraph


