#include "TemporalFacade.h"

namespace jasminegraph {

TemporalFacade::TemporalFacade(const std::string& baseDir, std::chrono::seconds snapshotInterval) {
    SnapshotManager::Config cfg; cfg.baseDir = baseDir; cfg.snapshotInterval = snapshotInterval;
    snapshotMgr.reset(new SnapshotManager(cfg));
}

std::vector<EdgeRecordOnDisk> TemporalFacade::getActiveEdgesFor(VertexID v, SnapshotID s) {
    std::vector<EdgeRecordOnDisk> result;
    auto adj = snapshotMgr->buildAdjacency(s);
    auto it = adj.find(v);
    if (it == adj.end()) return result;
    result.reserve(it->second.size());
    for (const auto& ref : it->second) {
        // Note: Full edge records can be retrieved by scanning storage; for scaffolding we return only IDs.
        result.push_back(EdgeRecordOnDisk{ref.edgeId, v, 0, 0});
    }
    return result;
}

std::optional<std::string> TemporalFacade::getEdgeProperty(EdgeID e, SnapshotID s, const std::string& key) {
    return snapshotMgr->getEdgeProperty(e, s, key);
}

std::optional<std::string> TemporalFacade::getVertexProperty(VertexID v, SnapshotID s, const std::string& key) {
    return snapshotMgr->getVertexProperty(v, s, key);
}

} // namespace jasminegraph


