#include "SnapshotManager.h"

#include <ctime>
#include <sstream>

namespace jasminegraph {

struct SnapshotManager::Impl {
    Config cfg;
    EdgeStorage edgeStorage;
    BitmapManager bitmapMgr;
    EdgePropertyStore epStore;
    VertexPropertyStore vpStore;

    SnapshotID currentSnapshot{0};

    // Buffer keyed by EdgeID for operations within Δt
    struct PendingEdgeOp { StreamOp op; std::unordered_map<std::string, std::string> properties; };
    std::unordered_map<EdgeID, PendingEdgeOp> buffer;

    Impl(const Config& c)
        : cfg(c),
          edgeStorage(EdgeStorage::Paths{c.baseDir + "/edges"}),
          bitmapMgr(BitmapManager::Paths{c.baseDir + "/bitmaps"}),
          epStore(EdgePropertyStore::Paths{c.baseDir + "/ep"}),
          vpStore(VertexPropertyStore::Paths{c.baseDir + "/vp"}) {}
};

SnapshotManager::SnapshotManager(const Config& cfg) : impl(new Impl(cfg)) {}
SnapshotManager::~SnapshotManager() = default;

void SnapshotManager::ingest(const StreamEdgeRecord& rec) {
    // TODO: normalize timestamp -> snapshot bucket, map string vertex IDs -> VertexID via NodeManager/metadata.
    // EdgeID resolution
    EdgeID eid = impl->edgeStorage.getOrCreateEdgeId(rec.source.id, rec.destination.id);

    auto& entry = impl->buffer[eid];
    entry.op = rec.op;
    entry.properties = rec.properties; // For updates, overwrite latest within Δt
}

SnapshotID SnapshotManager::flush() {
    // Apply buffered ops to storage
    for (const auto& kv : impl->buffer) {
        EdgeID eid = kv.first;
        const auto& pe = kv.second;
        switch (pe.op) {
            case StreamOp::Insert:
                impl->bitmapMgr.setActive(eid, impl->currentSnapshot, true);
                break;
            case StreamOp::Update:
                for (const auto& p : pe.properties) {
                    impl->epStore.append(eid, p.first, p.second, impl->currentSnapshot, UINT64_MAX);
                }
                break;
            case StreamOp::Delete:
                impl->bitmapMgr.setActive(eid, impl->currentSnapshot, false);
                break;
        }
    }
    impl->buffer.clear();
    return impl->currentSnapshot++;
}

std::unordered_map<VertexID, std::vector<TemporalEdgeRef>> SnapshotManager::buildAdjacency(SnapshotID snapshotId) {
    std::unordered_map<VertexID, std::vector<TemporalEdgeRef>> adj;
    impl->edgeStorage.forEachEdgeUpTo(snapshotId, [&](const EdgeRecordOnDisk& rec) {
        if (impl->bitmapMgr.isActive(rec.edgeId, snapshotId)) {
            adj[rec.srcId].push_back(TemporalEdgeRef{rec.edgeId, 0});
        }
    });
    return adj;
}

void SnapshotManager::compact(SnapshotID upTo) {
    impl->edgeStorage.compactUpTo(upTo);
    impl->bitmapMgr.compactTimelinesUpTo(upTo);
}

std::optional<std::string> SnapshotManager::getEdgeProperty(EdgeID e, SnapshotID s, const std::string& key) const {
    return impl->epStore.get(e, s, key);
}

std::optional<std::string> SnapshotManager::getVertexProperty(VertexID v, SnapshotID s, const std::string& key) const {
    return impl->vpStore.get(v, s, key);
}

} // namespace jasminegraph


