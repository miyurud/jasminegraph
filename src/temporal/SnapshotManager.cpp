#include "SnapshotManager.h"

#include <ctime>
#include <sstream>
#include <functional>
#include <algorithm>
#include "../util/logger/Logger.h"

Logger snapshot_manager_logger;

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
    
    // Store source and destination vertex properties if present
    // For scaffolding, use simple hash of vertex ID as VertexID
    if (!rec.source.properties.empty()) {
        VertexID srcVertexId = std::hash<std::string>{}(rec.source.id);
        for (const auto& prop : rec.source.properties) {
            impl->vpStore.append(srcVertexId, prop.first, prop.second, impl->currentSnapshot, UINT64_MAX);
        }
    }
    
    if (!rec.destination.properties.empty()) {
        VertexID dstVertexId = std::hash<std::string>{}(rec.destination.id);
        for (const auto& prop : rec.destination.properties) {
            impl->vpStore.append(dstVertexId, prop.first, prop.second, impl->currentSnapshot, UINT64_MAX);
        }
    }
}

SnapshotID SnapshotManager::flush() {
    snapshot_manager_logger.info("Flushing snapshot " + std::to_string(impl->currentSnapshot) + 
                                " with " + std::to_string(impl->buffer.size()) + " buffered operations");
    
    // Apply buffered ops to storage
    for (const auto& kv : impl->buffer) {
        EdgeID eid = kv.first;
        const auto& pe = kv.second;
        
        std::string opName;
        switch (pe.op) {
            case StreamOp::Insert:
                opName = "INSERT";
                impl->bitmapMgr.setActive(eid, impl->currentSnapshot, true);
                // Store properties for new edges
                for (const auto& p : pe.properties) {
                    impl->epStore.append(eid, p.first, p.second, impl->currentSnapshot, UINT64_MAX);
                    snapshot_manager_logger.debug("Stored edge property: EdgeID=" + std::to_string(eid) + 
                                                 ", key=" + p.first + ", value=" + p.second + 
                                                 ", snapshot=" + std::to_string(impl->currentSnapshot));
                }
                break;
            case StreamOp::Update:
                opName = "UPDATE";
                // Keep edge active and update properties
                impl->bitmapMgr.setActive(eid, impl->currentSnapshot, true);
                for (const auto& p : pe.properties) {
                    impl->epStore.append(eid, p.first, p.second, impl->currentSnapshot, UINT64_MAX);
                    snapshot_manager_logger.debug("Updated edge property: EdgeID=" + std::to_string(eid) + 
                                                 ", key=" + p.first + ", value=" + p.second + 
                                                 ", snapshot=" + std::to_string(impl->currentSnapshot));
                }
                break;
            case StreamOp::Delete:
                opName = "DELETE";
                impl->bitmapMgr.setActive(eid, impl->currentSnapshot, false);
                // Properties remain in store but edge becomes inactive
                snapshot_manager_logger.debug("Deactivated edge: EdgeID=" + std::to_string(eid) + 
                                             ", snapshot=" + std::to_string(impl->currentSnapshot));
                break;
        }
        
        snapshot_manager_logger.debug("Processed " + opName + " operation for EdgeID=" + std::to_string(eid) + 
                                    " with " + std::to_string(pe.properties.size()) + " properties");
    }
    
    size_t processedOps = impl->buffer.size();
    impl->buffer.clear();
    
    SnapshotID completedSnapshot = impl->currentSnapshot++;
    snapshot_manager_logger.info("Completed snapshot " + std::to_string(completedSnapshot) + 
                                " with " + std::to_string(processedOps) + " operations");
    
    return completedSnapshot;
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

PropertyResult<std::string> SnapshotManager::getEdgeProperty(EdgeID e, SnapshotID s, const std::string& key) const {
    return impl->epStore.get(e, s, key);
}

PropertyResult<std::string> SnapshotManager::getVertexProperty(VertexID v, SnapshotID s, const std::string& key) const {
    return impl->vpStore.get(v, s, key);
}

} // namespace jasminegraph


