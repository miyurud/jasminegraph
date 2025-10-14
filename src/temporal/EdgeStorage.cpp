#include "EdgeStorage.h"

#include <algorithm>
#include <fstream>
#include <sstream>

namespace jasminegraph {

struct EdgeStorage::Impl {
    Paths paths;
    std::mutex ioMutex;
    Impl(const Paths& p) : paths(p) {}
};

EdgeStorage::EdgeStorage(const Paths& paths) : impl(std::make_unique<Impl>(paths)) {}
EdgeStorage::~EdgeStorage() = default;

EdgeID EdgeStorage::getOrCreateEdgeId(const std::string& srcExtId, const std::string& dstExtId) {
    // TODO: Integrate with SQLite metadata: edge id counter and mapping table (src,dst)->EdgeID.
    // For scaffolding purposes, return 0; actual implementation will ensure durability and reuse.
    (void)srcExtId; (void)dstExtId;
    return 0ULL;
}

void EdgeStorage::appendNewEdge(EdgeID edgeId, VertexID src, VertexID dst, std::uint64_t propertiesPtr) {
    // TODO: Append binary record {edgeId, src, dst, propertiesPtr} to active NEDB file.
    (void)edgeId; (void)src; (void)dst; (void)propertiesPtr;
}

void EdgeStorage::forEachEdgeUpTo(SnapshotID snapshotId, const std::function<void(const EdgeRecordOnDisk&)>& visitor) const {
    // TODO: Enumerate FEDB + NEDBs <= snapshotId and invoke visitor for each decoded record.
    (void)snapshotId; (void)visitor;
}

void EdgeStorage::compactUpTo(SnapshotID snapshotId) {
    // TODO: Merge NEDBs up to snapshotId into a new FEDB; ensure crash safety via temp files + atomic rename.
    (void)snapshotId;
}

} // namespace jasminegraph


