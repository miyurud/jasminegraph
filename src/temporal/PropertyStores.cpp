#include "PropertyStores.h"

#include <mutex>

namespace jasminegraph {

struct EdgePropertyStore::Impl {
    Paths paths; mutable std::mutex mu; Impl(const Paths& p) : paths(p) {}
};

struct VertexPropertyStore::Impl {
    Paths paths; mutable std::mutex mu; Impl(const Paths& p) : paths(p) {}
};

EdgePropertyStore::EdgePropertyStore(const Paths& paths) : impl(new Impl(paths)) {}
EdgePropertyStore::~EdgePropertyStore() = default;

void EdgePropertyStore::append(EdgeID edgeId, const std::string& key, const std::string& value,
                               SnapshotID startSnapshot, SnapshotID endSnapshot) {
    // TODO: Append to EP log; ensure fsync batching per snapshot interval.
    (void)edgeId; (void)key; (void)value; (void)startSnapshot; (void)endSnapshot;
}

std::optional<std::string> EdgePropertyStore::get(EdgeID edgeId, SnapshotID snapshot, const std::string& key) const {
    // TODO: Index by (edgeId,key) -> segments; binary search for snapshot.
    (void)edgeId; (void)snapshot; (void)key; return std::nullopt;
}

VertexPropertyStore::VertexPropertyStore(const Paths& paths) : impl(new Impl(paths)) {}
VertexPropertyStore::~VertexPropertyStore() = default;

void VertexPropertyStore::append(VertexID vertexId, const std::string& key, const std::string& value,
                                 SnapshotID startSnapshot, SnapshotID endSnapshot) {
    // TODO: Append to VP log similar to EP.
    (void)vertexId; (void)key; (void)value; (void)startSnapshot; (void)endSnapshot;
}

std::optional<std::string> VertexPropertyStore::get(VertexID vertexId, SnapshotID snapshot, const std::string& key) const {
    (void)vertexId; (void)snapshot; (void)key; return std::nullopt;
}

} // namespace jasminegraph


