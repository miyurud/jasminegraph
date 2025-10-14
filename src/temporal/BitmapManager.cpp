#include "BitmapManager.h"

namespace jasminegraph {

struct BitmapManager::Impl {
    Paths paths;
    mutable std::mutex mu;
    Impl(const Paths& p) : paths(p) {}
};

BitmapManager::BitmapManager(const Paths& paths) : impl(new Impl(paths)) {}
BitmapManager::~BitmapManager() = default;

void BitmapManager::setActive(EdgeID edgeId, SnapshotID snapshotId, bool active) {
    // TODO: maintain compressed bitset (e.g., Roaring) per edge or a global bit-packed file with index.
    (void)edgeId; (void)snapshotId; (void)active;
}

bool BitmapManager::isActive(EdgeID edgeId, SnapshotID snapshotId) const {
    // TODO: query compressed structure; return default false in scaffolding.
    (void)edgeId; (void)snapshotId;
    return false;
}

void BitmapManager::compactTimelinesUpTo(SnapshotID snapshotId) {
    // TODO: trim historical ranges and rewrite segments as needed.
    (void)snapshotId;
}

} // namespace jasminegraph


