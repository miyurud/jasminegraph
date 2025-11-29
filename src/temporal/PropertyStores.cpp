#include "PropertyStores.h"

#include <mutex>
#include <map>
#include <fstream>
#include <algorithm>
#include <tuple>
#include <memory>

namespace jasminegraph {

struct EdgePropertyStore::Impl {
    std::string baseDir;
    std::mutex mutex;
    
    // In-memory property maps: edgeId -> key -> [(startSnapshot, endSnapshot, value)]
    std::unordered_map<EdgeID, std::unordered_map<std::string, std::vector<std::tuple<SnapshotID, SnapshotID, std::string>>>> propertyMap;
    
    Impl(const std::string& dir) : baseDir(dir) {
        // Create directory if it doesn't exist
        // In a real implementation, you'd use filesystem operations
    }
};

EdgePropertyStore::EdgePropertyStore(const Paths& paths) : impl(std::make_unique<Impl>(paths.baseDir)) {}

EdgePropertyStore::~EdgePropertyStore() = default;

void EdgePropertyStore::append(EdgeID edgeId, const std::string& key, const std::string& value,
                              SnapshotID startSnapshot, SnapshotID endSnapshot) {
    std::lock_guard<std::mutex> lock(impl->mutex);
    
    // Add to in-memory map
    impl->propertyMap[edgeId][key].emplace_back(startSnapshot, endSnapshot, value);
    
    // Sort by start snapshot to maintain temporal order
    auto& intervals = impl->propertyMap[edgeId][key];
    std::sort(intervals.begin(), intervals.end(), 
              [](const auto& a, const auto& b) { return std::get<0>(a) < std::get<0>(b); });
    
    // In a real implementation, you'd also write to persistent storage
    // std::ofstream logFile(impl->baseDir + "/edge_" + std::to_string(edgeId) + "_" + key + ".log", std::ios::app);
    // logFile << startSnapshot << "," << endSnapshot << "," << value << "\n";
}

PropertyResult<std::string> EdgePropertyStore::get(EdgeID edgeId, SnapshotID snapshot, const std::string& key) const {
    std::lock_guard<std::mutex> lock(impl->mutex);
    
    auto edgeIt = impl->propertyMap.find(edgeId);
    if (edgeIt == impl->propertyMap.end()) {
        return PropertyResult<std::string>();
    }
    
    auto keyIt = edgeIt->second.find(key);
    if (keyIt == edgeIt->second.end()) {
        return PropertyResult<std::string>();
    }
    
    // Find the interval that contains the snapshot
    for (const auto& interval : keyIt->second) {
        SnapshotID start = std::get<0>(interval);
        SnapshotID end = std::get<1>(interval);
        
        if (snapshot >= start && snapshot <= end) {
            return PropertyResult<std::string>(std::get<2>(interval));
        }
    }
    
    return PropertyResult<std::string>();
}

// Similar implementation for VertexPropertyStore
struct VertexPropertyStore::Impl {
    std::string baseDir;
    std::mutex mutex;
    
    // In-memory property maps: vertexId -> key -> [(startSnapshot, endSnapshot, value)]
    std::unordered_map<VertexID, std::unordered_map<std::string, std::vector<std::tuple<SnapshotID, SnapshotID, std::string>>>> propertyMap;
    
    Impl(const std::string& dir) : baseDir(dir) {}
};

VertexPropertyStore::VertexPropertyStore(const Paths& paths) : impl(std::make_unique<Impl>(paths.baseDir)) {}

VertexPropertyStore::~VertexPropertyStore() = default;

void VertexPropertyStore::append(VertexID vertexId, const std::string& key, const std::string& value,
                                 SnapshotID startSnapshot, SnapshotID endSnapshot) {
    std::lock_guard<std::mutex> lock(impl->mutex);
    
    // Add to in-memory map
    impl->propertyMap[vertexId][key].emplace_back(startSnapshot, endSnapshot, value);
    
    // Sort by start snapshot to maintain temporal order
    auto& intervals = impl->propertyMap[vertexId][key];
    std::sort(intervals.begin(), intervals.end(), 
              [](const auto& a, const auto& b) { return std::get<0>(a) < std::get<0>(b); });
    
    // In a real implementation, you'd also write to persistent storage
    // std::ofstream logFile(impl->baseDir + "/vertex_" + std::to_string(vertexId) + "_" + key + ".log", std::ios::app);
    // logFile << startSnapshot << "," << endSnapshot << "," << value << "\n";
}

PropertyResult<std::string> VertexPropertyStore::get(VertexID vertexId, SnapshotID snapshot, const std::string& key) const {
    std::lock_guard<std::mutex> lock(impl->mutex);
    
    auto vertexIt = impl->propertyMap.find(vertexId);
    if (vertexIt == impl->propertyMap.end()) {
        return PropertyResult<std::string>();
    }
    
    auto keyIt = vertexIt->second.find(key);
    if (keyIt == vertexIt->second.end()) {
        return PropertyResult<std::string>();
    }
    
    // Find the interval that contains the snapshot
    for (const auto& interval : keyIt->second) {
        SnapshotID start = std::get<0>(interval);
        SnapshotID end = std::get<1>(interval);
        
        if (snapshot >= start && snapshot <= end) {
            return PropertyResult<std::string>(std::get<2>(interval));
        }
    }
    
    return PropertyResult<std::string>();
}

} // namespace jasminegraph
