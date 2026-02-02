/**
Copyright 2026 JasminGraph Team
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

#include "HistoryTriangles.h"

#include <algorithm>
#include <set>
#include <sstream>

Logger history_triangle_logger;

TemporalTriangleResult HistoryTriangles::countTrianglesAtSnapshot(
    int graphId,
    uint32_t snapshotId,
    const std::string& snapshotDir,
    uint64_t timeThreshold,
    uint64_t edgeThreshold) {
    
    auto start = std::chrono::high_resolution_clock::now();
    
    // Default initialize all fields to 0
    TemporalTriangleResult result{};
    
    // Collect all edges from all stores
    std::vector<TemporalStore::EdgeKey> allEdges;
    int partitionsProcessed = 0;
    
    // Phase 1: Load edges from LOCAL partition snapshots
    history_triangle_logger.info("Loading local partition snapshots for graph " + 
                                std::to_string(graphId) + " at snapshot " + 
                                std::to_string(snapshotId));
    
    for (int partitionId = 0; partitionId < 100; partitionId++) {
        std::string filePath = TemporalStorePersistence::generateFilePath(
            snapshotDir, graphId, partitionId, snapshotId);
        
        TemporalStore localStore(graphId, partitionId, timeThreshold, edgeThreshold, 
                                SnapshotManager::SnapshotMode::HYBRID);
        
        if (localStore.loadSnapshotFromDisk(filePath)) {
            partitionsProcessed++;
            
            // Extract edges at this snapshot
            auto edgesAtSnapshot = localStore.getEdgesAtSnapshot(snapshotId);
            allEdges.insert(allEdges.end(), edgesAtSnapshot.begin(), edgesAtSnapshot.end());
            
            history_triangle_logger.info("Loaded local partition " + std::to_string(partitionId) + 
                                       ": " + std::to_string(edgesAtSnapshot.size()) + " edges");
        } else {
            // No more local partitions
            break;
        }
    }
    
    result.localEdges = allEdges.size();
    result.partitionsProcessed = partitionsProcessed;
    
    // Phase 2: Load edges from CENTRAL store snapshot (cross-partition edges)
    history_triangle_logger.info("Loading central store snapshot for graph " + 
                                std::to_string(graphId));
    
    size_t centralEdgesLoaded = 0;
    for (int centralId = partitionsProcessed; centralId < partitionsProcessed + 10; centralId++) {
        std::string filePath = TemporalStorePersistence::generateFilePath(
            snapshotDir, graphId, centralId, snapshotId);
        
        TemporalStore centralStore(graphId, centralId, timeThreshold, edgeThreshold,
                                  SnapshotManager::SnapshotMode::HYBRID);
        
        if (centralStore.loadSnapshotFromDisk(filePath)) {
            auto edgesAtSnapshot = centralStore.getEdgesAtSnapshot(snapshotId);
            allEdges.insert(allEdges.end(), edgesAtSnapshot.begin(), edgesAtSnapshot.end());
            centralEdgesLoaded += edgesAtSnapshot.size();
            
            history_triangle_logger.info("Loaded central store (partition " + 
                                       std::to_string(centralId) + "): " + 
                                       std::to_string(edgesAtSnapshot.size()) + " edges");
            break;  // Usually only one central store
        }
    }
    
    result.centralEdges = centralEdgesLoaded;
    result.totalEdges = allEdges.size();
    
    if (partitionsProcessed == 0) {
        history_triangle_logger.error("No snapshot files found for graph " + 
                                    std::to_string(graphId) + " at snapshot " + 
                                    std::to_string(snapshotId));
        return result;
    }
    
    // Phase 3: Count triangles on merged graph
    history_triangle_logger.info("Counting triangles on merged graph with " + 
                                std::to_string(allEdges.size()) + " edges");
    
    auto [triangleCount, uniqueNodes] = countTrianglesOnMergedGraph(allEdges);
    
    result.triangleCount = triangleCount;
    result.uniqueNodes = uniqueNodes;
    
    auto end = std::chrono::high_resolution_clock::now();
    result.durationMs = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    
    history_triangle_logger.info("Triangle count completed: " + std::to_string(triangleCount) + 
                               " triangles found in " + std::to_string(result.durationMs) + "ms");
    
    return result;
}

std::pair<uint64_t, uint32_t> HistoryTriangles::countTrianglesOnMergedGraph(
    const std::vector<TemporalStore::EdgeKey>& allEdges) {
    
    if (allEdges.empty()) {
        return {0, 0};
    }
    
    // Build node index
    std::map<std::string, uint32_t> nodeToIndex;
    std::vector<std::string> indexToNode;
    uint32_t uniqueNodes = buildNodeIndex(allEdges, nodeToIndex, indexToNode);
    
    // Build adjacency bitmaps
    auto neighbors = buildAdjacencyBitmaps(allEdges, nodeToIndex);
    
    // Count triangles using bitmap intersections
    uint64_t triangleCountRaw = countTrianglesFromBitmaps(allEdges, nodeToIndex, neighbors);
    
    // Cleanup bitmaps
    for (auto& [idx, bitmap] : neighbors) {
        roaring_bitmap_free(bitmap);
    }
    
    // Each triangle counted 3 times (once per edge), divide by 3
    uint64_t triangleCount = triangleCountRaw / 3;
    
    return {triangleCount, uniqueNodes};
}

uint32_t HistoryTriangles::buildNodeIndex(
    const std::vector<TemporalStore::EdgeKey>& edges,
    std::map<std::string, uint32_t>& nodeToIndex,
    std::vector<std::string>& indexToNode) {
    
    uint32_t nextIndex = 0;
    
    for (const auto& edge : edges) {
        if (nodeToIndex.find(edge.sourceId) == nodeToIndex.end()) {
            nodeToIndex[edge.sourceId] = nextIndex++;
            indexToNode.push_back(edge.sourceId);
        }
        if (nodeToIndex.find(edge.destId) == nodeToIndex.end()) {
            nodeToIndex[edge.destId] = nextIndex++;
            indexToNode.push_back(edge.destId);
        }
    }
    
    return nextIndex;
}

std::map<uint32_t, roaring_bitmap_t*> HistoryTriangles::buildAdjacencyBitmaps(
    const std::vector<TemporalStore::EdgeKey>& edges,
    const std::map<std::string, uint32_t>& nodeToIndex) {
    
    std::map<uint32_t, roaring_bitmap_t*> neighbors;
    
    // Initialize bitmaps for all nodes
    for (const auto& [nodeId, index] : nodeToIndex) {
        neighbors[index] = roaring_bitmap_create();
    }
    
    // Add edges to adjacency bitmaps (undirected: both directions)
    for (const auto& edge : edges) {
        auto itU = nodeToIndex.find(edge.sourceId);
        auto itV = nodeToIndex.find(edge.destId);
        
        if (itU != nodeToIndex.end() && itV != nodeToIndex.end()) {
            uint32_t u = itU->second;
            uint32_t v = itV->second;
            
            roaring_bitmap_add(neighbors[u], v);
            roaring_bitmap_add(neighbors[v], u);
        }
    }
    
    return neighbors;
}

uint64_t HistoryTriangles::countTrianglesFromBitmaps(
    const std::vector<TemporalStore::EdgeKey>& edges,
    const std::map<std::string, uint32_t>& nodeToIndex,
    const std::map<uint32_t, roaring_bitmap_t*>& neighbors) {
    
    uint64_t triangleCount = 0;
    std::set<std::pair<uint32_t, uint32_t>> processedEdges;
    
    for (const auto& edge : edges) {
        auto itU = nodeToIndex.find(edge.sourceId);
        auto itV = nodeToIndex.find(edge.destId);
        
        if (itU == nodeToIndex.end() || itV == nodeToIndex.end()) {
            continue;
        }
        
        uint32_t u = itU->second;
        uint32_t v = itV->second;
        
        // Process each undirected edge only once
        if (u > v) std::swap(u, v);
        if (processedEdges.count({u, v})) {
            continue;
        }
        processedEdges.insert({u, v});
        
        // Count common neighbors: neighbors(u) ∩ neighbors(v)
        // We count all common neighbors, then divide by 3 at the end
        auto itNeighborsU = neighbors.find(u);
        auto itNeighborsV = neighbors.find(v);
        
        if (itNeighborsU != neighbors.end() && itNeighborsV != neighbors.end()) {
            roaring_bitmap_t* intersection = roaring_bitmap_and(
                itNeighborsU->second, 
                itNeighborsV->second
            );
            triangleCount += roaring_bitmap_get_cardinality(intersection);
            roaring_bitmap_free(intersection);
        }
    }
    
    return triangleCount;
}
