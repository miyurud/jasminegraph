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
#include <unordered_set>

#include "../../../util/Utils.h"

namespace {

bool parsePartitionIdFromBitmapFileName(const std::string& fileName,
                                        int graphId,
                                        uint32_t& partitionId) {
    const std::string prefix = "graph" + std::to_string(graphId) + "_part";
    const std::string suffix = "_bitmaps.ebm";

    if (fileName.rfind(prefix, 0) != 0) {
        return false;
    }
    if (fileName.size() <= prefix.size() + suffix.size()) {
        return false;
    }
    if (fileName.compare(fileName.size() - suffix.size(), suffix.size(), suffix) != 0) {
        return false;
    }

    std::string partitionText =
        fileName.substr(prefix.size(), fileName.size() - prefix.size() - suffix.size());
    if (partitionText.empty()) {
        return false;
    }
    if (!std::all_of(partitionText.begin(), partitionText.end(),
                     [](unsigned char ch) { return std::isdigit(ch); })) {
        return false;
    }

    partitionId = static_cast<uint32_t>(std::stoul(partitionText));
    return true;
}

std::vector<uint32_t> discoverBitmapPartitions(const std::string& snapshotDir, int graphId) {
    std::vector<std::string> files = Utils::getListOfFilesInDirectory(snapshotDir);
    std::vector<uint32_t> partitionIds;
    partitionIds.reserve(files.size());

    for (const auto& file : files) {
        uint32_t partitionId = 0;
        if (parsePartitionIdFromBitmapFileName(file, graphId, partitionId)) {
            partitionIds.push_back(partitionId);
        }
    }

    std::sort(partitionIds.begin(), partitionIds.end());
    partitionIds.erase(std::unique(partitionIds.begin(), partitionIds.end()), partitionIds.end());
    return partitionIds;
}

std::vector<TemporalStore::EdgeKey> collectCumulativeEdgesUpToSnapshot(TemporalStore& store,
                                                                        uint32_t snapshotId) {
    std::unordered_set<TemporalStore::EdgeKey, TemporalStore::EdgeKey::Hash> cumulativeEdgeSet;

    for (uint32_t sid = 0; sid <= snapshotId; sid++) {
        auto edgesAtSnapshot = store.getEdgesAtSnapshot(sid);
        cumulativeEdgeSet.insert(edgesAtSnapshot.begin(), edgesAtSnapshot.end());
    }

    return std::vector<TemporalStore::EdgeKey>(cumulativeEdgeSet.begin(), cumulativeEdgeSet.end());
}

}  // namespace

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

    // Discover partitions from available bitmap files instead of scanning guessed IDs.
    // This supports sparse partition IDs and worker-distributed snapshot staging.
    history_triangle_logger.info("Discovering partition bitmap files for graph " +
                                 std::to_string(graphId));
    std::vector<uint32_t> partitionIds = discoverBitmapPartitions(snapshotDir, graphId);

    if (partitionIds.empty()) {
        history_triangle_logger.error("No bitmap index files found for graph " +
                                      std::to_string(graphId) + " in " + snapshotDir);
        return result;
    }

    history_triangle_logger.info("Loading cumulative edges from snapshot 0 to " +
                                 std::to_string(snapshotId) + " for " +
                                 std::to_string(partitionIds.size()) + " partition stores");

    for (uint32_t partitionId : partitionIds) {
        std::string filePath = TemporalStorePersistence::generateBitmapFilePath(
            snapshotDir, graphId, partitionId);

        TemporalStore localStore(graphId, partitionId, timeThreshold, edgeThreshold,
                                SnapshotManager::SnapshotMode::HYBRID);

        if (localStore.loadBitmapIndexFromDisk(filePath)) {
            partitionsProcessed++;

            auto cumulativeEdges = collectCumulativeEdgesUpToSnapshot(localStore, snapshotId);
            allEdges.insert(allEdges.end(), cumulativeEdges.begin(), cumulativeEdges.end());

            history_triangle_logger.info("Loaded partition " + std::to_string(partitionId) +
                                         ": " + std::to_string(cumulativeEdges.size()) +
                                         " cumulative edges");
        } else {
            history_triangle_logger.warn("Failed to load partition bitmap file " + filePath);
        }
    }

    result.localEdges = allEdges.size();
    result.centralEdges = 0;
    result.partitionsProcessed = partitionsProcessed;
    result.totalEdges = allEdges.size();  // raw directed edge count from all stores

    // Log unique directed edge count for diagnostics (cross-store duplicates are
    // collapsed, but opposite directions are counted separately).
    {
        std::set<std::pair<std::string, std::string>> uniqueEdgeSet;
        for (const auto& edge : allEdges) {
            uniqueEdgeSet.insert({edge.sourceId, edge.destId});
        }
        size_t uniqueEdgeCount = uniqueEdgeSet.size();
        result.uniqueEdges = uniqueEdgeCount;
        history_triangle_logger.info("Raw cumulative edges [0.." + std::to_string(snapshotId) +
                         "]: " + std::to_string(allEdges.size()) +
                         " Unique directed edges: " + std::to_string(uniqueEdgeCount));
    }

    if (result.uniqueEdges == 0) {
        std::set<std::pair<std::string, std::string>> uniqueEdgeSet;
        for (const auto& edge : allEdges) {
            uniqueEdgeSet.insert({edge.sourceId, edge.destId});
        }
        result.uniqueEdges = uniqueEdgeSet.size();
    }

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
    // Skip self-loops: they corrupt adjacency sets and produce spurious triangle counts.
    for (const auto& edge : edges) {
        if (edge.sourceId == edge.destId) continue;  // skip self-loops

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

        if (u == v) continue;  // skip self-loops

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
                itNeighborsV->second);
            triangleCount += roaring_bitmap_get_cardinality(intersection);
            roaring_bitmap_free(intersection);
        }
    }

    return triangleCount;
}
