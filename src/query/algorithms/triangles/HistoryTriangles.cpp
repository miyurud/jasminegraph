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
#include <fstream>
#include <sstream>
#include <unordered_set>

#include "../../../util/Utils.h"

#ifdef _OPENMP
#include <omp.h>
#endif

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

bool parsePartitionIdFromDeltaFileName(const std::string& fileName,
                                       int graphId,
                                       uint32_t& partitionId) {
    const std::string prefix = "graph" + std::to_string(graphId) + "_part";
    const std::string splitMarker = "_snap";
    const std::string suffix = ".delta";

    if (fileName.rfind(prefix, 0) != 0) {
        return false;
    }
    if (fileName.size() <= prefix.size() + splitMarker.size() + suffix.size()) {
        return false;
    }
    if (fileName.compare(fileName.size() - suffix.size(), suffix.size(), suffix) != 0) {
        return false;
    }

    size_t snapPos = fileName.find(splitMarker, prefix.size());
    if (snapPos == std::string::npos || snapPos <= prefix.size()) {
        return false;
    }

    std::string partitionText = fileName.substr(prefix.size(), snapPos - prefix.size());
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
        if (parsePartitionIdFromBitmapFileName(file, graphId, partitionId) ||
            parsePartitionIdFromDeltaFileName(file, graphId, partitionId)) {
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

uint64_t encodeUndirectedEdge(uint32_t sourceIndex, uint32_t destIndex) {
    if (sourceIndex > destIndex) {
        std::swap(sourceIndex, destIndex);
    }
    return (static_cast<uint64_t>(sourceIndex) << 32) | static_cast<uint64_t>(destIndex);
}

uint32_t decodeSourceIndex(uint64_t encoded) {
    return static_cast<uint32_t>(encoded >> 32);
}

uint32_t decodeDestIndex(uint64_t encoded) {
    return static_cast<uint32_t>(encoded & 0xffffffffULL);
}

size_t countCommonSortedValues(const std::vector<uint32_t>& left,
                               const std::vector<uint32_t>& right) {
    size_t count = 0;
    size_t leftIndex = 0;
    size_t rightIndex = 0;

    while (leftIndex < left.size() && rightIndex < right.size()) {
        uint32_t leftValue = left[leftIndex];
        uint32_t rightValue = right[rightIndex];
        if (leftValue == rightValue) {
            ++count;
            ++leftIndex;
            ++rightIndex;
        } else if (leftValue < rightValue) {
            ++leftIndex;
        } else {
            ++rightIndex;
        }
    }

    return count;
}

uint64_t countTrianglesOnForwardGraph(const std::vector<std::vector<uint32_t>>& forwardNeighbors) {
    if (forwardNeighbors.empty()) {
        return 0;
    }
    uint64_t triangleCount = 0;

#ifdef _OPENMP
    if (forwardNeighbors.size() >= 2048) {
#pragma omp parallel for schedule(dynamic, 64) reduction(+:triangleCount)
        for (int64_t sourceIndex = 0; sourceIndex < static_cast<int64_t>(forwardNeighbors.size());
             ++sourceIndex) {
            const std::vector<uint32_t>& sourceNeighbors = forwardNeighbors[sourceIndex];
            for (uint32_t middleIndex : sourceNeighbors) {
                const std::vector<uint32_t>& middleNeighbors = forwardNeighbors[middleIndex];
                if (sourceNeighbors.size() < middleNeighbors.size()) {
                    triangleCount += countCommonSortedValues(sourceNeighbors, middleNeighbors);
                } else {
                    triangleCount += countCommonSortedValues(middleNeighbors, sourceNeighbors);
                }
            }
        }
        return triangleCount;
    }
#endif

    for (uint32_t sourceIndex = 0; sourceIndex < forwardNeighbors.size(); ++sourceIndex) {
        const std::vector<uint32_t>& sourceNeighbors = forwardNeighbors[sourceIndex];
        for (uint32_t middleIndex : sourceNeighbors) {
            const std::vector<uint32_t>& middleNeighbors = forwardNeighbors[middleIndex];
            if (sourceNeighbors.size() < middleNeighbors.size()) {
                triangleCount += countCommonSortedValues(sourceNeighbors, middleNeighbors);
            } else {
                triangleCount += countCommonSortedValues(middleNeighbors, sourceNeighbors);
            }
        }
    }

    return triangleCount;
}

}  // namespace

Logger history_triangle_logger;

TemporalTriangleResult HistoryTriangles::countTrianglesAtSnapshot(
    int graphId,
    uint32_t snapshotId,
    const std::string& snapshotDir,
    uint64_t timeThreshold,
    uint64_t edgeThreshold) {

    (void)timeThreshold;
    (void)edgeThreshold;

    auto start = std::chrono::high_resolution_clock::now();

    // Default initialize all fields to 0
    TemporalTriangleResult result{};
    int partitionsProcessed = 0;
    std::unordered_map<std::string, uint32_t> nodeToIndex;
    std::vector<uint64_t> encodedUndirectedEdges;
    uint32_t nodeCount = 0;

    auto getOrAddNode = [&](const std::string& node) -> uint32_t {
        auto it = nodeToIndex.find(node);
        if (it != nodeToIndex.end()) {
            return it->second;
        }
        uint32_t idx = nodeCount++;
        nodeToIndex.emplace(node, idx);
        return idx;
    };

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

        bool loaded = TemporalStorePersistence::forEachActiveBitmapEdgeAtSnapshot(
            filePath, snapshotId,
            [&](const std::string& sourceId, const std::string& destId) {
                if (sourceId == destId) {
                    return true;
                }

                uint32_t sourceIndex = getOrAddNode(sourceId);
                uint32_t destIndex = getOrAddNode(destId);
                encodedUndirectedEdges.push_back(encodeUndirectedEdge(sourceIndex, destIndex));
                return true;
            });

        if (loaded) {
            partitionsProcessed++;
            history_triangle_logger.info("Loaded partition " + std::to_string(partitionId) +
                                         ": streamed cumulative edges");
        } else {
            history_triangle_logger.warn("Failed to load partition bitmap file " + filePath);
        }
    }

    if (!encodedUndirectedEdges.empty()) {
        std::sort(encodedUndirectedEdges.begin(), encodedUndirectedEdges.end());
        encodedUndirectedEdges.erase(
            std::unique(encodedUndirectedEdges.begin(), encodedUndirectedEdges.end()),
            encodedUndirectedEdges.end());
    }

    result.localEdges = encodedUndirectedEdges.size();
    result.centralEdges = 0;
    result.partitionsProcessed = partitionsProcessed;
    result.totalEdges = encodedUndirectedEdges.size();
    result.uniqueEdges = encodedUndirectedEdges.size();
    result.uniqueNodes = nodeCount;

    if (partitionsProcessed == 0) {
        history_triangle_logger.error("No snapshot files found for graph " +
                                    std::to_string(graphId) + " at snapshot " +
                                    std::to_string(snapshotId));
        return result;
    }

    if (encodedUndirectedEdges.empty()) {
        history_triangle_logger.info("No edges active at snapshot " + std::to_string(snapshotId));
        return result;
    }

    // String-to-index mapping is not needed after this point; release memory early.
    nodeToIndex.clear();
    nodeToIndex.rehash(0);

    std::vector<uint32_t> degree(nodeCount, 0);
    for (uint64_t encoded : encodedUndirectedEdges) {
        uint32_t sourceIndex = decodeSourceIndex(encoded);
        uint32_t destIndex = decodeDestIndex(encoded);
        degree[sourceIndex]++;
        degree[destIndex]++;
    }

    std::vector<uint32_t> forwardDegree(nodeCount, 0);
    for (uint64_t encoded : encodedUndirectedEdges) {
        uint32_t sourceIndex = decodeSourceIndex(encoded);
        uint32_t destIndex = decodeDestIndex(encoded);

        bool sourceBeforeDest = (degree[sourceIndex] < degree[destIndex]) ||
                                (degree[sourceIndex] == degree[destIndex] &&
                                 sourceIndex < destIndex);
        if (sourceBeforeDest) {
            forwardDegree[sourceIndex]++;
        } else {
            forwardDegree[destIndex]++;
        }
    }

    std::vector<std::vector<uint32_t>> forwardNeighbors(nodeCount);
    for (uint32_t nodeIndex = 0; nodeIndex < nodeCount; ++nodeIndex) {
        forwardNeighbors[nodeIndex].reserve(forwardDegree[nodeIndex]);
    }

    for (uint64_t encoded : encodedUndirectedEdges) {
        uint32_t sourceIndex = decodeSourceIndex(encoded);
        uint32_t destIndex = decodeDestIndex(encoded);

        bool sourceBeforeDest = (degree[sourceIndex] < degree[destIndex]) ||
                                (degree[sourceIndex] == degree[destIndex] &&
                                 sourceIndex < destIndex);
        if (sourceBeforeDest) {
            forwardNeighbors[sourceIndex].push_back(destIndex);
        } else {
            forwardNeighbors[destIndex].push_back(sourceIndex);
        }
    }

    for (auto& neighbors : forwardNeighbors) {
        std::sort(neighbors.begin(), neighbors.end());
    }

    encodedUndirectedEdges.clear();
    encodedUndirectedEdges.shrink_to_fit();

    history_triangle_logger.info("Counting triangles on degree-ordered forward graph with " +
                                 std::to_string(result.uniqueEdges) + " edges");

    result.triangleCount = countTrianglesOnForwardGraph(forwardNeighbors);

    auto end = std::chrono::high_resolution_clock::now();
    result.durationMs = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    history_triangle_logger.info("Triangle count completed: " + std::to_string(result.triangleCount) +
                               " triangles found in " + std::to_string(result.durationMs) + "ms");

    return result;
}

std::pair<uint64_t, uint32_t> HistoryTriangles::countTrianglesOnMergedGraph(
    const std::vector<TemporalStore::EdgeKey>& allEdges) {

    if (allEdges.empty()) {
        return {0, 0};
    }

    std::unordered_map<std::string, uint32_t> nodeToIndex;
    std::vector<uint64_t> encodedUndirectedEdges;
    uint32_t nodeCount = 0;

    auto getOrAddNode = [&](const std::string& node) -> uint32_t {
        auto it = nodeToIndex.find(node);
        if (it != nodeToIndex.end()) {
            return it->second;
        }
        uint32_t idx = nodeCount++;
        nodeToIndex.emplace(node, idx);
        return idx;
    };

    for (const auto& edge : allEdges) {
        if (edge.sourceId == edge.destId) {
            continue;
        }
        uint32_t sourceIndex = getOrAddNode(edge.sourceId);
        uint32_t destIndex = getOrAddNode(edge.destId);
        encodedUndirectedEdges.push_back(encodeUndirectedEdge(sourceIndex, destIndex));
    }

    if (encodedUndirectedEdges.empty()) {
        return {0, nodeCount};
    }

    std::sort(encodedUndirectedEdges.begin(), encodedUndirectedEdges.end());
    encodedUndirectedEdges.erase(
        std::unique(encodedUndirectedEdges.begin(), encodedUndirectedEdges.end()),
        encodedUndirectedEdges.end());

    nodeToIndex.clear();
    nodeToIndex.rehash(0);

    std::vector<uint32_t> degree(nodeCount, 0);
    for (uint64_t encoded : encodedUndirectedEdges) {
        uint32_t sourceIndex = decodeSourceIndex(encoded);
        uint32_t destIndex = decodeDestIndex(encoded);
        degree[sourceIndex]++;
        degree[destIndex]++;
    }

    std::vector<uint32_t> forwardDegree(nodeCount, 0);
    for (uint64_t encoded : encodedUndirectedEdges) {
        uint32_t sourceIndex = decodeSourceIndex(encoded);
        uint32_t destIndex = decodeDestIndex(encoded);

        bool sourceBeforeDest = (degree[sourceIndex] < degree[destIndex]) ||
                                (degree[sourceIndex] == degree[destIndex] &&
                                 sourceIndex < destIndex);
        if (sourceBeforeDest) {
            forwardDegree[sourceIndex]++;
        } else {
            forwardDegree[destIndex]++;
        }
    }

    std::vector<std::vector<uint32_t>> forwardNeighbors(nodeCount);
    for (uint32_t nodeIndex = 0; nodeIndex < nodeCount; ++nodeIndex) {
        forwardNeighbors[nodeIndex].reserve(forwardDegree[nodeIndex]);
    }

    for (uint64_t encoded : encodedUndirectedEdges) {
        uint32_t sourceIndex = decodeSourceIndex(encoded);
        uint32_t destIndex = decodeDestIndex(encoded);

        bool sourceBeforeDest = (degree[sourceIndex] < degree[destIndex]) ||
                                (degree[sourceIndex] == degree[destIndex] &&
                                 sourceIndex < destIndex);
        if (sourceBeforeDest) {
            forwardNeighbors[sourceIndex].push_back(destIndex);
        } else {
            forwardNeighbors[destIndex].push_back(sourceIndex);
        }
    }

    for (auto& neighbors : forwardNeighbors) {
        std::sort(neighbors.begin(), neighbors.end());
    }

    encodedUndirectedEdges.clear();
    encodedUndirectedEdges.shrink_to_fit();

    uint64_t triangleCount = countTrianglesOnForwardGraph(forwardNeighbors);

    return {triangleCount, nodeCount};
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
