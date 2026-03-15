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

#ifndef JASMINEGRAPH_HISTORYTRIANGLES_H
#define JASMINEGRAPH_HISTORYTRIANGLES_H

#include <map>
#include <string>
#include <vector>
#include <set>
#include <chrono>

#include "../../../temporalstore/TemporalStore.h"
#include "../../../temporalstore/TemporalStorePersistence.h"
#include "../../../util/logger/Logger.h"
#include <roaring/roaring.h>

// Result structure for temporal triangle counting
struct TemporalTriangleResult {
    uint64_t triangleCount;
    size_t totalEdges;
    size_t localEdges;
    size_t centralEdges;
    uint32_t uniqueNodes;
    int partitionsProcessed;
    long durationMs;
};

/**
 * HistoryTriangles: Triangle counting for temporal graph snapshots
 * 
 * This class implements efficient triangle counting on historical snapshots
 * of streaming graphs stored in temporal storage. It merges edges from both
 * local partition stores and central stores (cross-partition edges) to count
 * ALL triangles including those spanning multiple partitions.
 * 
 * Algorithm:
 * 1. Load edges from all local partition snapshots at given snapshot ID
 * 2. Load edges from central store snapshot (cross-partition edges)
 * 3. Build merged adjacency map using Roaring bitmaps for efficiency
 * 4. Count triangles using SIMD-optimized bitmap intersections
 * 5. Return complete triangle count on merged graph
 */
class HistoryTriangles {
 public:
    /**
     * Count triangles at a specific snapshot across all partitions
     * 
     * @param graphId Graph ID
     * @param snapshotId Snapshot ID to query
     * @param snapshotDir Directory containing temporal snapshot files
     * @param timeThreshold Time threshold for snapshot creation (seconds)
     * @param edgeThreshold Edge count threshold for snapshot creation
     * @return TemporalTriangleResult containing triangle count and metadata
     */
    static TemporalTriangleResult countTrianglesAtSnapshot(
        int graphId,
        uint32_t snapshotId,
        const std::string& snapshotDir,
        uint64_t timeThreshold = 60,
        uint64_t edgeThreshold = 10000);

    /**
     * Count triangles using merged adjacency from all stores
     * This is the core algorithm that handles the actual triangle counting
     * 
     * @param allEdges Vector of all edges from local + central stores
     * @return Pair of (triangle count, unique node count)
     */
    static std::pair<uint64_t, uint32_t> countTrianglesOnMergedGraph(
        const std::vector<TemporalStore::EdgeKey>& allEdges);

 private:
    /**
     * Build node index mapping: string nodeId -> uint32_t index
     * This enables efficient Roaring bitmap operations
     * 
     * @param edges Vector of edges
     * @param nodeToIndex Output map: nodeId -> index
     * @param indexToNode Output vector: index -> nodeId
     * @return Number of unique nodes
     */
    static uint32_t buildNodeIndex(
        const std::vector<TemporalStore::EdgeKey>& edges,
        std::map<std::string, uint32_t>& nodeToIndex,
        std::vector<std::string>& indexToNode);

    /**
     * Build adjacency map using Roaring bitmaps
     * 
     * @param edges Vector of edges
     * @param nodeToIndex Map from nodeId to index
     * @return Map from node index to neighbor bitmap
     */
    static std::map<uint32_t, roaring_bitmap_t*> buildAdjacencyBitmaps(
        const std::vector<TemporalStore::EdgeKey>& edges,
        const std::map<std::string, uint32_t>& nodeToIndex);

    /**
     * Count triangles using bitmap intersection (SIMD-optimized)
     * 
     * @param edges Vector of edges
     * @param nodeToIndex Map from nodeId to index
     * @param neighbors Adjacency bitmap map
     * @return Raw triangle count (counted 3x, needs division by 3)
     */
    static uint64_t countTrianglesFromBitmaps(
        const std::vector<TemporalStore::EdgeKey>& edges,
        const std::map<std::string, uint32_t>& nodeToIndex,
        const std::map<uint32_t, roaring_bitmap_t*>& neighbors);
};

#endif  // JASMINEGRAPH_HISTORYTRIANGLES_H
