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

#ifndef JASMINEGRAPH_HISTORYPAGERANK_H
#define JASMINEGRAPH_HISTORYPAGERANK_H

#include <map>
#include <string>
#include <vector>
#include <unordered_map>
#include <chrono>
#include <utility>

#include "../../../temporalstore/TemporalStore.h"
#include "../../../temporalstore/TemporalStorePersistence.h"
#include "../../../util/logger/Logger.h"

// Result structure for temporal PageRank computation
struct HistoryPageRankResult {
    // Top-K nodes sorted by PageRank score (descending)
    std::vector<std::pair<std::string, double>> rankedNodes;
    size_t totalEdges;
    size_t localEdges;
    size_t centralEdges;
    uint32_t totalNodes;
    int partitionsProcessed;
    int iterations;
    long durationMs;
};

/**
 * HistoryPageRank: PageRank computation on temporal graph snapshots
 *
 * Loads edges from all local partition stores and the central store at a given
 * snapshot ID, merges them into a single directed graph, and runs the iterative
 * PageRank algorithm (damping factor d, N iterations).
 *
 * Algorithm:
 * 1. Load edges from all local partition snapshots at given snapshot ID
 * 2. Load edges from central store snapshot (cross-partition edges)
 * 3. Build directed out-adjacency list on the merged graph
 * 4. Run iterative PageRank:
 *      PR_new(v) = (1-d)/N + d * sum_{u->v} PR(u) / out_degree(u)
 *    Dangling-node mass (nodes with no outgoing edges) is redistributed
 *    uniformly across all nodes at each iteration.
 * 5. Return top-K nodes sorted by score descending
 */
class HistoryPageRank {
 public:
    /**
     * Compute PageRank at a specific snapshot across all partitions
     *
     * @param graphId        Graph ID
     * @param snapshotId     Snapshot ID to query
     * @param snapshotDir    Directory containing temporal snapshot files
     * @param topK           Number of top-ranked nodes to return (0 = all)
     * @param iterations     Number of PageRank iterations
     * @param dampingFactor  Damping factor d (typically 0.85)
     * @param timeThreshold  Time threshold for snapshot creation (seconds)
     * @param edgeThreshold  Edge count threshold for snapshot creation
     * @return HistoryPageRankResult containing ranked nodes and metadata
     */
    static HistoryPageRankResult computePageRankAtSnapshot(
        int graphId,
        uint32_t snapshotId,
        const std::string& snapshotDir,
        int topK = 10,
        int iterations = 10,
        double dampingFactor = 0.85,
        uint64_t timeThreshold = 60,
        uint64_t edgeThreshold = 10000);

    /**
     * Run iterative PageRank on a set of (directed) edges.
     *
     * @param allEdges      All directed edges from merged multi-partition graph
     * @param topK          Number of top-ranked nodes to return (0 = all)
     * @param iterations    Number of PageRank iterations
     * @param dampingFactor Damping factor d
     * @return Vector of (nodeId, score) pairs sorted by score descending
     */
    static std::vector<std::pair<std::string, double>> computePageRankOnMergedGraph(
        const std::vector<TemporalStore::EdgeKey>& allEdges,
        int topK = 10,
        int iterations = 10,
        double dampingFactor = 0.85,
        int* actualIterationsOut = nullptr);
};

#endif  // JASMINEGRAPH_HISTORYPAGERANK_H
