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

#include "HistoryPageRank.h"

#include <algorithm>
#include <unordered_set>
#include <numeric>
#include <sstream>

Logger history_pagerank_logger;

HistoryPageRankResult HistoryPageRank::computePageRankAtSnapshot(
    int graphId,
    uint32_t snapshotId,
    const std::string& snapshotDir,
    int topK,
    int iterations,
    double dampingFactor,
    uint64_t timeThreshold,
    uint64_t edgeThreshold) {

    auto start = std::chrono::high_resolution_clock::now();

    HistoryPageRankResult result{};
    result.iterations = 0;  // will be set to actual iterations used after convergence

    std::vector<TemporalStore::EdgeKey> allEdges;
    int partitionsProcessed = 0;

    // -----------------------------------------------------------------------
    // Phase 1: Load edges from LOCAL partition snapshots
    // -----------------------------------------------------------------------
    history_pagerank_logger.info("Loading local partition snapshots for graph " +
                                 std::to_string(graphId) + " at snapshot " +
                                 std::to_string(snapshotId));

    int maxPartitionFound = -1;
    for (int partitionId = 0; partitionId < 100; partitionId++) {
        std::string filePath = TemporalStorePersistence::generateBitmapFilePath(
            snapshotDir, graphId, partitionId);

        TemporalStore localStore(graphId, partitionId, timeThreshold, edgeThreshold,
                                 SnapshotManager::SnapshotMode::HYBRID);

        if (localStore.loadBitmapIndexFromDisk(filePath)) {
            partitionsProcessed++;
            if (partitionId > maxPartitionFound) maxPartitionFound = partitionId;

            auto edgesAtSnapshot = localStore.getEdgesAtSnapshot(snapshotId);
            allEdges.insert(allEdges.end(), edgesAtSnapshot.begin(), edgesAtSnapshot.end());

            history_pagerank_logger.info("Loaded local partition " + std::to_string(partitionId) +
                                         ": " + std::to_string(edgesAtSnapshot.size()) + " edges");
        } else if (partitionsProcessed > 0 && partitionId > maxPartitionFound + 5) {
            break;
        }
    }

    result.localEdges = allEdges.size();
    result.partitionsProcessed = partitionsProcessed;

    // -----------------------------------------------------------------------
    // Phase 2: Load edges from CENTRAL store snapshot(s)
    // -----------------------------------------------------------------------
    history_pagerank_logger.info("Loading central store snapshot(s) for graph " +
                                 std::to_string(graphId));

    int centralStart = (partitionsProcessed > 0) ? partitionsProcessed : 1;
    size_t centralEdgesLoaded = 0;
    for (int centralId = centralStart; centralId < centralStart + 20; centralId++) {
        std::string filePath = TemporalStorePersistence::generateBitmapFilePath(
            snapshotDir, graphId, centralId);

        TemporalStore centralStore(graphId, centralId, timeThreshold, edgeThreshold,
                                   SnapshotManager::SnapshotMode::HYBRID);

        if (centralStore.loadBitmapIndexFromDisk(filePath)) {
            auto edgesAtSnapshot = centralStore.getEdgesAtSnapshot(snapshotId);
            allEdges.insert(allEdges.end(), edgesAtSnapshot.begin(), edgesAtSnapshot.end());
            centralEdgesLoaded += edgesAtSnapshot.size();

            history_pagerank_logger.info("Loaded central store (partition " +
                                         std::to_string(centralId) + "): " +
                                         std::to_string(edgesAtSnapshot.size()) + " edges");
        }
    }

    result.centralEdges = centralEdgesLoaded;
    result.totalEdges = allEdges.size();

    if (partitionsProcessed == 0) {
        history_pagerank_logger.error("No snapshot files found for graph " +
                                      std::to_string(graphId) + " at snapshot " +
                                      std::to_string(snapshotId));
        return result;
    }

    // -----------------------------------------------------------------------
    // Phase 3: Compute PageRank on merged graph
    // -----------------------------------------------------------------------
    history_pagerank_logger.info("Computing PageRank on merged graph with " +
                                  std::to_string(allEdges.size()) + " edges, " +
                                  std::to_string(iterations) + " iterations, " +
                                  "d=" + std::to_string(dampingFactor));

    result.rankedNodes = computePageRankOnMergedGraph(allEdges, topK, iterations, dampingFactor,
                                                        &result.iterations);

    // Count unique nodes across all edges
    {
        std::unordered_set<std::string> nodeSet;
        for (const auto& e : allEdges) {
            nodeSet.insert(e.sourceId);
            nodeSet.insert(e.destId);
        }
        result.totalNodes = static_cast<uint32_t>(nodeSet.size());
    }

    auto end = std::chrono::high_resolution_clock::now();
    result.durationMs = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    history_pagerank_logger.info("PageRank completed: " +
                                  std::to_string(result.rankedNodes.size()) +
                                  " ranked nodes in " + std::to_string(result.durationMs) + "ms");

    return result;
}

std::vector<std::pair<std::string, double>> HistoryPageRank::computePageRankOnMergedGraph(
    const std::vector<TemporalStore::EdgeKey>& allEdges,
    int topK,
    int iterations,
    double dampingFactor,
    int* actualIterationsOut) {

    if (allEdges.empty()) {
        return {};
    }

    // -----------------------------------------------------------------------
    // Build node index and directed out-adjacency list
    // -----------------------------------------------------------------------
    // Collect all unique nodes
    std::unordered_map<std::string, uint32_t> nodeToIndex;
    std::vector<std::string> indexToNode;

    auto getOrAdd = [&](const std::string& id) -> uint32_t {
        auto it = nodeToIndex.find(id);
        if (it == nodeToIndex.end()) {
            uint32_t idx = static_cast<uint32_t>(indexToNode.size());
            nodeToIndex[id] = idx;
            indexToNode.push_back(id);
            return idx;
        }
        return it->second;
    };

    // Collect unique nodes first (needed before getOrAdd can assign stable indices).
    // Self-loops are skipped as they carry no structural PageRank information.
    // Edges that cross partition boundaries are stored in multiple partition files,
    // so we must deduplicate (u,v) pairs to match NetworkX DiGraph semantics.

    // First pass: register all node IDs
    for (const auto& e : allEdges) {
        if (e.sourceId == e.destId) continue;
        getOrAdd(e.sourceId);
        getOrAdd(e.destId);
    }

    const uint32_t N = static_cast<uint32_t>(indexToNode.size());
    if (N == 0) return {};

    // Build out-adjacency list and out-degree — deduplicate via seen-set
    std::vector<std::vector<uint32_t>> outAdj(N);
    std::vector<uint32_t> outDegree(N, 0);

    // Use a flat hash set of encoded 64-bit edge keys for deduplication
    struct PairHash {
        std::size_t operator()(std::pair<uint32_t,uint32_t> p) const noexcept {
            return std::hash<uint64_t>{}((static_cast<uint64_t>(p.first) << 32) | p.second);
        }
    };
    std::unordered_set<std::pair<uint32_t,uint32_t>, PairHash> seenEdges;
    seenEdges.reserve(allEdges.size());

    for (const auto& e : allEdges) {
        if (e.sourceId == e.destId) continue;
        uint32_t u = nodeToIndex.at(e.sourceId);
        uint32_t v = nodeToIndex.at(e.destId);
        if (seenEdges.emplace(u, v).second) {   // only first occurrence
            outAdj[u].push_back(v);
            outDegree[u]++;
        }
    }

    if (seenEdges.empty()) return {};

    // -----------------------------------------------------------------------
    // Iterative PageRank (matches NetworkX _pagerank_scipy formula exactly)
    //   PR_new(v) = (1-d)/N + d * [ dangling_mass/N + sum_{u->v} PR(u)/out(u) ]
    // Convergence: stop when L1 norm of change < N * tol  (same as NetworkX)
    // -----------------------------------------------------------------------
    const double base  = (1.0 - dampingFactor) / static_cast<double>(N);
    const double inv_N = 1.0 / static_cast<double>(N);
    const double tol   = 1.0e-6;   // same default tolerance as NetworkX

    std::vector<double> pr(N, inv_N);   // initialise uniformly
    std::vector<double> pr_new(N, 0.0);

    int actualIter = 0;
    for (int iter = 0; iter < iterations; iter++) {
        actualIter = iter + 1;

        // Accumulate dangling-node mass (nodes with no outgoing edges)
        double danglingMass = 0.0;
        for (uint32_t u = 0; u < N; u++) {
            if (outDegree[u] == 0) {
                danglingMass += pr[u];
            }
        }
        double danglingContrib = dampingFactor * danglingMass * inv_N;

        // Reset pr_new
        std::fill(pr_new.begin(), pr_new.end(), base + danglingContrib);

        // Distribute PR from nodes with outgoing edges
        for (uint32_t u = 0; u < N; u++) {
            if (outDegree[u] == 0) continue;
            double share = dampingFactor * pr[u] / static_cast<double>(outDegree[u]);
            for (uint32_t v : outAdj[u]) {
                pr_new[v] += share;
            }
        }

        // Check L1-norm convergence (NetworkX criterion: err < N * tol)
        double err = 0.0;
        for (uint32_t i = 0; i < N; i++) {
            err += std::abs(pr_new[i] - pr[i]);
        }

        std::swap(pr, pr_new);

        if (err < static_cast<double>(N) * tol) {
            break;   // converged
        }
    }

    if (actualIterationsOut) *actualIterationsOut = actualIter;

    // -----------------------------------------------------------------------
    // Collect and sort results
    // -----------------------------------------------------------------------
    std::vector<std::pair<std::string, double>> result;
    result.reserve(N);
    for (uint32_t i = 0; i < N; i++) {
        result.emplace_back(indexToNode[i], pr[i]);
    }

    std::sort(result.begin(), result.end(),
              [](const std::pair<std::string, double>& a,
                 const std::pair<std::string, double>& b) {
                  return a.second > b.second;
              });

    if (topK > 0 && static_cast<size_t>(topK) < result.size()) {
        result.resize(static_cast<size_t>(topK));
    }

    return result;
}
