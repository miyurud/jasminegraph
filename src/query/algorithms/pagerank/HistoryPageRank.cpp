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
#include <cmath>
#include <cstdint>
#include <unordered_set>
#include <numeric>
#include <sstream>
#include <set>

namespace {

std::vector<TemporalStore::EdgeKey> collectCumulativeEdgesUpToSnapshot(TemporalStore& store,
                                                                       uint32_t snapshotId) {
    std::unordered_set<TemporalStore::EdgeKey, TemporalStore::EdgeKey::Hash> cumulativeEdgeSet;

    for (uint32_t sid = 0; sid <= snapshotId; sid++) {
        auto edgesAtSnapshot = store.getEdgesAtSnapshot(sid);
        cumulativeEdgeSet.insert(edgesAtSnapshot.begin(), edgesAtSnapshot.end());
    }

    return std::vector<TemporalStore::EdgeKey>(cumulativeEdgeSet.begin(), cumulativeEdgeSet.end());
}

uint64_t encodeEdgeKey(uint32_t sourceIndex, uint32_t destIndex) {
    return (static_cast<uint64_t>(sourceIndex) << 32) | static_cast<uint64_t>(destIndex);
}

uint32_t decodeSourceIndex(uint64_t encoded) {
    return static_cast<uint32_t>(encoded >> 32);
}

uint32_t decodeDestIndex(uint64_t encoded) {
    return static_cast<uint32_t>(encoded & 0xffffffffULL);
}

}  // namespace

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

    (void)timeThreshold;
    (void)edgeThreshold;

    auto start = std::chrono::high_resolution_clock::now();

    HistoryPageRankResult result{};
    result.iterations = 0;  // will be set to actual iterations used after convergence
    int partitionsProcessed = 0;
    std::unordered_map<std::string, uint32_t> nodeToIndex;
    std::vector<std::string> indexToNode;
    std::vector<uint32_t> outDegree;
    std::unordered_set<uint64_t> uniqueEdges;

    auto getOrAddNode = [&](const std::string& node) -> uint32_t {
        auto it = nodeToIndex.find(node);
        if (it != nodeToIndex.end()) {
            return it->second;
        }
        uint32_t idx = static_cast<uint32_t>(indexToNode.size());
        nodeToIndex.emplace(node, idx);
        indexToNode.push_back(node);
        outDegree.push_back(0);
        return idx;
    };

    // -----------------------------------------------------------------------
    // Phase 1: Stream active edges and build a deduplicated edge set.
    // -----------------------------------------------------------------------
    history_pagerank_logger.info("Streaming snapshot edges for graph " +
                                 std::to_string(graphId) + " at snapshot " +
                                 std::to_string(snapshotId));

    int maxPartitionFound = -1;
    for (int partitionId = 0; partitionId < 100; partitionId++) {
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
                uint64_t encoded = encodeEdgeKey(sourceIndex, destIndex);
                if (uniqueEdges.insert(encoded).second) {
                    outDegree[sourceIndex]++;
                }
                return true;
            });

        if (loaded) {
            partitionsProcessed++;
            if (partitionId > maxPartitionFound) maxPartitionFound = partitionId;
        } else if (partitionsProcessed > 0 && partitionId > maxPartitionFound + 5) {
            break;
        }
    }

    result.localEdges = uniqueEdges.size();
    result.partitionsProcessed = partitionsProcessed;

    // -----------------------------------------------------------------------
    // Phase 2: no separate central-store pass is needed; the bitmap index files
    // already contain cumulative edges for the snapshot.
    // -----------------------------------------------------------------------
    result.centralEdges = 0;
    result.totalEdges = uniqueEdges.size();
    result.totalNodes = static_cast<uint32_t>(indexToNode.size());

    if (result.totalNodes == 0) {
        history_pagerank_logger.error("No active nodes found for graph " +
                                      std::to_string(graphId) + " at snapshot " +
                                      std::to_string(snapshotId));
        return result;
    }

    if (partitionsProcessed == 0) {
        history_pagerank_logger.error("No snapshot files found for graph " +
                                      std::to_string(graphId) + " at snapshot " +
                                      std::to_string(snapshotId));
        return result;
    }

    history_pagerank_logger.info("Computing PageRank on merged graph with " +
                                 std::to_string(result.totalEdges) + " unique edges, " +
                                 std::to_string(iterations) + " iterations, " +
                                 "d=" + std::to_string(dampingFactor));

    // -----------------------------------------------------------------------
    // Phase 3: Compute PageRank using pull-style iterations over the unique edge set.
    // -----------------------------------------------------------------------
    std::vector<double> pr(result.totalNodes, result.totalNodes > 0 ? 1.0 / result.totalNodes : 0.0);
    std::vector<double> prNext(result.totalNodes, 0.0);
    const double base = (result.totalNodes > 0) ? (1.0 - dampingFactor) / static_cast<double>(result.totalNodes) : 0.0;
    const double invNodeCount = (result.totalNodes > 0) ? 1.0 / static_cast<double>(result.totalNodes) : 0.0;
    const double tol = 1.0e-6;

    int actualIter = 0;
    for (int iter = 0; iter < iterations; ++iter) {
        actualIter = iter + 1;

        double danglingMass = 0.0;
        for (uint32_t nodeIndex = 0; nodeIndex < result.totalNodes; ++nodeIndex) {
            if (outDegree[nodeIndex] == 0) {
                danglingMass += pr[nodeIndex];
            }
        }

        double danglingContribution = dampingFactor * danglingMass * invNodeCount;
        std::fill(prNext.begin(), prNext.end(), base + danglingContribution);

        for (uint64_t encoded : uniqueEdges) {
            uint32_t sourceIndex = decodeSourceIndex(encoded);
            uint32_t destIndex = decodeDestIndex(encoded);
            if (outDegree[sourceIndex] == 0) {
                continue;
            }
            prNext[destIndex] += dampingFactor * pr[sourceIndex] / static_cast<double>(outDegree[sourceIndex]);
        }

        double err = 0.0;
        for (uint32_t nodeIndex = 0; nodeIndex < result.totalNodes; ++nodeIndex) {
            err += std::abs(prNext[nodeIndex] - pr[nodeIndex]);
        }

        pr.swap(prNext);
        if (err < static_cast<double>(result.totalNodes) * tol) {
            break;
        }
    }

    result.iterations = actualIter;

    std::vector<std::pair<std::string, double>> ranked;
    ranked.reserve(result.totalNodes);
    for (uint32_t nodeIndex = 0; nodeIndex < result.totalNodes; ++nodeIndex) {
        ranked.emplace_back(indexToNode[nodeIndex], pr[nodeIndex]);
    }

    std::sort(ranked.begin(), ranked.end(),
              [](const std::pair<std::string, double>& a,
                 const std::pair<std::string, double>& b) {
                  return a.second > b.second;
              });

    if (topK > 0 && static_cast<size_t>(topK) < ranked.size()) {
        ranked.resize(static_cast<size_t>(topK));
    }

    result.rankedNodes = std::move(ranked);

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
        if (actualIterationsOut) {
            *actualIterationsOut = 0;
        }
        return {};
    }

    std::unordered_map<std::string, uint32_t> nodeToIndex;
    std::vector<std::string> indexToNode;
    std::vector<uint32_t> outDegree;
    std::unordered_set<uint64_t> uniqueEdges;

    auto getOrAddNode = [&](const std::string& node) -> uint32_t {
        auto it = nodeToIndex.find(node);
        if (it != nodeToIndex.end()) {
            return it->second;
        }
        uint32_t idx = static_cast<uint32_t>(indexToNode.size());
        nodeToIndex.emplace(node, idx);
        indexToNode.push_back(node);
        outDegree.push_back(0);
        return idx;
    };

    for (const auto& edge : allEdges) {
        if (edge.sourceId == edge.destId) {
            continue;
        }
        uint32_t sourceIndex = getOrAddNode(edge.sourceId);
        uint32_t destIndex = getOrAddNode(edge.destId);
        uint64_t encoded = encodeEdgeKey(sourceIndex, destIndex);
        if (uniqueEdges.insert(encoded).second) {
            outDegree[sourceIndex]++;
        }
    }

    const uint32_t nodeCount = static_cast<uint32_t>(indexToNode.size());
    if (nodeCount == 0 || uniqueEdges.empty()) {
        if (actualIterationsOut) {
            *actualIterationsOut = 0;
        }
        return {};
    }

    std::vector<double> pr(nodeCount, 1.0 / static_cast<double>(nodeCount));
    std::vector<double> prNext(nodeCount, 0.0);
    const double base = (1.0 - dampingFactor) / static_cast<double>(nodeCount);
    const double invNodeCount = 1.0 / static_cast<double>(nodeCount);
    const double tol = 1.0e-6;

    int actualIter = 0;
    for (int iter = 0; iter < iterations; ++iter) {
        actualIter = iter + 1;

        double danglingMass = 0.0;
        for (uint32_t nodeIndex = 0; nodeIndex < nodeCount; ++nodeIndex) {
            if (outDegree[nodeIndex] == 0) {
                danglingMass += pr[nodeIndex];
            }
        }

        double danglingContribution = dampingFactor * danglingMass * invNodeCount;
        std::fill(prNext.begin(), prNext.end(), base + danglingContribution);

        for (uint64_t encoded : uniqueEdges) {
            uint32_t sourceIndex = decodeSourceIndex(encoded);
            uint32_t destIndex = decodeDestIndex(encoded);
            if (outDegree[sourceIndex] == 0) {
                continue;
            }
            prNext[destIndex] += dampingFactor * pr[sourceIndex] / static_cast<double>(outDegree[sourceIndex]);
        }

        double err = 0.0;
        for (uint32_t nodeIndex = 0; nodeIndex < nodeCount; ++nodeIndex) {
            err += std::abs(prNext[nodeIndex] - pr[nodeIndex]);
        }

        pr.swap(prNext);
        if (err < static_cast<double>(nodeCount) * tol) {
            break;
        }
    }

    if (actualIterationsOut) {
        *actualIterationsOut = actualIter;
    }

    std::vector<std::pair<std::string, double>> result;
    result.reserve(nodeCount);
    for (uint32_t nodeIndex = 0; nodeIndex < nodeCount; ++nodeIndex) {
        result.emplace_back(indexToNode[nodeIndex], pr[nodeIndex]);
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
