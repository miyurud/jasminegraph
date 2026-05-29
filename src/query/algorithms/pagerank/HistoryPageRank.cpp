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
#include <array>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <unordered_set>
#include <numeric>
#include <sstream>
#include <set>

#include "../../../util/Utils.h"

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

std::string createEdgeShardTempDir() {
    char tempDirTemplate[] = "/tmp/jg_histpgr_edges_XXXXXX";
    char* createdDir = mkdtemp(tempDirTemplate);
    if (createdDir == nullptr) {
        return "";
    }
    return std::string(createdDir);
}

std::vector<uint64_t> readEncodedEdgesFromBinaryFile(const std::string& filePath) {
    std::vector<uint64_t> edges;

    std::ifstream in(filePath, std::ios::binary);
    if (!in.is_open()) {
        return edges;
    }

    in.seekg(0, std::ios::end);
    std::streamoff fileSize = in.tellg();
    in.seekg(0, std::ios::beg);

    if (fileSize > 0) {
        edges.reserve(static_cast<size_t>(fileSize / static_cast<std::streamoff>(sizeof(uint64_t))));
    }

    uint64_t encoded = 0;
    while (in.read(reinterpret_cast<char*>(&encoded), sizeof(encoded))) {
        edges.push_back(encoded);
    }

    return edges;
}

template <typename Handler>
void streamEncodedEdgesFromBinaryFile(const std::string& filePath, Handler&& handler) {
    std::ifstream in(filePath, std::ios::binary);
    if (!in.is_open()) {
        return;
    }

    uint64_t encoded = 0;
    while (in.read(reinterpret_cast<char*>(&encoded), sizeof(encoded))) {
        handler(encoded);
    }
}

bool rewriteUniqueEncodedEdgesToBinaryFile(const std::string& filePath,
                                          const std::vector<uint64_t>& edges) {
    std::ofstream out(filePath, std::ios::binary | std::ios::trunc);
    if (!out.is_open()) {
        return false;
    }
    if (!edges.empty()) {
        out.write(reinterpret_cast<const char*>(edges.data()),
                  static_cast<std::streamsize>(edges.size() * sizeof(uint64_t)));
    }
    return static_cast<bool>(out);
}

void cleanupEdgeShardTempDir(const std::string& tempDir) {
    if (!tempDir.empty()) {
        Utils::deleteDirectory(tempDir);
    }
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

struct PageRankCsrGraph {
    std::vector<uint64_t> offsets;
    std::vector<uint32_t> neighbors;
    std::vector<std::string> indexToNode;
    size_t rawEdges{0};
    size_t uniqueEdges{0};
    int partitionsProcessed{0};
};

bool buildPageRankCsrFromSnapshotDir(int graphId,
                                     uint32_t snapshotId,
                                     const std::string& snapshotDir,
                                     PageRankCsrGraph& graph) {
    constexpr size_t EDGE_SHARD_COUNT = 256;
    constexpr size_t SHARD_WRITE_BATCH_EDGES = 4096;

    std::vector<uint32_t> partitionIds = discoverBitmapPartitions(snapshotDir, graphId);
    if (partitionIds.empty()) {
        return false;
    }

    std::string shardTempDir = createEdgeShardTempDir();
    if (shardTempDir.empty()) {
        return false;
    }

    std::array<std::string, EDGE_SHARD_COUNT> shardFilePaths{};
    std::array<std::ofstream, EDGE_SHARD_COUNT> shardWriters;
    std::array<std::vector<uint64_t>, EDGE_SHARD_COUNT> shardWriteBuffers;
    for (size_t shardId = 0; shardId < EDGE_SHARD_COUNT; ++shardId) {
        shardFilePaths[shardId] = shardTempDir + "/edges_" + std::to_string(shardId) + ".bin";
        shardWriters[shardId].open(shardFilePaths[shardId], std::ios::binary | std::ios::trunc);
        if (!shardWriters[shardId].is_open()) {
            cleanupEdgeShardTempDir(shardTempDir);
            return false;
        }
        shardWriteBuffers[shardId].reserve(SHARD_WRITE_BATCH_EDGES);
    }

    auto flushShardBuffer = [&](size_t shardId) -> bool {
        auto& buffer = shardWriteBuffers[shardId];
        if (buffer.empty()) {
            return true;
        }
        shardWriters[shardId].write(reinterpret_cast<const char*>(buffer.data()),
                                    static_cast<std::streamsize>(buffer.size() * sizeof(uint64_t)));
        if (!shardWriters[shardId].good()) {
            return false;
        }
        buffer.clear();
        return true;
    };

    std::unordered_map<std::string, uint32_t> nodeToIndex;
    std::vector<uint32_t> outDegree;
    bool streamFailed = false;
    nodeToIndex.reserve(1024);
    graph.indexToNode.reserve(1024);

    auto getOrAddNode = [&](const std::string& node) -> uint32_t {
        auto it = nodeToIndex.find(node);
        if (it != nodeToIndex.end()) {
            return it->second;
        }
        uint32_t idx = static_cast<uint32_t>(graph.indexToNode.size());
        nodeToIndex.emplace(node, idx);
        graph.indexToNode.push_back(node);
        outDegree.push_back(0);
        return idx;
    };

    for (uint32_t partitionId : partitionIds) {
        std::string filePath = TemporalStorePersistence::generateBitmapFilePath(snapshotDir, graphId, partitionId);
        bool loaded = TemporalStorePersistence::forEachActiveBitmapEdgeAtSnapshot(
            filePath, snapshotId,
            [&](const std::string& sourceId, const std::string& destId) {
                if (sourceId == destId) {
                    return true;
                }

                uint32_t sourceIndex = getOrAddNode(sourceId);
                uint32_t destIndex = getOrAddNode(destId);
                uint64_t encoded = encodeEdgeKey(sourceIndex, destIndex);
                size_t shardId = static_cast<size_t>(encoded & (EDGE_SHARD_COUNT - 1));
                graph.rawEdges++;
                shardWriteBuffers[shardId].push_back(encoded);
                if (shardWriteBuffers[shardId].size() >= SHARD_WRITE_BATCH_EDGES) {
                    if (!flushShardBuffer(shardId)) {
                        streamFailed = true;
                        return false;
                    }
                }
                return true;
            });

        if (streamFailed) {
            cleanupEdgeShardTempDir(shardTempDir);
            return false;
        }

        if (loaded) {
            graph.partitionsProcessed++;
        }
    }

    for (size_t shardId = 0; shardId < EDGE_SHARD_COUNT; ++shardId) {
        if (!flushShardBuffer(shardId)) {
            cleanupEdgeShardTempDir(shardTempDir);
            return false;
        }
        shardWriters[shardId].close();
    }

    if (graph.partitionsProcessed == 0 || graph.indexToNode.empty()) {
        cleanupEdgeShardTempDir(shardTempDir);
        return false;
    }

    outDegree.assign(graph.indexToNode.size(), 0);
    graph.uniqueEdges = 0;

    for (size_t shardId = 0; shardId < EDGE_SHARD_COUNT; ++shardId) {
        std::vector<uint64_t> shardEdges = readEncodedEdgesFromBinaryFile(shardFilePaths[shardId]);
        if (shardEdges.empty()) {
            continue;
        }

        std::sort(shardEdges.begin(), shardEdges.end());
        shardEdges.erase(std::unique(shardEdges.begin(), shardEdges.end()), shardEdges.end());
        graph.uniqueEdges += shardEdges.size();

        if (!rewriteUniqueEncodedEdgesToBinaryFile(shardFilePaths[shardId], shardEdges)) {
            cleanupEdgeShardTempDir(shardTempDir);
            return false;
        }

        for (uint64_t encoded : shardEdges) {
            uint32_t sourceIndex = decodeSourceIndex(encoded);
            if (sourceIndex < outDegree.size()) {
                outDegree[sourceIndex]++;
            }
        }
    }

    graph.offsets.assign(outDegree.size() + 1, 0);
    for (size_t nodeIndex = 0; nodeIndex < outDegree.size(); ++nodeIndex) {
        graph.offsets[nodeIndex + 1] = graph.offsets[nodeIndex] + outDegree[nodeIndex];
    }

    graph.neighbors.assign(static_cast<size_t>(graph.offsets.back()), 0);
    std::fill(outDegree.begin(), outDegree.end(), 0);
    nodeToIndex.clear();
    nodeToIndex.rehash(0);

    for (size_t shardId = 0; shardId < EDGE_SHARD_COUNT; ++shardId) {
        streamEncodedEdgesFromBinaryFile(shardFilePaths[shardId], [&](uint64_t encoded) {
            uint32_t sourceIndex = decodeSourceIndex(encoded);
            uint32_t destIndex = decodeDestIndex(encoded);
            if (sourceIndex >= outDegree.size()) {
                return;
            }
            uint32_t slot = outDegree[sourceIndex]++;
            graph.neighbors[static_cast<size_t>(graph.offsets[sourceIndex] + slot)] = destIndex;
        });
    }

    std::vector<uint32_t>().swap(outDegree);
    cleanupEdgeShardTempDir(shardTempDir);
    return true;
}

std::vector<std::pair<std::string, double>> runPageRankOnCSR(
    const std::vector<uint64_t>& csrOffsets,
    const std::vector<uint32_t>& csrNeighbors,
    const std::vector<std::string>& indexToNode,
    int topK,
    int iterations,
    double dampingFactor,
    int* actualIterationsOut) {

    if (csrOffsets.size() < 2 || csrNeighbors.empty() || indexToNode.empty()) {
        if (actualIterationsOut) {
            *actualIterationsOut = 0;
        }
        return {};
    }

    const uint32_t nodeCount = static_cast<uint32_t>(indexToNode.size());
    std::vector<double> pr(nodeCount, 1.0 / static_cast<double>(nodeCount));
    std::vector<double> prNext(nodeCount, 0.0);
    std::vector<uint32_t> outDegree(nodeCount, 0);

    for (uint32_t nodeIndex = 0; nodeIndex < nodeCount; ++nodeIndex) {
        outDegree[nodeIndex] = static_cast<uint32_t>(csrOffsets[nodeIndex + 1] - csrOffsets[nodeIndex]);
    }

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

        for (uint32_t sourceIndex = 0; sourceIndex < nodeCount; ++sourceIndex) {
            uint64_t begin = csrOffsets[sourceIndex];
            uint64_t end = csrOffsets[sourceIndex + 1];
            if (begin == end || outDegree[sourceIndex] == 0) {
                continue;
            }

            double share = dampingFactor * pr[sourceIndex] / static_cast<double>(outDegree[sourceIndex]);
            for (uint64_t edgeIndex = begin; edgeIndex < end; ++edgeIndex) {
                uint32_t destIndex = csrNeighbors[static_cast<size_t>(edgeIndex)];
                prNext[destIndex] += share;
            }
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

    std::vector<std::pair<std::string, double>> ranked;
    ranked.reserve(nodeCount);
    for (uint32_t nodeIndex = 0; nodeIndex < nodeCount; ++nodeIndex) {
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

    return ranked;
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
    result.iterations = 0;

    PageRankCsrGraph graph;
    if (!buildPageRankCsrFromSnapshotDir(graphId, snapshotId, snapshotDir, graph)) {
        history_pagerank_logger.error("No snapshot files or edges found for graph " +
                                      std::to_string(graphId) + " at snapshot " +
                                      std::to_string(snapshotId));
        return result;
    }

    result.rawEdges = graph.rawEdges;
    result.localEdges = graph.uniqueEdges;
    result.centralEdges = 0;
    result.totalEdges = graph.uniqueEdges;
    result.totalNodes = static_cast<uint32_t>(graph.indexToNode.size());
    result.partitionsProcessed = graph.partitionsProcessed;

    if (result.totalNodes == 0 || result.partitionsProcessed == 0) {
        history_pagerank_logger.error("No active nodes found for graph " +
                                      std::to_string(graphId) + " at snapshot " +
                                      std::to_string(snapshotId));
        return result;
    }

    history_pagerank_logger.info("Computing PageRank on CSR graph with " +
                                 std::to_string(result.totalEdges) + " unique edges, " +
                                 std::to_string(iterations) + " iterations, " +
                                 "d=" + std::to_string(dampingFactor));

    result.rankedNodes = runPageRankOnCSR(graph.offsets, graph.neighbors, graph.indexToNode,
                                          topK, iterations, dampingFactor, &result.iterations);

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
