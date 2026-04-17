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

#include "HistoryBFS.h"

#include <algorithm>
#include <cctype>
#include <queue>
#include <set>
#include <unordered_map>
#include <unordered_set>

#include "../../../temporalstore/TemporalStorePersistence.h"
#include "../../../util/Utils.h"
#include "../../../util/logger/Logger.h"

namespace {

Logger history_bfs_logger;

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

HistoryBFSResult HistoryBFS::runBFSAtSnapshot(
    int graphId,
    uint32_t snapshotId,
    const std::string& sourceNode,
    int maxDepth,
    const std::string& snapshotDir,
    uint64_t timeThreshold,
    uint64_t edgeThreshold) {

    auto start = std::chrono::high_resolution_clock::now();
    HistoryBFSResult result{};

    std::vector<TemporalStore::EdgeKey> allEdges;

    std::vector<uint32_t> partitionIds = discoverBitmapPartitions(snapshotDir, graphId);
    if (partitionIds.empty()) {
        return result;
    }

    for (uint32_t partitionId : partitionIds) {
        std::string filePath = TemporalStorePersistence::generateBitmapFilePath(
            snapshotDir, graphId, partitionId);

        TemporalStore store(graphId, partitionId, timeThreshold, edgeThreshold,
                            SnapshotManager::SnapshotMode::HYBRID);

        if (!store.loadBitmapIndexFromDisk(filePath)) {
            continue;
        }

        result.partitionsProcessed++;
        auto cumulativeEdges = collectCumulativeEdgesUpToSnapshot(store, snapshotId);
        allEdges.insert(allEdges.end(), cumulativeEdges.begin(), cumulativeEdges.end());
    }

    if (result.partitionsProcessed == 0) {
        return result;
    }

    {
        std::set<std::pair<std::string, std::string>> uniqueRawDirectedEdges;
        for (const auto& edge : allEdges) {
            uniqueRawDirectedEdges.insert({edge.sourceId, edge.destId});
        }
        result.rawEdges = uniqueRawDirectedEdges.size();

    }

    std::unordered_map<std::string, uint32_t> nodeToIndex;
    std::vector<std::string> indexToNode;
    auto getOrAdd = [&](const std::string& node) {
        auto it = nodeToIndex.find(node);
        if (it != nodeToIndex.end()) {
            return it->second;
        }
        uint32_t idx = static_cast<uint32_t>(indexToNode.size());
        nodeToIndex[node] = idx;
        indexToNode.push_back(node);
        return idx;
    };

    for (const auto& edge : allEdges) {
        if (edge.sourceId == edge.destId) {
            continue;
        }
        getOrAdd(edge.sourceId);
        getOrAdd(edge.destId);
    }

    uint32_t sourceIndex = getOrAdd(sourceNode);
    const uint32_t nodeCount = static_cast<uint32_t>(indexToNode.size());
    result.totalNodes = nodeCount;

    std::vector<std::vector<uint32_t>> outAdj(nodeCount);
    struct PairHash {
        std::size_t operator()(const std::pair<uint32_t, uint32_t>& p) const noexcept {
            return std::hash<uint64_t>{}((static_cast<uint64_t>(p.first) << 32) | p.second);
        }
    };
    std::unordered_set<std::pair<uint32_t, uint32_t>, PairHash> uniqueDirectedEdges;
    uniqueDirectedEdges.reserve(allEdges.size());

    for (const auto& edge : allEdges) {
        if (edge.sourceId == edge.destId) {
            continue;
        }
        uint32_t u = nodeToIndex.at(edge.sourceId);
        uint32_t v = nodeToIndex.at(edge.destId);
        if (uniqueDirectedEdges.emplace(u, v).second) {
            outAdj[u].push_back(v);
        }
    }

    result.totalEdges = uniqueDirectedEdges.size();

    std::vector<int> distance(nodeCount, -1);
    std::queue<uint32_t> bfsQueue;

    distance[sourceIndex] = 0;
    bfsQueue.push(sourceIndex);

    while (!bfsQueue.empty()) {
        uint32_t u = bfsQueue.front();
        bfsQueue.pop();

        int currentDepth = distance[u];
        result.traversalOrder.push_back({indexToNode[u], currentDepth});

        if (maxDepth > 0 && currentDepth >= maxDepth) {
            continue;
        }

        for (uint32_t v : outAdj[u]) {
            if (distance[v] != -1) {
                continue;
            }
            distance[v] = currentDepth + 1;
            bfsQueue.push(v);
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    result.durationMs = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    history_bfs_logger.info("History BFS completed: graph " + std::to_string(graphId) +
                            " snapshot " + std::to_string(snapshotId) +
                            " visited " + std::to_string(result.traversalOrder.size()) + " nodes");

    return result;
}
