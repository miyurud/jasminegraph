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
#include <fstream>
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

uint64_t encodeEdgeKey(uint32_t sourceIndex, uint32_t destIndex) {
    return (static_cast<uint64_t>(sourceIndex) << 32) | static_cast<uint64_t>(destIndex);
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

    (void)timeThreshold;
    (void)edgeThreshold;

    auto start = std::chrono::high_resolution_clock::now();
    HistoryBFSResult result{};

    std::vector<uint32_t> partitionIds = discoverBitmapPartitions(snapshotDir, graphId);
    if (partitionIds.empty()) {
        return result;
    }

    std::unordered_map<std::string, uint32_t> nodeToIndex;
    std::vector<std::string> indexToNode;
    indexToNode.reserve(1024);

    auto getOrAddNode = [&](const std::string& node) -> uint32_t {
        auto it = nodeToIndex.find(node);
        if (it != nodeToIndex.end()) {
            return it->second;
        }
        uint32_t idx = static_cast<uint32_t>(indexToNode.size());
        nodeToIndex.emplace(node, idx);
        indexToNode.push_back(node);
        return idx;
    };

    getOrAddNode(sourceNode);

    std::unordered_set<uint64_t> uniqueEdges;
    uniqueEdges.reserve(1024);

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
                uint64_t encodedKey = encodeEdgeKey(sourceIndex, destIndex);
                uniqueEdges.insert(encodedKey);
                return true;
            });

        if (loaded) {
            result.partitionsProcessed++;
        }
    }

    if (result.partitionsProcessed == 0) {
        return result;
    }

    uint32_t sourceIndex = getOrAddNode(sourceNode);
    const uint32_t nodeCount = static_cast<uint32_t>(indexToNode.size());
    result.totalNodes = nodeCount;

    result.totalEdges = uniqueEdges.size();

    std::vector<uint8_t> visited(nodeCount, 0);
    std::vector<uint32_t> frontier;
    std::vector<uint32_t> nextFrontier;
    frontier.push_back(sourceIndex);
    visited[sourceIndex] = 1;

    auto nowEpoch = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    std::string outputPath = "/tmp/jg_hisbfs_graph" + std::to_string(graphId) +
                             "_snapshot" + std::to_string(snapshotId) +
                             "_" + std::to_string(nowEpoch) + ".txt";
    std::ofstream outFile(outputPath, std::ios::trunc);
    if (!outFile.is_open()) {
        return result;
    }

    outFile << "graph_id=" << graphId << "\n";
    outFile << "snapshot_id=" << snapshotId << "\n";
    outFile << "source_node=" << sourceNode << "\n";
    outFile << "max_depth=" << maxDepth << "\n";
    outFile << "visited_nodes=" << 0 << "\n";
    outFile << "total_nodes=" << result.totalNodes << "\n";
    outFile << "raw_edges=" << result.totalEdges << "\n";
    outFile << "duration_ms=" << 0 << "\n";
    outFile << "\nindex\tdepth\tnode\n";

    size_t visitedCount = 0;
    int currentDepth = 0;

    while (!frontier.empty()) {
        for (uint32_t nodeIndex : frontier) {
            outFile << visitedCount << "\t" << currentDepth << "\t" << indexToNode[nodeIndex] << "\n";
            visitedCount++;
        }

        if (maxDepth > 0 && currentDepth >= maxDepth) {
            break;
        }

        std::unordered_set<uint32_t> frontierSet(frontier.begin(), frontier.end());
        nextFrontier.clear();

        for (uint64_t encoded : uniqueEdges) {
            uint32_t source = static_cast<uint32_t>(encoded >> 32);
            uint32_t destination = static_cast<uint32_t>(encoded & 0xffffffffULL);
            if (frontierSet.find(source) == frontierSet.end()) {
                continue;
            }
            if (visited[destination]) {
                continue;
            }
            visited[destination] = 1;
            nextFrontier.push_back(destination);
        }

        frontier.swap(nextFrontier);
        currentDepth++;
    }

    outFile << "\nsummary_visited_nodes=" << visitedCount << "\n";
    outFile << "summary_total_nodes=" << result.totalNodes << "\n";
    outFile << "summary_raw_edges=" << result.totalEdges << "\n";
    outFile.close();

    auto end = std::chrono::high_resolution_clock::now();
    result.durationMs = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    result.visitedNodes = visitedCount;
    result.outputPath = outputPath;

    history_bfs_logger.info("History BFS completed: graph " + std::to_string(graphId) +
                            " snapshot " + std::to_string(snapshotId) +
                            " visited " + std::to_string(result.visitedNodes) + " nodes");

    return result;
}
