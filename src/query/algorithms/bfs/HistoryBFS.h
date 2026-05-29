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

#ifndef JASMINEGRAPH_HISTORYBFS_H
#define JASMINEGRAPH_HISTORYBFS_H

#include <chrono>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "../../../temporalstore/TemporalStore.h"

struct HistoryBFSResult {
    std::string outputPath;
    size_t visitedNodes;
    size_t totalEdges;
    uint32_t totalNodes;
    int partitionsProcessed;
    long durationMs;
};

class HistoryBFS {
 public:
    static HistoryBFSResult runBFSAtSnapshot(
        int graphId,
        uint32_t snapshotId,
        const std::string& sourceNode,
        int maxDepth,
        const std::string& snapshotDir,
        uint64_t timeThreshold = 60,
        uint64_t edgeThreshold = 10000);
};

#endif  // JASMINEGRAPH_HISTORYBFS_H
