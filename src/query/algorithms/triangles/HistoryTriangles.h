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

// Result structure for temporal triangle counting
struct TemporalTriangleResult {
    uint64_t triangleCount{0};
    size_t rawEdges{0};
    size_t totalEdges{0};
    size_t uniqueEdges{0};
    size_t localEdges{0};
    size_t centralEdges{0};
    uint32_t uniqueNodes{0};
    int partitionsProcessed{0};
    long durationMs{0};
    long stagingMs{0};
    long loadShardMs{0};
    long dedupMs{0};
    long degreeMs{0};
    long forwardBuildMs{0};
    long sortMs{0};
    long countMs{0};
};

/**
 * HistoryTriangles: Triangle counting for temporal graph snapshots
 *
 * Counts triangles on the cumulative graph formed by all edges that were
 * active at a given snapshotId.  The algorithm uses a degree-ordered forward
 * adjacency list and sorted-merge intersection (nodeIterator approach), with
 * OpenMP parallelisation for large graphs.
 */
class HistoryTriangles {
 public:
    /**
     * Count triangles at a specific snapshot across all partitions.
     * Loads all bitmap-index files from snapshotDir, merges edges, and
     * counts triangles using forward-adjacency + OpenMP.
     *
     * @param graphId     Graph ID
     * @param snapshotId  Target snapshot (inclusive upper bound)
     * @param snapshotDir Directory containing temporal bitmap index files
     * @return TemporalTriangleResult with triangle count and metadata
     */
    static TemporalTriangleResult countTrianglesAtSnapshot(
        int graphId,
        uint32_t snapshotId,
        const std::string& snapshotDir);
};

#endif  // JASMINEGRAPH_HISTORYTRIANGLES_H
