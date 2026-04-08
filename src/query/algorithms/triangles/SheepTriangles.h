/**
Copyright 2024 JasminGraph Team
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

#ifndef JASMINEGRAPH_SHEEPTRIANGLES_H
#define JASMINEGRAPH_SHEEPTRIANGLES_H

#include <map>
#include <unordered_set>
#include <string>
#include "../../../centralstore/JasmineGraphHashMapCentralStore.h"
#include "../../../centralstore/JasmineGraphHashMapDuplicateCentralStore.h"
#include "../../../localstore/JasmineGraphHashMapLocalStore.h"

struct SheepTriangleResult {
    long count;
    std::string triangles;
};

/**
 * Optimized triangle counting for sheep-partitioned graphs.
 * 
 * Key differences from standard algorithm:
 * - Handles bidirectional edge representation efficiently
 * - Optimized for sheep partitioner's edge distribution pattern
 * - Uses vertex-ordered triangle enumeration to prevent double counting
 */
class SheepTriangles {
 public:
    /**
     * Count triangles in sheep-partitioned graph
     * @param graphDB Local partition data
     * @param centralStore Central store with cross-partition edges
     * @param duplicateCentralStore Duplicate central store
     * @param graphId Graph identifier
     * @param partitionId Partition identifier
     * @return Triangle count
     */
    static long run(JasmineGraphHashMapLocalStore &graphDB, 
                   JasmineGraphHashMapCentralStore &centralStore,
                   JasmineGraphHashMapDuplicateCentralStore &duplicateCentralStore, 
                   std::string graphId,
                   std::string partitionId);

 private:
    /**
     * Core triangle counting with ordered enumeration
     * Uses vertex ordering (u < v < w) to ensure each triangle counted exactly once
     */
    static SheepTriangleResult countTriangles(
        std::map<long, std::unordered_set<long>> &edgeMap,
        bool returnTriangles = false);
    
    /**
     * Merge stores efficiently for sheep format
     */
    static void mergeStores(
        std::map<long, std::unordered_set<long>> &localMap,
        std::map<long, std::unordered_set<long>> &centralMap,
        std::map<long, std::unordered_set<long>> &duplicateMap);
};

#endif  // JASMINEGRAPH_SHEEPTRIANGLES_H
