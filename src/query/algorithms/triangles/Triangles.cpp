/**
Copyright 2018 JasmineGraph Team
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

#include "Triangles.h"

#include <algorithm>
#include <chrono>
#include <ctime>
#include <sstream>
#include <vector>

#include "../../../localstore/JasmineGraphHashMapLocalStore.h"
#include "../../../util/logger/Logger.h"
#include "../../../util/telemetry/OpenTelemetryUtil.h"  

Logger triangle_logger;

long Triangles::run(JasmineGraphHashMapLocalStore &graphDB, JasmineGraphHashMapCentralStore &centralStore,
                    JasmineGraphHashMapDuplicateCentralStore &duplicateCentralStore, std::string hostName) {
    return run(graphDB, centralStore, duplicateCentralStore, NULL, NULL, 0);
}

long Triangles::run(JasmineGraphHashMapLocalStore &graphDB, JasmineGraphHashMapCentralStore &centralStore,
                    JasmineGraphHashMapDuplicateCentralStore &duplicateCentralStore, std::string graphId,
                    std::string partitionId, int threadPriority) {
    OTEL_TRACE_FUNCTION(); 
    
    triangle_logger.info("###TRIANGLE### Triangle Counting: Started");
    
    // Trace data extraction from stores
    map<long, unordered_set<long>> localSubGraphMap;
    map<long, unordered_set<long>> centralDBSubGraphMap;
    map<long, unordered_set<long>> duplicateCentralDBSubGraphMap;
    map<long, long> degreeDistribution;
    map<long, long> centralDBDegreeDistribution;
    map<long, long> centralDuplicateDBDegreeDistribution;
    
    {
        OTEL_TRACE_OPERATION("extract_data_from_stores");
        localSubGraphMap = graphDB.getUnderlyingHashMap();
        centralDBSubGraphMap = centralStore.getUnderlyingHashMap();
        duplicateCentralDBSubGraphMap = duplicateCentralStore.getUnderlyingHashMap();
        degreeDistribution = graphDB.getOutDegreeDistributionHashMap();
        centralDBDegreeDistribution = centralStore.getOutDegreeDistributionHashMap();
        centralDuplicateDBDegreeDistribution = duplicateCentralStore.getOutDegreeDistributionHashMap();
    }

    auto mergeBbegin = std::chrono::high_resolution_clock::now();

    for (auto centralDuplicateDBDegreeDistributionIterator = centralDuplicateDBDegreeDistribution.begin();
         centralDuplicateDBDegreeDistributionIterator != centralDuplicateDBDegreeDistribution.end();
         ++centralDuplicateDBDegreeDistributionIterator) {
        long centralDuplicateDBStartVid = centralDuplicateDBDegreeDistributionIterator->first;

        unordered_set<long> &centralDBSecondVertexSet = centralDBSubGraphMap[centralDuplicateDBStartVid];
        const unordered_set<long> &duplicateSecondVertexSet = duplicateCentralDBSubGraphMap[centralDuplicateDBStartVid];
        unordered_set<long> result;

        std::set_difference(duplicateSecondVertexSet.begin(), duplicateSecondVertexSet.end(),
                            centralDBSecondVertexSet.begin(), centralDBSecondVertexSet.end(),
                            std::inserter(result, result.end()));

        if (result.size() > 0) {
            centralDBDegreeDistribution[centralDuplicateDBStartVid] += result.size();
            centralDBSecondVertexSet.insert(result.begin(), result.end());
        }
    }

    // Merging Local Store and Workers central stores before starting triangle count
    {
        OTEL_TRACE_OPERATION("merge_stores_data");
        for (auto centralDBDegreeDistributionIterator = centralDBDegreeDistribution.begin();
             centralDBDegreeDistributionIterator != centralDBDegreeDistribution.end();
             ++centralDBDegreeDistributionIterator) {
            long centralDBStartVid = centralDBDegreeDistributionIterator->first;
            long centralDBDegree = centralDBDegreeDistributionIterator->second;

            degreeDistribution[centralDBStartVid] += centralDBDegree;
            localSubGraphMap[centralDBStartVid].insert(centralDBSubGraphMap[centralDBStartVid].begin(),
                                                       centralDBSubGraphMap[centralDBStartVid].end());
        }
    }

    auto mergeEnd = std::chrono::high_resolution_clock::now();
    auto mergeDur = mergeEnd - mergeBbegin;
    auto mergeMsDuration = std::chrono::duration_cast<std::chrono::milliseconds>(mergeDur).count();

    triangle_logger.info(" Merge time Taken: " + std::to_string(mergeMsDuration) + " milliseconds");

    // Trace the core triangle counting algorithm
    TriangleResult triangleResult;
    {
        OTEL_TRACE_OPERATION("core_triangle_counting_algorithm");
        triangleResult = countTriangles(localSubGraphMap, degreeDistribution, false);
    }
    return triangleResult.count;
}

TriangleResult Triangles::countTriangles(map<long, unordered_set<long>> &centralStore, map<long, long> &distributionMap,
                                         bool returnTriangles) {
    OTEL_TRACE_FUNCTION();
    
    std::map<long, std::set<long>> degreeMap;
    std::basic_ostringstream<char> triangleStream;

    long startVertexId;
    long degree;

    // Trace degree map creation
    {
        OTEL_TRACE_OPERATION("build_degree_map");
        for (auto it = distributionMap.begin(); it != distributionMap.end(); ++it) {
            degree = it->second;
            if (degree == 1) continue;
            startVertexId = it->first;
            degreeMap[degree].insert(startVertexId);
        }
    }

    long triangleCount = 0;
    std::unordered_map<long, std::unordered_map<long, std::unordered_set<long>>> triangleTree;
    // TODO(thevindu-w): Make centralstore undirected and prevent saving triangles in-memory

    // Trace the main triangle detection loop
    {
        OTEL_TRACE_OPERATION("triangle_detection_loop");
        
        for (auto iterator = degreeMap.begin(); iterator != degreeMap.end(); ++iterator) {
            auto &vertices = iterator->second;

            for (auto verticesIterator = vertices.begin(); verticesIterator != vertices.end(); ++verticesIterator) {
            long temp = *verticesIterator;
            auto &unorderedUSet = centralStore[temp];
            for (auto uSetIterator = unorderedUSet.begin(); uSetIterator != unorderedUSet.end(); ++uSetIterator) {
                long u = *uSetIterator;
                if (temp == u) continue;
                auto &unorderedNuSet = centralStore[u];
                for (auto nuSetIterator = unorderedNuSet.begin(); nuSetIterator != unorderedNuSet.end();
                     ++nuSetIterator) {
                    long nu = *nuSetIterator;
                    if (temp == nu) continue;
                    if (u == nu) continue;
                    auto &centralStoreNu = centralStore[nu];
                    if ((unorderedUSet.find(nu) != unorderedUSet.end()) ||
                        (centralStoreNu.find(temp) != centralStoreNu.end())) {
                        long varOne = temp;
                        long varTwo = u;
                        long varThree = nu;
                        if (varOne > varTwo) {  // swap
                            varOne ^= varTwo;
                            varTwo ^= varOne;
                            varOne ^= varTwo;
                        }
                        if (varOne > varThree) {  // swap
                            varOne ^= varThree;
                            varThree ^= varOne;
                            varOne ^= varThree;
                        }
                        if (varTwo > varThree) {  // swap
                            varTwo ^= varThree;
                            varThree ^= varTwo;
                            varTwo ^= varThree;
                        }

                        auto &itemRes = triangleTree[varOne];
                        auto itemResIterator = itemRes.find(varTwo);
                        if (itemResIterator != itemRes.end()) {
                            auto &set2 = itemRes[varTwo];
                            auto set2Iter = set2.find(varThree);
                            if (set2Iter == set2.end()) {
                                set2.insert(varThree);
                                triangleCount++;
                                if (returnTriangles) {
                                    // TODO(thevindu-w): Flush to file on count exceeds value
                                    triangleStream << varOne << "," << varTwo << "," << varThree << ":";
                                }
                            }
                        } else {
                            triangleTree[varOne][varTwo].insert(varThree);
                            triangleCount++;
                            if (returnTriangles) {
                                // TODO(thevindu-w): Flush to file on count exceeds value
                                triangleStream << varOne << "," << varTwo << "," << varThree << ":";
                            }
                        }
                    }
                }
            }
            }
        }
    } // End of triangle_detection_loop trace scope
    
    {
        OTEL_TRACE_OPERATION("cleanup_and_result_preparation");
        triangleTree.clear();
    }

    TriangleResult result;
    if (returnTriangles) {
        string triangle = triangleStream.str();
        if (triangle.empty()) {
            result.triangles = "NILL";
        } else {
            triangle.erase(triangle.size() - 1);
            result.triangles = std::move(triangle);
        }
    } else {
        result.count = triangleCount;
    }
    return result;
}
