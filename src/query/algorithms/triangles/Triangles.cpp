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

Logger triangle_logger;

long Triangles::run(JasmineGraphHashMapLocalStore graphDB, JasmineGraphHashMapCentralStore centralStore,
                    JasmineGraphHashMapDuplicateCentralStore duplicateCentralStore, std::string hostName) {
    return run(graphDB, centralStore, duplicateCentralStore, NULL, NULL, 0);
}

long Triangles::run(JasmineGraphHashMapLocalStore graphDB, JasmineGraphHashMapCentralStore centralStore,
                    JasmineGraphHashMapDuplicateCentralStore duplicateCentralStore, std::string graphId,
                    std::string partitionId, int threadPriority) {
    triangle_logger.log("###TRIANGLE### Triangle Counting: Started", "info");
    map<long, unordered_set<long>> localSubGraphMap = graphDB.getUnderlyingHashMap();
    map<long, unordered_set<long>> centralDBSubGraphMap = centralStore.getUnderlyingHashMap();
    map<long, unordered_set<long>> duplicateCentralDBSubGraphMap = duplicateCentralStore.getUnderlyingHashMap();
    map<long, long> degreeDistribution = graphDB.getOutDegreeDistributionHashMap();
    map<long, long> centralDBDegreeDistribution = centralStore.getOutDegreeDistributionHashMap();
    map<long, long> centralDuplicateDBDegreeDistribution = duplicateCentralStore.getOutDegreeDistributionHashMap();
    std::map<long, std::set<long>> degreeMap;

    std::map<long, long>::iterator it;
    std::map<long, long>::iterator centralDBDegreeDistributionIterator;
    std::map<long, long>::iterator centralDuplicateDBDegreeDistributionIterator;

    auto mergeBbegin = std::chrono::high_resolution_clock::now();

    for (centralDuplicateDBDegreeDistributionIterator = centralDuplicateDBDegreeDistribution.begin();
         centralDuplicateDBDegreeDistributionIterator != centralDuplicateDBDegreeDistribution.end();
         ++centralDuplicateDBDegreeDistributionIterator) {
        long centralDuplicateDBStartVid = centralDuplicateDBDegreeDistributionIterator->first;

        unordered_set<long> centralDBSecondVertexSet = centralDBSubGraphMap[centralDuplicateDBStartVid];
        unordered_set<long> duplicateSecondVertexSet = duplicateCentralDBSubGraphMap[centralDuplicateDBStartVid];
        std::set<long> result;

        std::set_difference(duplicateSecondVertexSet.begin(), duplicateSecondVertexSet.end(),
                            centralDBSecondVertexSet.begin(), centralDBSecondVertexSet.end(),
                            std::inserter(result, result.end()));

        if (result.size() > 0) {
            centralDBDegreeDistribution[centralDuplicateDBStartVid] += result.size();
            centralDBSubGraphMap[centralDuplicateDBStartVid].insert(result.begin(), result.end());
        }
    }

    // Merging Local Store and Workers central stores before starting triangle count
    for (centralDBDegreeDistributionIterator = centralDBDegreeDistribution.begin();
         centralDBDegreeDistributionIterator != centralDBDegreeDistribution.end();
         ++centralDBDegreeDistributionIterator) {
        long centralDBStartVid = centralDBDegreeDistributionIterator->first;
        long centralDBDegree = centralDBDegreeDistributionIterator->second;

        degreeDistribution[centralDBStartVid] += centralDBDegree;
        localSubGraphMap[centralDBStartVid].insert(centralDBSubGraphMap[centralDBStartVid].begin(),
                                                   centralDBSubGraphMap[centralDBStartVid].end());
    }

    auto mergeEnd = std::chrono::high_resolution_clock::now();
    auto mergeDur = mergeEnd - mergeBbegin;
    auto mergeMsDuration = std::chrono::duration_cast<std::chrono::milliseconds>(mergeDur).count();

    triangle_logger.log(" Merge time Taken: " + std::to_string(mergeMsDuration) + " milliseconds", "info");

    const TriangleResult &triangleResult = countTriangles(localSubGraphMap, degreeDistribution, false);
    return triangleResult.count;
}

TriangleResult Triangles::countTriangles(map<long, unordered_set<long>> centralStore, map<long, long> distributionMap,
                                         bool returnTriangles) {
    std::map<long, std::set<long>> degreeMap;
    std::basic_ostringstream<char> triangleStream;

    long startVId;
    long degree;

    for (auto it = distributionMap.begin(); it != distributionMap.end(); ++it) {
        degree = it->second;
        if (degree == 1) continue;
        startVId = it->first;
        degreeMap[degree].insert(startVId);
    }

    long triangleCount = 0;
    std::unordered_map<long, std::unordered_map<long, std::unordered_set<long>>> triangleTree;

    for (auto iterator = degreeMap.begin(); iterator != degreeMap.end(); ++iterator) {
        long key = iterator->first;
        std::set<long> &vertices = iterator->second;

        for (auto verticesIterator = vertices.begin(); verticesIterator != vertices.end(); ++verticesIterator) {
            long temp = *verticesIterator;
            std::unordered_set<long> &unorderedUSet = centralStore[temp];
            for (auto uSetIterator = unorderedUSet.begin(); uSetIterator != unorderedUSet.end(); ++uSetIterator) {
                long u = *uSetIterator;
                if (temp == u) continue;
                std::unordered_set<long> &unorderedNuSet = centralStore[u];
                for (auto nuSetIterator = unorderedNuSet.begin(); nuSetIterator != unorderedNuSet.end();
                     ++nuSetIterator) {
                    long nu = *nuSetIterator;
                    if (temp == nu) continue;
                    if (u == nu) continue;
                    std::unordered_set<long> &centralStoreNu = centralStore[nu];
                    if ((unorderedUSet.find(nu) != unorderedUSet.end()) || (centralStoreNu.find(temp) != centralStoreNu.end())) {
                        register long varOne = temp;
                        register long varTwo = u;
                        register long varThree = nu;
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
                                    triangleStream << varOne << "," << varTwo << "," << varThree << ":";
                                }
                            }
                        } else {
                            triangleTree[varOne][varTwo].insert(varThree);
                            triangleCount++;
                            if (returnTriangles) {
                                triangleStream << varOne << "," << varTwo << "," << varThree << ":";
                            }
                        }
                    }
                }
            }
        }
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
