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
    std::map<long, long> degreeReverseLookupMap;
    std::map<long, std::set<long>> degreeMap;
    std::set<long> degreeSet;

    std::map<long, long>::iterator it;
    std::map<long, long>::iterator degreeDistributionIterator;
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

    TriangleResult triangleResult = countTriangles(localSubGraphMap, degreeDistribution, false);
    return triangleResult.count;
}

TriangleResult Triangles::countTriangles(map<long, unordered_set<long>> centralStore,
                                             map<long, long> distributionMap, bool returnTriangles) {

    std::map<long, std::set<long>> degreeMap;
    std::string triangle = "";

    long startVId;
    long degree;

    std::map<long, long>::iterator it;

    for (it = distributionMap.begin(); it != distributionMap.end(); ++it) {
        startVId = it->first;
        degree = it->second;
        degreeMap[degree].insert(startVId);
    }

    long triangleCount = 0;
    long varOne = 0;
    long varTwo = 0;
    long varThree = 0;

    std::map<long, std::map<long, std::vector<long>>> triangleTree;
    std::vector<long> degreeListVisited;

    std::map<long, std::set<long>>::iterator iterator;

    for (iterator = degreeMap.begin(); iterator != degreeMap.end(); ++iterator) {
        long key = iterator->first;
        std::set<long> vertices = iterator->second;

        if (key==1){
            continue;
        }
        std::set<long>::iterator verticesIterator;

        for (verticesIterator = vertices.begin(); verticesIterator != vertices.end(); ++verticesIterator) {
            long temp = *verticesIterator;
            std::set<long> orderedUList(centralStore[temp].begin(), centralStore[temp].end());
            std::set<long>::iterator uListIterator;
            for (uListIterator = orderedUList.begin(); uListIterator != orderedUList.end(); ++uListIterator) {
                long u = *uListIterator;
                std::set<long> orderedNuList(centralStore[u].begin(), centralStore[u].end());
                std::set<long>::iterator nuListIterator;
                for (nuListIterator = orderedNuList.begin(); nuListIterator != orderedNuList.end(); ++nuListIterator) {
                    long nu = *nuListIterator;
                    if ( ((centralStore[temp].find(nu) != centralStore[temp].end()) ||
                          (centralStore[nu].find(temp) != centralStore[nu].end())) &&
                            (temp != nu) && (u != nu) && (temp != u) ) {
                        std::vector<long> tempVector;
                        tempVector.push_back(temp);
                        tempVector.push_back(u);
                        tempVector.push_back(nu);
                        std::sort(tempVector.begin(), tempVector.end());

                        varOne = tempVector[0];
                        varTwo = tempVector[1];
                        varThree = tempVector[2];

                        std::map<long, std::vector<long>> itemRes = triangleTree[varOne];

                        std::map<long, std::vector<long>>::iterator itemResIterator = itemRes.find(varTwo);

                        if (itemResIterator != itemRes.end()) {
                            std::vector<long> list = itemRes[varTwo];

                            std::vector<long>::iterator listIterator;
                            if (std::find(list.begin(), list.end(), varThree) == list.end()) {
                                list.push_back(varThree);
                                itemRes[varTwo] = list;
                                triangleTree[varOne] = itemRes;
                                triangleCount++;
                                if (returnTriangles) {
                                    triangle = triangle + std::to_string(varOne) + "," + std::to_string(varTwo) + "," +
                                               std::to_string(varThree) + ":";
                                }
                            }
                        } else {
                            triangleTree[varOne][varTwo].push_back(varThree);
                            triangleCount++;
                            if (returnTriangles) {
                                triangle = triangle + std::to_string(varOne) + "," + std::to_string(varTwo) + "," +
                                           std::to_string(varThree) + ":";
                            }
                        }
                    }
                }
            }
        }
        degreeListVisited.push_back(key);
    }

    TriangleResult result;

    if (returnTriangles) {
        if (triangle.empty()) {
            result.triangles = "NILL";
        } else {
            result.triangles = triangle.substr(0, triangle.size() - 1);
        }
    } else {
        result.count = triangleCount;
    }

    return result;
}