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

#include "StreamingTriangles.h"
#include <algorithm>
#include <vector>

#include "../../../util/logger/Logger.h"

Logger streaming_triangle_logger;

long count(std::map<long, std::unordered_set<long>>& g1,
           std::map<long, std::unordered_set<long>>& g2,
           std::map<long, long>& edges);
long totalCount(std::map<long, std::unordered_set<long>>& g1,
           std::map<long, std::unordered_set<long>>& g2,
           std::map<long, long>& edges);

TriangleResult StreamingTriangles::countTriangles(NodeManager* nodeManager, bool returnTriangles){
    std::map<long, std::unordered_set<long>> adjacenyList = nodeManager->getAdjacencyList();
    std::map<long, long> distributionMap = nodeManager->getDistributionMap();

    TriangleResult result = Triangles::countTriangles(adjacenyList, distributionMap, returnTriangles);

    return result;
}

//Count triangles in local store from scratch
NativeStoreTriangleResult StreamingTriangles::countLocalStreamingTriangles(
        JasmineGraphIncrementalLocalStore *incrementalLocalStoreInstance) {
    streaming_triangle_logger.log("###STREAMING TRIANGLE### Static Streaming Local Triangle Counting: Started", "info");
    TriangleResult result = countTriangles(incrementalLocalStoreInstance->nm, false);
    long triangleCount = result.count;

    NodeManager* nodeManager = incrementalLocalStoreInstance->nm;
    std::string graphID = std::to_string(nodeManager->getGraphID());
    std::string partitionID = std::to_string(nodeManager->getPartitionID());


    std::string dbPrefix = nodeManager->dbPrefix;
    long local_relation_count = nodeManager->dbSize(dbPrefix + "_relations.db") / RelationBlock::BLOCK_SIZE;
    long central_relation_count = nodeManager->dbSize(dbPrefix + "_central_relations.db") / RelationBlock::BLOCK_SIZE;

    NativeStoreTriangleResult nativeStoreTriangleResult;
    nativeStoreTriangleResult.localRelationCount = local_relation_count;
    nativeStoreTriangleResult.centralRelationCount = central_relation_count;
    nativeStoreTriangleResult.result = triangleCount;

    streaming_triangle_logger.log("###STREAMING TRIANGLE### Static Streaming Local Triangle Counting: Completed: StreamingTriangles" + std::to_string(triangleCount),
                                  "info");
    return nativeStoreTriangleResult ;
}

std::string StreamingTriangles::countCentralStoreStreamingTriangles(std::vector<JasmineGraphIncrementalLocalStore*> incrementalLocalStoreInstances) {
    streaming_triangle_logger.log("###STREAMING TRIANGLE### Static Streaming Central Triangle Counting: Started", "info");
    std::map<long, std::unordered_set<long>> adjacencyList;
    std::map<long, long> degreeMap;

    for (JasmineGraphIncrementalLocalStore* jasmineGraphIncrementalLocalStore : incrementalLocalStoreInstances) {
        NodeManager* nm = jasmineGraphIncrementalLocalStore->nm;

        // Merge adjacency lists
        std::map<long, std::unordered_set<long>> currentAdjacencyList = nm->getAdjacencyList();
        for (const auto& entry : currentAdjacencyList) {
            adjacencyList[entry.first].insert(entry.second.begin(), entry.second.end());
        }

        // Merge degree maps
        std::map<long, long> currentDegreeMap = nm->getDistributionMap();
        for (const auto& entry : currentDegreeMap) {
            degreeMap[entry.first] += entry.second;
        }
    }

    TriangleResult result = Triangles::countTriangles(adjacencyList, degreeMap, true);
    streaming_triangle_logger.log("###STREAMING TRIANGLE### Static Streaming Central Triangle Counting: Completed", "info");
    return result.triangles;
}

//Count triangles in local store incrementally
NativeStoreTriangleResult StreamingTriangles::countDynamicLocalTriangles(
        JasmineGraphIncrementalLocalStore *incrementalLocalStoreInstance,
        long old_local_relation_count, long old_central_relation_count) {
    streaming_triangle_logger.log("###STREAMING TRIANGLE### Dynamic Streaming Local Triangle Counting: Started", "info");
    NodeManager* nodeManager = incrementalLocalStoreInstance->nm;
    std::map<long, long> edges;

    streaming_triangle_logger.debug("got previous count " + std::to_string(old_local_relation_count) + " " +
                                  std::to_string(old_central_relation_count));

    std::string dbPrefix = nodeManager->dbPrefix;
    int relationBlockSize = RelationBlock::BLOCK_SIZE;

    long new_local_relation_count = nodeManager->dbSize(dbPrefix + "_relations.db") / relationBlockSize;
    long new_central_relation_count = nodeManager->dbSize(dbPrefix + "_central_relations.db") / relationBlockSize;
    streaming_triangle_logger.debug("got relation count " + std::to_string(new_local_relation_count) + " " +
                                  std::to_string(new_central_relation_count));

    for (int i = old_local_relation_count; i < new_local_relation_count; ++i) {
        RelationBlock* relationBlock = RelationBlock::get(i*relationBlockSize);
        edges.emplace(std::stol(relationBlock->getSource()->id), std::stol(relationBlock->getDestination()->id));
    }

    for (int i = old_central_relation_count; i < new_central_relation_count ; ++i) {
        RelationBlock* relationBlock = RelationBlock::getCentral(i*relationBlockSize);
        edges.emplace(std::stol(relationBlock->getSource()->id), std::stol(relationBlock->getDestination()->id));
    }

    std::map<long, std::unordered_set<long>> adjacenyList = nodeManager->getAdjacencyList();

    std::map<long, std::unordered_set<long>> newAdjacencyList;

    for (const auto& edge : edges) {
        long sourceNode = edge.first;
        long targetNode = edge.second;

        newAdjacencyList[sourceNode].insert(targetNode);
        newAdjacencyList[targetNode].insert(sourceNode);
    }

    long trianglesValue = totalCount(adjacenyList, newAdjacencyList, edges);

    NativeStoreTriangleResult nativeStoreTriangleResult;
    nativeStoreTriangleResult.localRelationCount = new_local_relation_count;
    nativeStoreTriangleResult.centralRelationCount = new_central_relation_count;
    nativeStoreTriangleResult.result = trianglesValue;
    streaming_triangle_logger.log("###STREAMING TRIANGLE### Dynamic Streaming Local Triangle Counting: Completed", "info");
    return  nativeStoreTriangleResult;
}

//Count triangles in composte central store incrementally
std::string StreamingTriangles::countDynamicCentralTriangles(
        std::vector<JasmineGraphIncrementalLocalStore*> incrementalLocalStoreInstances,
        std::vector<std::string> oldCentralRelationCount) {
    streaming_triangle_logger.log("###STREAMING TRIANGLE### Dynamic Streaming Central Triangle Counting: Started", "info");
    std::map<long, std::unordered_set<long>> adjacencyList;
    std::map<long, long> edges;
    int position = 0;
    NodeManager* nodeManager;
    for (JasmineGraphIncrementalLocalStore* jasmineGraphIncrementalLocalStore : incrementalLocalStoreInstances) {
        nodeManager = jasmineGraphIncrementalLocalStore->nm;

        // Merge adjacency lists
        std::map<long, std::unordered_set<long>> currentAdjacencyList = nodeManager->getAdjacencyList();
        for (const auto& entry : currentAdjacencyList) {
            adjacencyList[entry.first].insert(entry.second.begin(), entry.second.end());
        }

        long previous_central_relation_count = std::stol(oldCentralRelationCount[position]);
        position++;
        streaming_triangle_logger.debug("got previous central count " +
                                      std::to_string(previous_central_relation_count));

        std::string dbPrefix = nodeManager->dbPrefix;
        int relationBlockSize = RelationBlock::BLOCK_SIZE;
        streaming_triangle_logger.debug("fetched dbprefix : " +
                                              dbPrefix);

        long new_central_relation_count = nodeManager->dbSize(dbPrefix + "_central_relations.db") / relationBlockSize;
        streaming_triangle_logger.debug("got current central relation count " +
                                      std::to_string(new_central_relation_count));

        for (int i = previous_central_relation_count; i < new_central_relation_count ; ++i) {
            RelationBlock* relationBlock = RelationBlock::getCentral(i*relationBlockSize);
            edges.emplace(std::stol(relationBlock->getSource()->id), std::stol(relationBlock->getDestination()->id));
        }
    }
    std::string triangle = "";
    long varOne = 0;
    long varTwo = 0;
    long varThree = 0;

    for (const auto& edge : edges) {
        long u = edge.first;
        long v = edge.second;
        long count = 0;

        for (long w : adjacencyList[u]) {
            if (adjacencyList[v].count(w) > 0) {
                std::vector<long> tempVector;
                tempVector.push_back(u);
                tempVector.push_back(v);
                tempVector.push_back(w);
                std::sort(tempVector.begin(), tempVector.end());

                varOne = tempVector[0];
                varTwo = tempVector[1];
                varThree = tempVector[2];
                triangle = triangle + std::to_string(varOne) + "," + std::to_string(varTwo) + "," +
                           std::to_string(varThree) + ":";
                count++;
            }
        }
    }
    streaming_triangle_logger.log("###STREAMING TRIANGLE### Dynamic Streaming Central Triangle Counting: Finished", "info");
    return triangle;

}

long count(std::map<long, std::unordered_set<long>>& g1,
           std::map<long, std::unordered_set<long>>& g2,
           std::map<long, long>& edges) {

    long total_count = 0;

    for (const auto& edge : edges) {
        long u = edge.first;
        long v = edge.second;
        long count = 0;

        for (long w : g1[u]) {
            if (g2[v].count(w) > 0) {
                count++;
            }
        }

        total_count += count;
    }

    return total_count;
}

long totalCount(std::map<long, std::unordered_set<long>>& g1,
                std::map<long, std::unordered_set<long>>& g2,
                std::map<long, long>& edges) {
    long s1 = count(g1, g1, edges);
    long s2 = count(g1, g2, edges);
    long s3 = count(g2, g2, edges);

    return 1 / 2 * ((s1 - s2) + (s3 / 3));
}