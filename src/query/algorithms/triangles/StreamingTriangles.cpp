//
// Created by ashokkumar on 23/12/23.
//

#include "StreamingTriangles.h"
#include <algorithm>
#include <chrono>
#include <ctime>
#include <vector>

#include "../../../util/logger/Logger.h"

Logger streaming_triangle_logger;

long StreamingTriangles::run(JasmineGraphIncrementalLocalStore *incrementalLocalStoreInstance) {
    streaming_triangle_logger.log("###STREAMING TRIANGLE### Streaming Triangle Counting: Started", "info");
    long triangleCount = 0;

    NodeManager* nodeManager = incrementalLocalStoreInstance->getNodeManager();
    std::list<NodeBlock> allNodes = nodeManager->getGraph();

    for (NodeBlock node:allNodes) {
        std::list<NodeBlock> neighboursLevelOne = node.getAllEdges();
        streaming_triangle_logger.log("###STREAMING TRIANGLE### Entering vertice : " + node.id, "info");

        for (NodeBlock neighbour_1:neighboursLevelOne) {
            std::list<NodeBlock> neighboursLevelTwo = neighbour_1.getAllEdges();
            streaming_triangle_logger.log("###STREAMING TRIANGLE### Entering neighbour 1 : " + neighbour_1.id, "info");

            for (NodeBlock neighbour_2:neighboursLevelTwo) {
                streaming_triangle_logger.log("###STREAMING TRIANGLE### Entering neighbour 2 : " + neighbour_2.id, "info");
                if (neighbour_2.searchRelation(node)){
                    triangleCount += 1;
                }
            }
        }

    }

    streaming_triangle_logger.log("###STREAMING TRIANGLE### Streaming Triangle Counting: Completed: StreamingTriangles" + std::to_string(triangleCount),
                        "info");
    return triangleCount / 6;
}

//string StramingTriangles::countCentralStoreStramingTriangles(map<long, unordered_set<long>> centralStore,
//                                             map<long, long> distributionMap, int threadPriority) {

//    return triangle.substr(0, triangle.size() - 1);
//}