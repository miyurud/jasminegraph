/**
Copyright 2025 JasmineGraph Team
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

#include "OperatorExecutor.h"
#include "InstanceHandler.h"
#include "../util/Const.h"
#include "../../../../util/logger/Logger.h"
#include "Helpers.h"
#include <thread>
#include <queue>
#include <chrono>
#include <random>

Logger execution_logger;
std::unordered_map<std::string,
    std::function<void(OperatorExecutor&, SharedBuffer&, std::string, GraphConfig)>> OperatorExecutor::methodMap;
OperatorExecutor::OperatorExecutor(GraphConfig gc, std::string queryPlan, std::string masterIP):
    queryPlan(queryPlan), gc(gc), masterIP(masterIP) {
    this->query = json::parse(queryPlan);
};

void OperatorExecutor::initializeMethodMap() {
    methodMap["AllNodeScan"] = [](OperatorExecutor &executor, SharedBuffer &buffer,
            std::string jsonPlan, GraphConfig gc) {
        executor.AllNodeScan(buffer, jsonPlan, gc);
    };

    methodMap["ProduceResult"] = [](OperatorExecutor &executor, SharedBuffer &buffer,
            std::string jsonPlan, GraphConfig gc) {
        executor.ProduceResult(buffer, jsonPlan, gc);
    };

    methodMap["Filter"] = [](OperatorExecutor &executor, SharedBuffer &buffer,
            std::string jsonPlan, GraphConfig gc) {
        executor.Filter(buffer, jsonPlan, gc);
    };

    methodMap["ExpandAll"] = [](OperatorExecutor &executor, SharedBuffer &buffer,
            std::string jsonPlan, GraphConfig gc) {
        executor.ExpandAll(buffer, jsonPlan, gc);
    };

    methodMap["UndirectedRelationshipTypeScan"] = [](OperatorExecutor &executor, SharedBuffer &buffer,
            std::string jsonPlan, GraphConfig gc) {
        executor.UndirectedRelationshipTypeScan(buffer, jsonPlan, gc);
    };

    methodMap["UndirectedAllRelationshipScan"] = [](OperatorExecutor &executor, SharedBuffer &buffer,
            std::string jsonPlan, GraphConfig gc) {
        executor.UndirectedAllRelationshipScan(buffer, jsonPlan, gc);
    };

    methodMap["DirectedRelationshipTypeScan"] = [](OperatorExecutor &executor, SharedBuffer &buffer,
                                                     std::string jsonPlan, GraphConfig gc) {
        executor.DirectedRelationshipTypeScan(buffer, jsonPlan, gc);
    };

    methodMap["DirectedAllRelationshipScan"] = [](OperatorExecutor &executor, SharedBuffer &buffer,
                                                    std::string jsonPlan, GraphConfig gc) {
        executor.DirectedAllRelationshipScan(buffer, jsonPlan, gc);
    };

    methodMap["NodeByIdSeek"] = [](OperatorExecutor &executor, SharedBuffer &buffer,
                                                    std::string jsonPlan, GraphConfig gc) {
        executor.NodeByIdSeek(buffer, jsonPlan, gc);
    };

    methodMap["Projection"] = [](OperatorExecutor &executor, SharedBuffer &buffer,
            std::string jsonPlan, GraphConfig gc) {
        executor.Projection(buffer, jsonPlan, gc);
    };

    methodMap["AggregationFunction"] = [](OperatorExecutor &executor, SharedBuffer &buffer,
                                   std::string jsonPlan, GraphConfig gc) {
        executor.AggregationFunction(buffer, jsonPlan, gc);
    };

    methodMap["Create"] = [](OperatorExecutor &executor, SharedBuffer &buffer,
                                    std::string jsonPlan, GraphConfig gc) {
        executor.Create(buffer, jsonPlan, gc);
    };

    methodMap["CartesianProduct"] = [](OperatorExecutor &executor, SharedBuffer &buffer,
                                     std::string jsonPlan, GraphConfig gc) {
        executor.CartesianProduct(buffer, jsonPlan, gc);
    };

    methodMap["Distinct"] = [](OperatorExecutor &executor, SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
        executor.Distinct(buffer, jsonPlan, gc);
    };

    methodMap["OrderBy"] = [](OperatorExecutor &executor, SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
        executor.OrderBy(buffer, jsonPlan, gc);
    };

    methodMap["NodeScanByLabel"] = [](OperatorExecutor &executor, SharedBuffer &buffer, std::string jsonPlan,
            GraphConfig gc) {
        executor.NodeScanByLabel(buffer, jsonPlan, gc);
    };
}

void OperatorExecutor::AllNodeScan(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    NodeManager nodeManager(gc);
    for (auto it : nodeManager.nodeIndex) {
        json nodeData;
        auto nodeId = it.first;
        NodeBlock *node = nodeManager.get(nodeId);
        std::string value(node->getMetaPropertyHead()->value);
        if (value == to_string(gc.partitionID)) {
            nodeData["partitionID"] = value;
            std::map<std::string, char*> properties = node->getAllProperties();
            for (auto property : properties) {
                nodeData[property.first] = property.second;
            }
            for (auto& [key, value] : properties) {
                delete[] value;  // Free each allocated char* array
            }
            properties.clear();

            json data;
            string variable = query["variables"];
            data[variable] = nodeData;
            buffer.add(data.dump());
        }
    }
    buffer.add("-1");
}

void OperatorExecutor::NodeScanByLabel(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    NodeManager nodeManager(gc);
    for (auto it : nodeManager.nodeIndex) {
        json nodeData;
        auto nodeId = it.first;
        NodeBlock *node = nodeManager.get(nodeId);
        string label = node->getLabel();
        std::string value(node->getMetaPropertyHead()->value);
        if (value == to_string(gc.partitionID) && label == query["Label"]) {
            nodeData["partitionID"] = value;
            std::map<std::string, char*> properties = node->getAllProperties();
            for (auto property : properties) {
                nodeData[property.first] = property.second;
            }
            for (auto& [key, value] : properties) {
                delete[] value;  // Free each allocated char* array
            }
            properties.clear();

            json data;
            string variable = query["variable"];
            data[variable] = nodeData;
            buffer.add(data.dump());
        }
    }
    buffer.add("-1");
}

void OperatorExecutor::ProduceResult(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    SharedBuffer sharedBuffer(INTER_OPERATOR_BUFFER_SIZE);
    std::string nextOpt = query["NextOperator"];
    json next = json::parse(nextOpt);
    auto method = OperatorExecutor::methodMap[next["Operator"]];
    // Launch the method in a new thread
    std::thread result(method, std::ref(*this), std::ref(sharedBuffer), query["NextOperator"], gc);

    while (true) {
        string raw = sharedBuffer.get();
        if (raw == "-1") {
            buffer.add(raw);
            result.join();
            break;
        }
        std::vector<std::string> values = query["variable"].get<std::vector<std::string>>();
        json data;
        json rawObj = json::parse(raw);
        for (auto value : values) {
            data[value] = rawObj[value];
        }
        buffer.add(data.dump());
    }
}

void OperatorExecutor::Filter(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    SharedBuffer sharedBuffer(INTER_OPERATOR_BUFFER_SIZE);
    std::string nextOpt = query["NextOperator"];
    json next = json::parse(nextOpt);
    auto method = OperatorExecutor::methodMap[next["Operator"]];
    // Launch the method in a new thread
    std::thread result(method, std::ref(*this), std::ref(sharedBuffer), query["NextOperator"], gc);

    auto condition = query["condition"];
    FilterHelper FilterHelper(condition.dump());
    while (true) {
        string raw = sharedBuffer.get();
        if (raw == "-1") {
            buffer.add(raw);
            result.join();
            break;
        }
        if (FilterHelper.evaluate(raw)) {
            buffer.add(raw);
        }
    }
}

void OperatorExecutor::UndirectedRelationshipTypeScan(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    NodeManager nodeManager(gc);

    const std::string& dbPrefix = nodeManager.getDbPrefix();
    long localRelationCount = nodeManager.dbSize(dbPrefix + "_relations.db") / RelationBlock::BLOCK_SIZE;
    long centralRelationCount = nodeManager.dbSize(dbPrefix +
                                                   "_central_relations.db") / RelationBlock::CENTRAL_BLOCK_SIZE;
    string direction = Utils::getGraphDirection(to_string(gc.graphID), masterIP);
    bool isDirected = false;
    if (direction == "TRUE") {
        isDirected = true;
    }
    int count = 1;
    for (long i = 1; i < localRelationCount; i++) {
        json startNodeData;
        json destNodeData;
        json relationData;
        RelationBlock* relation = RelationBlock::getLocalRelation(i*RelationBlock::BLOCK_SIZE);
        if (relation->getLocalRelationshipType() != query["relType"]) {
            continue;
        }
        NodeBlock* startNode = relation->getSource();
        NodeBlock* destNode = relation->getDestination();

        std::string startPid(startNode->getMetaPropertyHead()->value);
        startNodeData["partitionID"] = startPid;
        std::map<std::string, char*> startProperties = startNode->getAllProperties();
        for (auto property : startProperties) {
            startNodeData[property.first] = property.second;
        }
        for (auto& [key, value] : startProperties) {
            delete[] value;  // Free each allocated char* array
        }
        startProperties.clear();

        std::string destPid(destNode->getMetaPropertyHead()->value);
        destNodeData["partitionID"] = destPid;
        std::map<std::string, char*> destProperties = destNode->getAllProperties();
        for (auto property : destProperties) {
            destNodeData[property.first] = property.second;
        }
        for (auto& [key, value] : destProperties) {
            delete[] value;  // Free each allocated char* array
        }
        destProperties.clear();

        std::map<std::string, char*> relProperties = relation->getAllProperties();
        for (auto property : relProperties) {
            relationData[property.first] = property.second;
        }
        for (auto& [key, value] : relProperties) {
            delete[] value;  // Free each allocated char* array
        }
        relProperties.clear();

        json rightDirectionData;
        string start = query["sourceVariable"];
        string dest = query["destVariable"];
        string rel = query["relVariable"];

        rightDirectionData[start] = startNodeData;
        rightDirectionData[dest] = destNodeData;
        rightDirectionData[rel] = relationData;
        buffer.add(rightDirectionData.dump());

        if (!isDirected) {
            json leftDirectionData;
            leftDirectionData[start] = destNodeData;
            leftDirectionData[dest] = startNodeData;
            leftDirectionData[rel] = relationData;
            buffer.add(leftDirectionData.dump());
        }
        count++;
    }

    int central = 1;
    for (long i = 1; i < centralRelationCount; i++) {
        json startNodeData;
        json destNodeData;
        json relationData;
        RelationBlock* relation = RelationBlock::getCentralRelation(i * RelationBlock::CENTRAL_BLOCK_SIZE);
        if (relation->getCentralRelationshipType() != query["relType"]) {
            continue;
        }

        std::string pid(relation->getMetaPropertyHead()->value);
        if (pid != to_string(gc.partitionID)) {
            continue;
        }

        NodeBlock* startNode = relation->getSource();
        NodeBlock* destNode = relation->getDestination();

        std::string startPid(startNode->getMetaPropertyHead()->value);

        if (startPid != to_string(gc.partitionID)) {
            continue;
        }
        startNodeData["partitionID"] = startPid;
        std::map<std::string, char*> startProperties = startNode->getAllProperties();
        for (auto property : startProperties) {
            startNodeData[property.first] = property.second;
        }
        for (auto& [key, value] : startProperties) {
            delete[] value;  // Free each allocated char* array
        }
        startProperties.clear();

        std::string destPid(destNode->getMetaPropertyHead()->value);
        destNodeData["partitionID"] = destPid;

        std::map<std::string, char*> destProperties = destNode->getAllProperties();
        for (auto property : destProperties) {
            destNodeData[property.first] = property.second;
        }
        for (auto& [key, value] : destProperties) {
            delete[] value;  // Free each allocated char* array
        }
        destProperties.clear();

        std::map<std::string, char*> relProperties = relation->getAllProperties();
        for (auto property : relProperties) {
            relationData[property.first] = property.second;
        }
        for (auto& [key, value] : relProperties) {
            delete[] value;  // Free each allocated char* array
        }
        relProperties.clear();

        json rightDirectionData;
        string start = query["sourceVariable"];
        string dest = query["destVariable"];
        string rel = query["relVariable"];

        rightDirectionData[start] = startNodeData;
        rightDirectionData[dest] = destNodeData;
        rightDirectionData[rel] = relationData;
        buffer.add(rightDirectionData.dump());

        if (!isDirected) {
            json leftDirectionData;
            leftDirectionData[start] = destNodeData;
            leftDirectionData[dest] = startNodeData;
            leftDirectionData[rel] = relationData;
            buffer.add(leftDirectionData.dump());
        }
        central++;
    }
    buffer.add("-1");
}

void OperatorExecutor::UndirectedAllRelationshipScan(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    NodeManager nodeManager(gc);

    const std::string& dbPrefix = nodeManager.getDbPrefix();
    long localRelationCount = nodeManager.dbSize(dbPrefix + "_relations.db") / RelationBlock::BLOCK_SIZE;
    long centralRelationCount = nodeManager.dbSize(dbPrefix +
                                                    "_central_relations.db") / RelationBlock::CENTRAL_BLOCK_SIZE;
    string direction = Utils::getGraphDirection(to_string(gc.graphID), masterIP);
    bool isDirected = false;
    if (direction == "TRUE") {
        isDirected = true;
    }
    int count = 1;
    for (long i = 1; i < localRelationCount; i++) {
        json startNodeData;
        json destNodeData;
        json relationData;
        RelationBlock* relation = RelationBlock::getLocalRelation(i*RelationBlock::BLOCK_SIZE);
        NodeBlock* startNode = relation->getSource();
        NodeBlock* destNode = relation->getDestination();


        std::string startPid(startNode->getMetaPropertyHead()->value);
        startNodeData["partitionID"] = startPid;
        std::map<std::string, char*> startProperties = startNode->getAllProperties();
        for (auto property : startProperties) {
            startNodeData[property.first] = property.second;
        }
        for (auto& [key, value] : startProperties) {
            delete[] value;  // Free each allocated char* array
        }
        startProperties.clear();

        std::string destPid(destNode->getMetaPropertyHead()->value);
        destNodeData["partitionID"] = destPid;
        std::map<std::string, char*> destProperties = destNode->getAllProperties();
        for (auto property : destProperties) {
            destNodeData[property.first] = property.second;
        }
        for (auto& [key, value] : destProperties) {
            delete[] value;  // Free each allocated char* array
        }
        destProperties.clear();

        std::map<std::string, char*> relProperties = relation->getAllProperties();
        for (auto property : relProperties) {
            relationData[property.first] = property.second;
        }
        for (auto& [key, value] : relProperties) {
            delete[] value;  // Free each allocated char* array
        }
        relProperties.clear();

        json rightDirectionData;
        string start = query["sourceVariable"];
        string dest = query["destVariable"];
        string rel = query["relVariable"];

        rightDirectionData[start] = startNodeData;
        rightDirectionData[dest] = destNodeData;
        rightDirectionData[rel] = relationData;
        buffer.add(rightDirectionData.dump());

        if (!isDirected) {
            json leftDirectionData;
            leftDirectionData[start] = destNodeData;
            leftDirectionData[dest] = startNodeData;
            leftDirectionData[rel] = relationData;
            buffer.add(leftDirectionData.dump());
        }
        count++;
    }

    int central = 1;
    for (long i = 1; i < centralRelationCount; i++) {
        json startNodeData;
        json destNodeData;
        json relationData;
        RelationBlock* relation = RelationBlock::getCentralRelation(i*RelationBlock::CENTRAL_BLOCK_SIZE);
        std::string pid(relation->getMetaPropertyHead()->value);
        if (pid != to_string(gc.partitionID)) {
            continue;
        }

        NodeBlock* startNode = relation->getSource();
        NodeBlock* destNode = relation->getDestination();

        std::string startPid(startNode->getMetaPropertyHead()->value);
        startNodeData["partitionID"] = startPid;
        std::map<std::string, char*> startProperties = startNode->getAllProperties();
        for (auto property : startProperties) {
            startNodeData[property.first] = property.second;
        }
        for (auto& [key, value] : startProperties) {
            delete[] value;  // Free each allocated char* array
        }
        startProperties.clear();

        std::string destPid(destNode->getMetaPropertyHead()->value);
        destNodeData["partitionID"] = destPid;
        std::map<std::string, char*> destProperties = destNode->getAllProperties();
        for (auto property : destProperties) {
            destNodeData[property.first] = property.second;
        }
        for (auto& [key, value] : destProperties) {
            delete[] value;  // Free each allocated char* array
        }
        destProperties.clear();

        std::map<std::string, char*> relProperties = relation->getAllProperties();
        for (auto property : relProperties) {
            relationData[property.first] = property.second;
        }
        for (auto& [key, value] : relProperties) {
            delete[] value;  // Free each allocated char* array
        }
        relProperties.clear();

        json rightDirectionData;
        string start = query["sourceVariable"];
        string dest = query["destVariable"];
        string rel = query["relVariable"];

        rightDirectionData[start] = startNodeData;
        rightDirectionData[dest] = destNodeData;
        rightDirectionData[rel] = relationData;
        buffer.add(rightDirectionData.dump());

        if (!isDirected) {
            json leftDirectionData;
            leftDirectionData[start] = destNodeData;
            leftDirectionData[dest] = startNodeData;
            leftDirectionData[rel] = relationData;
            buffer.add(leftDirectionData.dump());
        }
        central++;
    }
    buffer.add("-1");
}

void OperatorExecutor::DirectedRelationshipTypeScan(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    NodeManager nodeManager(gc);
    string direction = query["direction"];
    const std::string& dbPrefix = nodeManager.getDbPrefix();
    long localRelationCount = nodeManager.dbSize(dbPrefix + "_relations.db") / RelationBlock::BLOCK_SIZE;
    long centralRelationCount = nodeManager.dbSize(dbPrefix +
                                                   "_central_relations.db") / RelationBlock::CENTRAL_BLOCK_SIZE;
    string graphDirection = Utils::getGraphDirection(to_string(gc.graphID), masterIP);
    bool isDirected = false;
    if (graphDirection == "TRUE") {
        isDirected = true;
    }
    bool isDirectionRight = query["direction"] == "right";
    int count = 1;
    for (long i = 1; i < localRelationCount; i++) {
        json startNodeData;
        json destNodeData;
        json relationData;
        RelationBlock* relation = RelationBlock::getLocalRelation(i*RelationBlock::BLOCK_SIZE);
        if (relation->getLocalRelationshipType() != query["relType"]) {
            continue;
        }
        NodeBlock* startNode = relation->getSource();
        NodeBlock* destNode = relation->getDestination();

        std::string startPid(startNode->getMetaPropertyHead()->value);
        startNodeData["partitionID"] = startPid;
        std::map<std::string, char*> startProperties = startNode->getAllProperties();
        for (auto property : startProperties) {
            startNodeData[property.first] = property.second;
        }
        for (auto& [key, value] : startProperties) {
            delete[] value;  // Free each allocated char* array
        }
        startProperties.clear();

        std::string destPid(destNode->getMetaPropertyHead()->value);
        destNodeData["partitionID"] = destPid;
        std::map<std::string, char*> destProperties = destNode->getAllProperties();
        for (auto property : destProperties) {
            destNodeData[property.first] = property.second;
        }
        for (auto& [key, value] : destProperties) {
            delete[] value;  // Free each allocated char* array
        }
        destProperties.clear();

        std::map<std::string, char*> relProperties = relation->getAllProperties();
        for (auto property : relProperties) {
            relationData[property.first] = property.second;
        }
        for (auto& [key, value] : relProperties) {
            delete[] value;  // Free each allocated char* array
        }
        relProperties.clear();

        json directionData;
        string start = query["sourceVariable"];
        string dest = query["destVariable"];
        string rel = query["relVariable"];

        if (isDirectionRight) {
            directionData[start] = startNodeData;
            directionData[dest] = destNodeData;
        } else if (!isDirected) {
            directionData[start] = destNodeData;
            directionData[dest] = startNodeData;
        }
        directionData[rel] = relationData;
        buffer.add(directionData.dump());
        count++;
    }

    int central = 1;
    for (long i = 1; i < centralRelationCount; i++) {
        json startNodeData;
        json destNodeData;
        json relationData;
        RelationBlock* relation = RelationBlock::getCentralRelation(i*RelationBlock::CENTRAL_BLOCK_SIZE);
        if (relation->getCentralRelationshipType() != query["relType"]) {
            continue;
        }

        std::string pid(relation->getMetaPropertyHead()->value);
        if (pid != to_string(gc.partitionID)) {
            continue;
        }

        NodeBlock* startNode = relation->getSource();
        NodeBlock* destNode = relation->getDestination();

        std::string startPid(startNode->getMetaPropertyHead()->value);
        startNodeData["partitionID"] = startPid;
        std::map<std::string, char*> startProperties = startNode->getAllProperties();
        for (auto property : startProperties) {
            startNodeData[property.first] = property.second;
        }
        for (auto& [key, value] : startProperties) {
            delete[] value;  // Free each allocated char* array
        }
        startProperties.clear();

        std::string destPid(destNode->getMetaPropertyHead()->value);
        destNodeData["partitionID"] = destPid;
        std::map<std::string, char*> destProperties = destNode->getAllProperties();
        for (auto property : destProperties) {
            destNodeData[property.first] = property.second;
        }
        for (auto& [key, value] : destProperties) {
            delete[] value;  // Free each allocated char* array
        }
        destProperties.clear();

        std::map<std::string, char*> relProperties = relation->getAllProperties();
        for (auto property : relProperties) {
            relationData[property.first] = property.second;
        }
        for (auto& [key, value] : relProperties) {
            delete[] value;  // Free each allocated char* array
        }
        relProperties.clear();

        json directionData;
        string start = query["sourceVariable"];
        string dest = query["destVariable"];
        string rel = query["relVariable"];

        if (isDirectionRight) {
            directionData[start] = startNodeData;
            directionData[dest] = destNodeData;
        } else if (!isDirected) {
            directionData[start] = destNodeData;
            directionData[dest] = startNodeData;
        }
        directionData[rel] = relationData;
        buffer.add(directionData.dump());

        central++;
    }
    buffer.add("-1");
}

void OperatorExecutor::DirectedAllRelationshipScan(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    NodeManager nodeManager(gc);
    string direction = query["direction"];
    const std::string& dbPrefix = nodeManager.getDbPrefix();
    long localRelationCount = nodeManager.dbSize(dbPrefix + "_relations.db") / RelationBlock::BLOCK_SIZE;
    long centralRelationCount = nodeManager.dbSize(dbPrefix +
                                                   "_central_relations.db") / RelationBlock::CENTRAL_BLOCK_SIZE;
    string graphDirection = Utils::getGraphDirection(to_string(gc.graphID), masterIP);
    bool isDirected = false;
    if (graphDirection == "TRUE") {
        isDirected = true;
    }
    bool isDirectionRight = query["direction"] == "right";
    int count = 1;
    for (long i = 1; i < localRelationCount; i++) {
        json startNodeData;
        json destNodeData;
        json relationData;
        RelationBlock* relation = RelationBlock::getLocalRelation(i * RelationBlock::BLOCK_SIZE);
        NodeBlock* startNode = relation->getSource();
        NodeBlock* destNode = relation->getDestination();

        std::string startPid(startNode->getMetaPropertyHead()->value);
        startNodeData["partitionID"] = startPid;
        std::map<std::string, char*> startProperties = startNode->getAllProperties();
        for (auto property : startProperties) {
            startNodeData[property.first] = property.second;
        }
        for (auto& [key, value] : startProperties) {
            delete[] value;  // Free each allocated char* array
        }
        startProperties.clear();

        std::string destPid(destNode->getMetaPropertyHead()->value);
        destNodeData["partitionID"] = destPid;
        std::map<std::string, char*> destProperties = destNode->getAllProperties();
        for (auto property : destProperties) {
            destNodeData[property.first] = property.second;
        }
        for (auto& [key, value] : destProperties) {
            delete[] value;  // Free each allocated char* array
        }
        destProperties.clear();

        std::map<std::string, char*> relProperties = relation->getAllProperties();
        for (auto property : relProperties) {
            relationData[property.first] = property.second;
        }
        for (auto& [key, value] : relProperties) {
            delete[] value;  // Free each allocated char* array
        }
        relProperties.clear();

        json directionData;
        string start = query["sourceVariable"];
        string dest = query["destVariable"];
        string rel = query["relVariable"];

        if (isDirectionRight) {
            directionData[start] = startNodeData;
            directionData[dest] = destNodeData;
        } else if (!isDirected) {
            directionData[start] = destNodeData;
            directionData[dest] = startNodeData;
        }
        directionData[rel] = relationData;
        buffer.add(directionData.dump());
        count++;
    }

    int central = 1;
    for (long i = 1; i < centralRelationCount; i++) {
        json startNodeData;
        json destNodeData;
        json relationData;
        RelationBlock* relation = RelationBlock::getCentralRelation(i * RelationBlock::CENTRAL_BLOCK_SIZE);
        std::string pid(relation->getMetaPropertyHead()->value);
        if (pid != to_string(gc.partitionID)) {
            continue;
        }

        NodeBlock* startNode = relation->getSource();
        NodeBlock* destNode = relation->getDestination();

        std::string startPid(startNode->getMetaPropertyHead()->value);
        startNodeData["partitionID"] = startPid;
        std::map<std::string, char*> startProperties = startNode->getAllProperties();
        for (auto property : startProperties) {
            startNodeData[property.first] = property.second;
        }
        for (auto& [key, value] : startProperties) {
            delete[] value;  // Free each allocated char* array
        }
        startProperties.clear();

        std::string destPid(destNode->getMetaPropertyHead()->value);
        destNodeData["partitionID"] = destPid;
        std::map<std::string, char*> destProperties = destNode->getAllProperties();
        for (auto property : destProperties) {
            destNodeData[property.first] = property.second;
        }
        for (auto& [key, value] : destProperties) {
            delete[] value;  // Free each allocated char* array
        }
        destProperties.clear();

        std::map<std::string, char*> relProperties = relation->getAllProperties();
        for (auto property : relProperties) {
            relationData[property.first] = property.second;
        }
        for (auto& [key, value] : relProperties) {
            delete[] value;  // Free each allocated char* array
        }
        relProperties.clear();

        json directionData;
        string start = query["sourceVariable"];
        string dest = query["destVariable"];
        string rel = query["relVariable"];

        if (isDirectionRight) {
            directionData[start] = startNodeData;
            directionData[dest] = destNodeData;
        } else if (!isDirected) {
            directionData[start] = destNodeData;
            directionData[dest] = startNodeData;
        }
        directionData[rel] = relationData;
        buffer.add(directionData.dump());

        central++;
    }
    buffer.add("-1");
}

void OperatorExecutor::NodeByIdSeek(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    NodeManager nodeManager(gc);
    NodeBlock* node = nodeManager.get(query["id"]);
    if (node) {
        json nodeData;
        std::string value(node->getMetaPropertyHead()->value);
        if (value == to_string(gc.partitionID)) {
            std::map<std::string, char*> properties = node->getAllProperties();
            nodeData["partitionID"] = value;
            for (auto property : properties) {
                nodeData[property.first] = property.second;
            }
            json data;
            string variable = query["variable"];
            data[variable] = nodeData;
            buffer.add(data.dump());
        }
    }
    buffer.add("-1");
}

void OperatorExecutor::ExpandAll(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    SharedBuffer sharedBuffer(INTER_OPERATOR_BUFFER_SIZE);
    std::string nextOpt = query["NextOperator"];
    json next = json::parse(nextOpt);
    auto method = OperatorExecutor::methodMap[next["Operator"]];
    // Launch the method in a new thread
    std::thread result(method, std::ref(*this), std::ref(sharedBuffer), query["NextOperator"], gc);

    string sourceVariable = query["sourceVariable"];
    string destVariable = query["destVariable"];
    string relVariable = query["relVariable"];

    string relType = "";
    if (query.contains("relType")) {
        relType = query["relType"];
    }
    string graphDirection = Utils::getGraphDirection(to_string(gc.graphID), masterIP);
    bool isDirected = false;
    if (graphDirection == "TRUE") {
        isDirected = true;
    }
    bool isDirectionRight = false;
    if (query.contains("direction")) {
        isDirectionRight = query["direction"] == "right";
    }

    string queryString;

    NodeManager nodeManager(gc);

    while (true) {
        string raw = sharedBuffer.get();
        if (raw == "-1") {
            buffer.add(raw);
            result.join();
            break;
        }
        json rawObj = json::parse(raw);
        string nodeId = rawObj[sourceVariable]["id"];
        if (rawObj[sourceVariable]["partitionID"] == to_string(gc.partitionID)) {
            NodeBlock* node = nodeManager.get(nodeId);
            if (node) {
                RelationBlock *relation = RelationBlock::getLocalRelation(node->edgeRef);
                if (relation) {
                    RelationBlock *nextRelation = relation;
                    bool isSource;
                    while (nextRelation) {
                        if (to_string(nextRelation->source.nodeId) == nodeId) {
                            isSource = true;
                        } else {
                            isSource = false;
                        }

                        json relationData;
                        json destNodeData;
                        std::map<std::string, char*> relProperties = nextRelation->getAllProperties();
                        for (auto property : relProperties) {
                            relationData[property.first] = property.second;
                        }

                        if (relType != "" && relationData["relationship"] != relType) {
                            if (isSource) {
                                nextRelation = nextRelation->nextLocalSource();
                            } else {
                                nextRelation = nextRelation->nextLocalDestination();
                            }
                            continue;
                        }
                        if (isDirected && !isSource) {
                            nextRelation = nextRelation->nextLocalDestination();
                            continue;
                        }
                        for (auto& [key, value] : relProperties) {
                            delete[] value;  // Free each allocated char* array
                        }
                        relProperties.clear();
                        NodeBlock *destNode;
                        if (isSource) {
                            destNode = nextRelation->getDestination();
                        } else {
                            destNode = nextRelation->getSource();
                        }
                        std::string value(destNode->getMetaPropertyHead()->value);
                        destNodeData["partitionID"] = value;
                        std::map<std::string, char*> destProperties = destNode->getAllProperties();
                        for (auto property : destProperties) {
                            destNodeData[property.first] = property.second;
                        }
                        for (auto& [key, value] : destProperties) {
                            delete[] value;  // Free each allocated char* array
                        }
                        destProperties.clear();
                        rawObj[relVariable] = relationData;
                        rawObj[destVariable] = destNodeData;

                        buffer.add(rawObj.dump());
                        if (isSource) {
                            nextRelation = nextRelation->nextLocalSource();
                        } else {
                            nextRelation = nextRelation->nextLocalDestination();
                        }
                    }
                }

                relation = RelationBlock::getCentralRelation(node->centralEdgeRef);
                if (relation) {
                    RelationBlock *nextRelation = relation;
                    bool isSource;

                    while (nextRelation) {
                        if (to_string(nextRelation->source.nodeId) == nodeId) {
                            isSource = true;
                        } else {
                            isSource = false;
                        }

                        json relationData;
                        json destNodeData;
                        std::map<std::string, char*> relProperties = nextRelation->getAllProperties();
                        for (auto property : relProperties) {
                            relationData[property.first] = property.second;
                        }

                        if (relType != "" && relationData["relationship"] != relType) {
                            if (isSource) {
                                nextRelation = nextRelation->nextCentralSource();
                            } else {
                                nextRelation = nextRelation->nextCentralDestination();
                            }
                            continue;
                        }

                        if (isDirected && !isSource) {
                            nextRelation = nextRelation->nextCentralDestination();
                            continue;
                        }

                        for (auto& [key, value] : relProperties) {
                            delete[] value;  // Free each allocated char* array
                        }
                        relProperties.clear();
                        NodeBlock *destNode;
                        if (isSource) {
                            destNode = nextRelation->getDestination();
                        } else {
                            destNode = nextRelation->getSource();
                        }
                        std::string value(destNode->getMetaPropertyHead()->value);
                        destNodeData["partitionID"] = value;
                        std::map<std::string, char*> destProperties = destNode->getAllProperties();
                        for (auto property : destProperties) {
                            destNodeData[property.first] = property.second;
                        }
                        for (auto& [key, value] : destProperties) {
                            delete[] value;  // Free each allocated char* array
                        }
                        destProperties.clear();
                        rawObj[relVariable] = relationData;
                        rawObj[destVariable] = destNodeData;
                        buffer.add(rawObj.dump());
                        if (isSource) {
                            nextRelation = nextRelation->nextCentralSource();
                        } else {
                            nextRelation = nextRelation->nextCentralDestination();
                        }
                    }
                }
            }
        } else {
            if (query.contains("relType")) {
                queryString = ExpandAllHelper::generateSubQuery(query["sourceVariable"],
                                                                query["destVariable"],
                                                                query["relVariable"],
                                                                isDirected,
                                                                rawObj[sourceVariable]["id"],
                                                                query["relType"]);
            } else {
                queryString = ExpandAllHelper::generateSubQuery(query["sourceVariable"],
                                                                query["destVariable"],
                                                                query["relVariable"],
                                                                isDirected,
                                                                rawObj[sourceVariable]["id"]);
            }
            string queryPlan = ExpandAllHelper::generateSubQueryPlan(queryString);
            SharedBuffer temp(INTER_OPERATOR_BUFFER_SIZE);
            std::thread t(Utils::sendDataFromWorkerToWorker,
                          masterIP,
                          gc.graphID,
                          rawObj[sourceVariable]["partitionID"],
                          std::ref(queryPlan),
                          std::ref(temp));
            while (true) {
                string tmpRaw = temp.get();
                if (tmpRaw == "-1") {
                    t.join();
                    break;
                }
                json tmpData = json::parse(tmpRaw);
                rawObj[relVariable] = tmpData[relVariable];
                rawObj[destVariable] = tmpData[destVariable];
                buffer.add(rawObj.dump());
            }
        }
    }
}

void OperatorExecutor::AggregationFunction(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    SharedBuffer sharedBuffer(INTER_OPERATOR_BUFFER_SIZE);
    std::string nextOpt = query["NextOperator"];
    json next = json::parse(nextOpt);
    auto method = OperatorExecutor::methodMap[next["Operator"]];
    // Launch the method in a new thread
    std::thread result(method, std::ref(*this), std::ref(sharedBuffer), query["NextOperator"], gc);
    AverageAggregationHelper* averageAggregationHelper =
            new AverageAggregationHelper(query["variable"], query["property"]);
    while (true) {
        string raw = sharedBuffer.get();
        if (raw == "-1") {
            buffer.add(averageAggregationHelper->getFinalResult());
            buffer.add(raw);
            result.join();
            break;
        }
        averageAggregationHelper->insertData(raw);
    }
}

void OperatorExecutor::Projection(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    SharedBuffer sharedBuffer(INTER_OPERATOR_BUFFER_SIZE);
    std::string nextOpt = query["NextOperator"];
    json next = json::parse(nextOpt);
    auto method = OperatorExecutor::methodMap[next["Operator"]];

    // Launch the method in a new thread
    std::thread result(method, std::ref(*this), std::ref(sharedBuffer), query["NextOperator"], gc);
    if (!query.contains("project") || !query["project"].is_array()) {
        while (true) {
            string raw = sharedBuffer.get();
            buffer.add(raw);
            if (raw == "-1") {
                result.join();
                break;
            }
        }
    } else {
        while (true) {
            string raw = sharedBuffer.get();
            if (raw == "-1") {
                buffer.add(raw);
                result.join();
                break;
            }
            auto data = json::parse(raw);
            for (const auto& operand : query["project"]) {
                for (auto& [key, value] : data.items()) {
                    if (operand.contains("variable") && key == operand["variable"]) {
                        string assign = operand["assign"];
                        string property = operand["property"];
                        data[assign] = value[property];
                    } else if (operand.contains("functionName") && key == operand["functionName"]) {
                        string assign = operand["assign"];
                        data["variable"] = assign;
                        data[assign] = value;
                    }
                }
            }
            buffer.add(data.dump());
        }
    }
}

void OperatorExecutor::Create(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    SharedBuffer sharedBuffer(INTER_OPERATOR_BUFFER_SIZE);
    string partitionAlgo = Utils::getPartitionAlgorithm(to_string(gc.graphID), masterIP);
    CreateHelper createHelper(query["elements"], partitionAlgo, gc, masterIP);
    if (query.contains("NextOperator")) {
        std::string nextOpt = query["NextOperator"];
        json next = json::parse(nextOpt);
        auto method = OperatorExecutor::methodMap[next["Operator"]];
        // Launch the method in a new thread
        std::thread result(method, std::ref(*this), std::ref(sharedBuffer), query["NextOperator"], gc);
        while (true) {
            string raw = sharedBuffer.get();
            if (raw == "-1") {
                buffer.add(raw);
                result.join();
                break;
            }
            createHelper.insertFromData(raw, std::ref(buffer));
        }
    } else {
        createHelper.insertWithoutData(std::ref(buffer));
        buffer.add("-1");
    }
}

void OperatorExecutor::CartesianProduct(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    SharedBuffer left(INTER_OPERATOR_BUFFER_SIZE);
    SharedBuffer right(INTER_OPERATOR_BUFFER_SIZE);
    std::string leftOpt = query["left"];
    std::string rightOpt = query["right"];
    json leftJson = json::parse(leftOpt);
    json rightJson = json::parse(rightOpt);
    auto leftMethod = OperatorExecutor::methodMap[leftJson["Operator"]];
    auto rightMethod = OperatorExecutor::methodMap[rightJson["Operator"]];
    // Launch the method in a new thread
    std::thread leftThread(leftMethod, std::ref(*this), std::ref(left), query["left"], gc);
    while (true) {
        string leftRaw = left.get();
        if (leftRaw == "-1") {
            buffer.add(leftRaw);
            leftThread.join();
            break;
        }

        string partitionCount = Utils::getJasmineGraphProperty("org.jasminegraph.server.npartitions");
        int numberOfPartitions = std::stoi(partitionCount);
        std::vector<std::thread> workerThreads;

        for (int i = 0; i < numberOfPartitions; i++) {
            if (i == gc.partitionID) {
                continue;
            }
            workerThreads.emplace_back(
                    Utils::sendDataFromWorkerToWorker,
                    masterIP,
                    gc.graphID,
                    to_string(i),
                    query["right"],
                    std::ref(right));
        }

        std::thread rightThread(rightMethod, std::ref(*this), std::ref(right), query["right"], gc);
        int count = 0;
        while (true) {
            string rightRaw = right.get();
            if (rightRaw == "-1") {
                count++;
                if (count == numberOfPartitions) {
                    buffer.add("-1");
                    rightThread.join();
                    for (auto& t : workerThreads) {
                        if (t.joinable()) {
                            t.join();
                        }
                    }
                }
                continue;
            }



            json leftData = json::parse(leftRaw);
            json rightData = json::parse(rightRaw);

            for (auto& [key, value] : rightData.items()) {
                leftData[key] = value;
            }
            buffer.add(leftData.dump());
        }
    }
}

void OperatorExecutor::Distinct(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    SharedBuffer sharedBuffer(INTER_OPERATOR_BUFFER_SIZE);
    std::string nextOpt = query["NextOperator"];
    json next = json::parse(nextOpt);
    auto method = OperatorExecutor::methodMap[next["Operator"]];

    // Launch the method in a new thread
    std::thread result(method, std::ref(*this), std::ref(sharedBuffer), query["NextOperator"], gc);
    if (!query.contains("project") || !query["project"].is_array()) {
        while (true) {
            string raw = sharedBuffer.get();
            buffer.add(raw);
            if (raw == "-1") {
                result.join();
                break;
            }
        }
    } else {
        while (true) {
            string raw = sharedBuffer.get();
            if (raw == "-1") {
                buffer.add(raw);
                result.join();
                break;
            }
            auto data = json::parse(raw);
            for (const auto& operand : query["project"]) {
                for (auto& [key, value] : data.items()) {
                    if (operand.contains("variable") && key == operand["variable"]) {
                        string assign = operand["assign"];
                        string property = operand["property"];
                        data[assign] = value[property];
                    } else if (operand.contains("functionName") && key == operand["functionName"]) {
                        string assign = operand["assign"];
                        data["variable"] = assign;
                        data[assign] = value;
                    }
                }
            }
            buffer.add(data.dump());
        }
    }
}

struct Row {
    json data;
    std::string jsonStr;
    std::string sortKey;
    bool isAsc;
    size_t sourceFileIndex;

    Row(const std::string& str, const std::string& key, bool asc, size_t srcIdx = 0)
            : jsonStr(str), sortKey(key), isAsc(asc), sourceFileIndex(srcIdx) {
        data = json::parse(str);
    }

    size_t memoryUsage() const {
        // Rough estimate: strings + json overhead
        return sizeof(Row)
               + jsonStr.size()
               + sortKey.size()
               + sizeof(bool)
               + sizeof(size_t);
    }

    json getNestedValue(const json& obj, const std::string& key) const {
        json current = obj;
        std::stringstream ss(key);
        std::string token;

        while (std::getline(ss, token, '.')) {
            if (!current.contains(token)) {
                return nullptr;
            }
            current = current[token];
        }
        return current;
    }

    bool operator<(const Row& other) const {
        json val1 = getNestedValue(data, sortKey);
        json val2 = getNestedValue(other.data, sortKey);

        bool result;
        if (val1.is_number_integer() && val2.is_number_integer()) {
            result = val1.get<int>() > val2.get<int>();
        } else if (val1.is_string() && val2.is_string()) {
            result = val1.get<std::string>() > val2.get<std::string>();
        } else {
            result = val1.dump() > val2.dump();
        }
        return isAsc ? result : !result;  // Flip for DESC
    }
};

// Define comparator for ASC
struct RowAscComparator {
    bool operator()(const Row& a, const Row& b) const {
        return a < b;  // Uses your Row::operator<
    }
};

// Define comparator for DESC
struct RowDescComparator {
    bool operator()(const Row& a, const Row& b) const {
        return b < a;  // Reverses your Row::operator<
    }
};

struct DynamicComparator {
    bool isAsc;
    bool operator()(const Row& a, const Row& b) const {
        return isAsc ? RowAscComparator {}(a, b) : RowDescComparator{}(a, b);
    }
};

std::string generateUniqueFilename() {
    auto now = std::chrono::system_clock::now();
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch()).count();

    std::stringstream uuid;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 15);

    for (int i = 0; i < 8; i++) {
        uuid << std::hex << dis(gen);
    }

    std::ostringstream filename;
    filename << "run_" << millis << "_" << uuid.str() << ".dat";
    return filename.str();
}

// Flush heap into a run file
void flushHeapToRunFile(
        std::priority_queue<Row, std::vector<Row>, DynamicComparator> &pq,
        std::vector<std::string> &runFiles
) {
    if (pq.empty()) return;

    std::string filename = generateUniqueFilename();
    std::ofstream outFile(filename, std::ios::out | std::ios::binary);

    if (!outFile) {
        execution_logger.error("OrderBy: Failed to create run file: " + filename);
        return;
    }

    execution_logger.info("OrderBy: Flushing heap to run file: " + filename);

    while (!pq.empty()) {
        const Row &row = pq.top();
        outFile << row.jsonStr << "\n";
        pq.pop();
    }

    outFile.close();
    runFiles.push_back(filename);
    execution_logger.info("OrderBy: Finished flushing heap to run file: " + filename);
}

void OperatorExecutor::OrderBy(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    SharedBuffer sharedBuffer(INTER_OPERATOR_BUFFER_SIZE);
    std::string nextOpt = query["NextOperator"];
    json next = json::parse(nextOpt);
    auto method = OperatorExecutor::methodMap[next["Operator"]];

    // Launch the method in a new thread
    std::thread result(method, std::ref(*this), std::ref(sharedBuffer), query["NextOperator"], gc);

    std::string sortKey = query["variable"];
    std::string order = query["order"];
    const size_t MAX_MEMORY_BYTES = 1024 * 1024;  // 1 MB
    size_t heapMemoryUsage = 0;
    bool isAsc = (order == "ASC");

    auto heap = std::priority_queue<Row, std::vector<Row>, DynamicComparator>(
            DynamicComparator{isAsc});
    std::vector<std::string> runFiles;  // store generated run file paths
    size_t runCounter = 0;

    auto localKWayMergeToBuffer = [&](
            const std::vector<std::string> &runFiles,
                                      SharedBuffer &buffer,
                                      bool isAsc,
                                      const std::string &sortKey) {
        // Comparator for heap
        auto cmp = [&](const Row &a, const Row &b) {
            json val1 = a.getNestedValue(a.data, sortKey);
            json val2 = b.getNestedValue(b.data, sortKey);

            bool result;
            if (val1.is_number_integer() && val2.is_number_integer()) {
                result = val1.get<int>() > val2.get<int>();
            } else if (val1.is_string() && val2.is_string()) {
                result = val1.get<std::string>() > val2.get<std::string>();
            } else {
                result = val1.dump() > val2.dump();
            }
            return isAsc ? result : !result;
        };

        std::priority_queue<Row, std::vector<Row>, decltype(cmp)> pq(cmp);
        std::vector<std::ifstream> files(runFiles.size());

        // push first row from each run
        for (size_t i = 0; i < runFiles.size(); ++i) {
            files[i].open(runFiles[i]);
            std::string line;
            if (std::getline(files[i], line)) {
                pq.emplace(line, sortKey, isAsc, i);
            }
        }

        // Merge directly to buffer
        while (!pq.empty()) {
            Row top = pq.top();
            pq.pop();
            buffer.add(top.jsonStr);

            size_t fileIndex = top.sourceFileIndex;
            std::string line;
            if (std::getline(files[fileIndex], line)) {
                pq.emplace(line, sortKey, isAsc, fileIndex);
            }
        }
        buffer.add("-1");  // close flag
        for (auto &f : files) {
            f.close();
        }
        for (const auto &file : runFiles) {
            if (std::remove(file.c_str()) != 0) {
                execution_logger.warn("OrderBy: Failed to delete temporary run file: " + file);
            } else {
                execution_logger.info("OrderBy: Deleted temporary run file: " + file);
            }
        }
    };

    while (true) {
        std::string jsonStr = sharedBuffer.get();
        if (jsonStr == "-1") {
            // Final flush: If heap still has data, either to run file (if other runs exist) or directly to buffer
            if (!runFiles.empty()) {
                flushHeapToRunFile(heap, runFiles);
                localKWayMergeToBuffer(runFiles, buffer, isAsc, sortKey);
            } else {
                while (!heap.empty()) {
                    buffer.add(heap.top().jsonStr);
                    heap.pop();
                }
                buffer.add(jsonStr);  // -1 close flag
                result.join();
                break;
            }
        }

        try {
            Row row(jsonStr, sortKey, isAsc);
            json nestedVal = row.getNestedValue(row.data, sortKey);

            if (nestedVal.is_null()) {
                execution_logger.warn("OrderBy: Sort key '" + sortKey + "' not found in row: " + jsonStr);
                continue;
            }
            heapMemoryUsage += row.memoryUsage();
            heap.push(row);
            if (heapMemoryUsage > MAX_MEMORY_BYTES) {
                execution_logger.info("OrderBy: Heap memory exceeded limit (" + std::to_string(heapMemoryUsage)
                + " bytes), flushing to runfile");
                flushHeapToRunFile(heap, runFiles);
                heapMemoryUsage = 0;  // reset after flush
            }
        } catch (const std::exception& e) {
            execution_logger.error("OrderBy: Error parsing row: " + std::string(e.what()));
        }
    }
}
