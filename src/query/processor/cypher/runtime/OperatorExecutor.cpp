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
#include "Helpers.h"
#include <thread>

std::unordered_map<std::string, std::function<void(OperatorExecutor&, SharedBuffer&,
        std::string, GraphConfig)>> OperatorExecutor::methodMap;
OperatorExecutor::OperatorExecutor(GraphConfig gc, std::string queryPlan):
    queryPlan(queryPlan), gc(gc) {
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

    methodMap["NodeByIdSeek"] = [](OperatorExecutor &executor, SharedBuffer &buffer,
                                                    std::string jsonPlan, GraphConfig gc) {
        executor.NodeByIdSeek(buffer, jsonPlan, gc);
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

void OperatorExecutor::ProduceResult(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    SharedBuffer sharedBuffer(INTER_OPERATOR_BUFFER_SIZE);
    std::string nextOpt = query["NextOperator"];
    json next = json::parse(nextOpt);
    auto method = OperatorExecutor::methodMap[next["Operator"]];
    // Launch the method in a new thread
    std::thread result(method, std::ref(*this), std::ref(sharedBuffer), query["NextOperator"], gc);
    result.detach();  // Detach the thread to let it run independently

    while (true) {
        string raw = sharedBuffer.get();
        if (raw == "-1") {
            buffer.add(raw);
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
    result.detach();  // Detach the thread to let it run independently

    auto condition = query["condition"];
    FilterHelper FilterHelper(condition.dump());
    while (true) {
        string raw = sharedBuffer.get();
        cout << raw << endl;
        if (raw == "-1") {
            buffer.add(raw);
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
    for (auto it : nodeManager.nodeIndex) {
        json nodeData;
        auto nodeId = it.first;
        NodeBlock *node = nodeManager.get(nodeId);
        std::string value(node->getMetaPropertyHead()->value);
        if (value == to_string(gc.partitionID)) {
            std::map<std::string, char*> properties = node->getAllProperties();
            for (auto property : properties) {
                nodeData[property.first] = property.second;
            }
            json data;
            string variable = query["relType"];
            data[variable] = nodeData;
            buffer.add(data.dump());
        }
    }
    buffer.add("-1");
}

void OperatorExecutor::UndirectedAllRelationshipScan(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    NodeManager nodeManager(gc);

    const std::string& dbPrefix = nodeManager.getDbPrefix();
    long localRelationCount = nodeManager.dbSize(dbPrefix + "_relations.db") / RelationBlock::BLOCK_SIZE - 1;
    long centralRelationCount = nodeManager.dbSize(dbPrefix +
                                                    "_central_relations.db") / RelationBlock::CENTRAL_BLOCK_SIZE - 1;

    for (long i = 1; i < localRelationCount; i++) {
        json startNodeData;
        json destNodeData;
        json relationData;
        RelationBlock* relation = RelationBlock::getLocalRelation(i*RelationBlock::BLOCK_SIZE);
        NodeBlock* startNode = relation->getSource();
        NodeBlock* destNode = relation->getDestination();

        std::map<std::string, char*> startProperties = startNode->getAllProperties();
        for (auto property : startProperties) {
            startNodeData[property.first] = property.second;
        }
        for (auto& [key, value] : startProperties) {
            delete[] value;  // Free each allocated char* array
        }
        startProperties.clear();

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

        json data;
        string start = query["sourceVariable"];
        string dest = query["destVariable"];
        string rel = query["relVariable"];

        data[start] = startNodeData;
        data[dest] = destNodeData;
        data[rel] = relationData;
        buffer.add(data.dump());
    }

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

        std::map<std::string, char*> startProperties = startNode->getAllProperties();
        for (auto property : startProperties) {
            startNodeData[property.first] = property.second;
        }
        for (auto& [key, value] : startProperties) {
            delete[] value;  // Free each allocated char* array
        }
        startProperties.clear();

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

        json data;
        string start = query["sourceVariable"];
        string dest = query["destVariable"];
        string rel = query["relVariable"];

        data[start] = startNodeData;
        data[dest] = destNodeData;
        data[rel] = relationData;
        buffer.add(data.dump());
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
    result.detach();  // Detach the thread to let it run independently

    while (true) {
        string raw = sharedBuffer.get();
        buffer.add(raw);
        if (raw == "-1") {
            break;
        }
    }
}
