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

    methodMap["NodeByIdSeek"] = [](OperatorExecutor &executor, SharedBuffer &buffer,
                                                    std::string jsonPlan, GraphConfig gc) {
        executor.NodeByIdSeek(buffer, jsonPlan, gc);
    };

    methodMap["Projection"] = [](OperatorExecutor &executor, SharedBuffer &buffer,
            std::string jsonPlan, GraphConfig gc) {
        executor.Projection(buffer, jsonPlan, gc);
    };

    methodMap["EagerFunction"] = [](OperatorExecutor &executor, SharedBuffer &buffer,
                                   std::string jsonPlan, GraphConfig gc) {
        executor.EargarAggregation(buffer, jsonPlan, gc);
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
    long localRelationCount = nodeManager.dbSize(dbPrefix + "_relations.db") / RelationBlock::BLOCK_SIZE;
    long centralRelationCount = nodeManager.dbSize(dbPrefix +
                                                    "_central_relations.db") / RelationBlock::CENTRAL_BLOCK_SIZE;
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

        json leftDirectionData;
        leftDirectionData[start] = destNodeData;
        leftDirectionData[dest] = startNodeData;
        leftDirectionData[rel] = relationData;
        buffer.add(leftDirectionData.dump());
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

        json leftDirectionData;
        leftDirectionData[start] = destNodeData;
        leftDirectionData[dest] = startNodeData;
        leftDirectionData[rel] = relationData;
        buffer.add(leftDirectionData.dump());
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
    string queryString;

    NodeManager nodeManager(gc);

    while (true) {
        string raw = sharedBuffer.get();          // Get raw JSON string

        if (raw == "-1") {
            buffer.add(raw);
            result.join();
            break;
        }
        json rawObj = json::parse(raw);           // Parse it into a json object
        string nodeId = rawObj[sourceVariable]["id"];
        if (rawObj[sourceVariable]["partitionID"] == to_string(gc.partitionID)) {
            NodeBlock* node = nodeManager.get(nodeId);
            if (node) {
                RelationBlock *relation = RelationBlock::getLocalRelation(node->edgeRef);
                if (relation) {
                    RelationBlock *nextRelation = relation;
                    RelationBlock *prevRelation;
                    bool isSource;

                    if (to_string(relation->source.nodeId) == nodeId) {
                        prevRelation =  relation->previousLocalSource();
                    } else {
                        prevRelation =  relation->previousLocalDestination();
                    }

                    int t = 1;
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
                        t++;
                        buffer.add(rawObj.dump());
                        if (isSource) {
                            nextRelation = nextRelation->nextLocalSource();
                        } else {
                            nextRelation = nextRelation->nextLocalDestination();
                        }
                    }
                    int p = 1;
                    while (prevRelation) {
                        if (to_string(prevRelation->source.nodeId) == nodeId) {
                            isSource = true;
                        } else {
                            isSource = false;
                        }

                        json relationData;
                        json destNodeData;
                        std::map<std::string, char*> relProperties = prevRelation->getAllProperties();
                        for (auto property : relProperties) {
                            relationData[property.first] = property.second;
                        }

                        if (relType != "" && relationData["relationship"] != relType) {
                            if (isSource) {
                                prevRelation = prevRelation->previousLocalSource();
                            } else {
                                prevRelation = prevRelation->previousLocalDestination();
                            }
                            continue;
                        }

                        for (auto& [key, value] : relProperties) {
                            delete[] value;  // Free each allocated char* array
                        }
                        relProperties.clear();
                        NodeBlock *destNode;
                        if (isSource) {
                            destNode = prevRelation->getDestination();
                        } else {
                            destNode = prevRelation->getSource();
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
                        p++;
                        buffer.add(rawObj.dump());
                        if (isSource) {
                            prevRelation = prevRelation->previousLocalSource();
                        } else {
                            prevRelation = prevRelation->previousLocalDestination();
                        }
                    }
                } else {
                }
                relation = RelationBlock::getCentralRelation(node->centralEdgeRef);
                if (relation) {
                    RelationBlock *nextRelation = relation;
                    RelationBlock *prevRelation;
                    bool isSource;

                    if (to_string(relation->source.nodeId) == nodeId) {
                        prevRelation =  relation->previousCentralSource();
                    } else {
                        prevRelation =  relation->previousCentralDestination();
                    }
                    int s = 1;
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
                        s++;
                        buffer.add(rawObj.dump());
                        if (isSource) {
                            nextRelation = nextRelation->nextCentralSource();
                        } else {
                            nextRelation = nextRelation->nextCentralDestination();
                        }
                    }

                    int r = 1;
                    while (prevRelation) {
                        if (to_string(prevRelation->source.nodeId) == nodeId) {
                            isSource = true;
                        } else {
                            isSource = false;
                        }

                        json relationData;
                        json destNodeData;
                        std::map<std::string, char*> relProperties = prevRelation->getAllProperties();
                        for (auto property : relProperties) {
                            relationData[property.first] = property.second;
                        }
                        if (relType != "" && relationData["relationship"] != relType) {
                            if (isSource) {
                                prevRelation = prevRelation->previousCentralSource();
                            } else {
                                prevRelation = prevRelation->previousCentralDestination();
                            }
                            continue;
                        }
                        for (auto& [key, value] : relProperties) {
                            delete[] value;  // Free each allocated char* array
                        }
                        relProperties.clear();
                        NodeBlock *destNode;
                        if (isSource) {
                            destNode = prevRelation->getDestination();
                        } else {
                            destNode = prevRelation->getSource();
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
                        r++;
                        buffer.add(rawObj.dump());
                        if (isSource) {
                            prevRelation = prevRelation->previousCentralSource();
                        } else {
                            prevRelation = prevRelation->previousCentralDestination();
                        }
                    }
                }
            }
        } else {
            if (query.contains("relType")) {
                queryString = ExpandAllHelper::generateSubQuery(query["sourceVariable"],
                                                                query["destVariable"],
                                                                query["relVariable"],
                                                                rawObj[sourceVariable]["id"],
                                                                query["relType"]);
            } else {
                queryString = ExpandAllHelper::generateSubQuery(query["sourceVariable"],
                                                                query["destVariable"],
                                                                query["relVariable"],
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

void OperatorExecutor::EargarAggregation(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
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

    Row(const std::string& str, const std::string& key, bool asc)
        : jsonStr(str), sortKey(key), isAsc(asc) {
        data = json::parse(str);
    }

    bool operator<(const Row& other) const {
        const auto& val1 = data[sortKey];
        const auto& val2 = other.data[sortKey];

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
    const size_t maxSize = 5000;
    bool isAsc = (order == "ASC");

    std::priority_queue<Row> heap;
    while (true) {
        std::string jsonStr = sharedBuffer.get();
        if (jsonStr == "-1") {
            while (!heap.empty()) {
                buffer.add(heap.top().jsonStr);
                heap.pop();
            }
            buffer.add(jsonStr);  // -1 close flag
            result.join();
            break;
        }

        try {
            Row row(jsonStr, sortKey, isAsc);
            execution_logger.info(row.jsonStr);
            if (row.data.contains(sortKey)) {  // Ensure field exists
                heap.push(row);
                if (heap.size() > maxSize) {
                    heap.pop();  // Remove smallest (ASC) or largest (DESC)
                }
            }
        } catch (const std::exception& e) {
            std::cerr << "Error parsing JSON: " << e.what() << "\n";
        }
    }
}
