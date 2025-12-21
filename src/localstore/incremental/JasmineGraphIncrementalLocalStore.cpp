/**
Copyright 2021 JasminGraph Team
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

#include "JasmineGraphIncrementalLocalStore.h"

#include <algorithm>
#include <memory>
#include <stdexcept>
#include <unordered_set>

#include "../../nativestore/RelationBlock.h"
#include "../../nativestore/MetaPropertyLink.h"
#include "../../nativestore/temporal/TemporalConstants.h"
#include "../../util/Utils.h"
#include "../../util/logger/Logger.h"

Logger incremental_localstore_logger;

namespace {
const std::string DEFAULT_OPERATION = TemporalConstants::OP_ADD;
const std::string LOCAL_EDGE_TYPE = "Local";

bool isLocalEdge(const json &edgeJson) {
    if (!edgeJson.contains("EdgeType")) {
        return false;
    }
    try {
        return edgeJson["EdgeType"].get<std::string>() == LOCAL_EDGE_TYPE;
    } catch (const std::exception &) {
        return false;
    }
}
}  // namespace

JasmineGraphIncrementalLocalStore::JasmineGraphIncrementalLocalStore(unsigned int graphID, unsigned int partitionID,
                                                                     std::string openMode) {
    gc.graphID = graphID;
    gc.partitionID = partitionID;
    gc.maxLabelSize = std::stoi(Utils::getJasmineGraphProperty("org.jasminegraph.nativestore.max.label.size"));
    gc.openMode = openMode;
    this->nm = new NodeManager(gc);
    this->temporalLogger = std::make_unique<TemporalEventLogger>(this->nm->getDbPrefix());
};

std::pair<std::string, unsigned int> JasmineGraphIncrementalLocalStore::getIDs(std::string edgeString) {
    try {
        auto edgeJson = json::parse(edgeString);
        if (edgeJson.contains("properties")) {
            auto edgeProperties = edgeJson["properties"];
            return {edgeProperties["graphId"], edgeJson["PID"]};
        }
    } catch (const std::exception& e) {  // TODO tmkasun: Handle multiple types of exceptions
        incremental_localstore_logger.log(
            "Error while processing edge data = " + std::string(e.what()) +
                "Could be due to JSON parsing error or error while persisting the data to disk",
            "error");
    }
    return {"", 0};  // all plath of the function must return std::pair<std::string, unsigned int>
    // type object even there is an error
}


void JasmineGraphIncrementalLocalStore::addEdgeFromString(std::string edgeString) {
    try {
        auto edgeJson = json::parse(edgeString);
        incremental_localstore_logger.info(edgeString);

        std::string operationType = resolveOperationType(edgeJson);
        std::string operationTimestamp = resolveOperationTimestamp(edgeJson);
        bool handled = false;

        if (edgeJson.contains("isNode")) {
            handled = handleNodeOperation(edgeJson, operationType, operationTimestamp);
        } else {
            bool localEdge = isLocalEdge(edgeJson);
            if (operationType == TemporalConstants::OP_ADD) {
                handled = handleEdgeAddition(edgeJson, operationType, operationTimestamp);
            } else if (operationType == TemporalConstants::OP_DELETE) {
                handled = handleEdgeDeletion(edgeJson, localEdge, operationType, operationTimestamp);
            } else if (operationType == TemporalConstants::OP_UPDATE) {
                handled = handleEdgeUpdate(edgeJson, localEdge, operationType, operationTimestamp);
            } else {
                incremental_localstore_logger.warn("Unsupported operation type '" + operationType +
                                                  "' received. Defaulting to ADD.");
                handled = handleEdgeAddition(edgeJson, DEFAULT_OPERATION, operationTimestamp);
                operationType = DEFAULT_OPERATION;
            }
        }

        logTemporalEvent(edgeJson, operationType, operationTimestamp);

        if (!handled) {
            incremental_localstore_logger.warn("Failed to handle operation '" + operationType +
                                              "' for payload: " + edgeString);
        }
    } catch (const std::exception&) {  // TODO tmkasun: Handle multiple types of exceptions
        incremental_localstore_logger.log(
                "Error while processing edge data = " + edgeString +
                "Could be due to JSON parsing error or error while persisting the data to disk",
                "error");
        incremental_localstore_logger.log("Error malformed JSON attributes!", "error");
        // TODO tmkasun: handle JSON errors
    }
}

void JasmineGraphIncrementalLocalStore::addLocalEdge(std::string edge) {
    auto jsonEdge = json::parse(edge);
    auto jsonSource = jsonEdge["source"];
    auto jsonDestination = jsonEdge["destination"];

    std::string sId = std::string(jsonSource["id"]);
    std::string dId = std::string(jsonDestination["id"]);

    RelationBlock* newRelation;
    newRelation = this->nm->addLocalEdge({sId, dId});

    if (!newRelation) {
        return;
    }

    addLocalEdgeProperties(newRelation, jsonEdge);
    addSourceProperties(newRelation, jsonSource);
    addDestinationProperties(newRelation, jsonDestination);

    incremental_localstore_logger.debug("Local edge (" + sId + "-> " + dId + " ) added successfully");
}

void JasmineGraphIncrementalLocalStore::addCentralEdge(std::string edge) {
    auto jsonEdge = json::parse(edge);
    auto jsonSource = jsonEdge["source"];
    auto jsonDestination = jsonEdge["destination"];

    std::string sId = std::string(jsonSource["id"]);
    std::string dId = std::string(jsonDestination["id"]);

    RelationBlock* newRelation;
    newRelation = this->nm->addCentralEdge({sId, dId});

    if (!newRelation) {
        return;
    }

    addCentralEdgeProperties(newRelation, jsonEdge);
    addSourceProperties(newRelation, jsonSource);
    addDestinationProperties(newRelation, jsonDestination);

    incremental_localstore_logger.debug("Central edge (" + sId + "-> " + dId + " ) added successfully");
}

void JasmineGraphIncrementalLocalStore::addCentralEdgeProperties(RelationBlock* relationBlock, const json& edgeJson) {
    char value[PropertyLink::MAX_VALUE_SIZE] = {};
    char type[RelationBlock::MAX_TYPE_SIZE] = {0};
    if (edgeJson.contains("properties")) {
        auto edgeProperties = edgeJson["properties"];
        for (auto it = edgeProperties.begin(); it != edgeProperties.end(); it++) {
            strcpy(value, it.value().get<std::string>().c_str());
            if (std::string(it.key()) == "type") {
                strcpy(type, it.value().get<std::string>().c_str());
                relationBlock->addCentralRelationshipType(&type[0]);
            }
            relationBlock->addCentralProperty(std::string(it.key()), &value[0]);
        }
    }
    std::string edgePid = std::to_string(edgeJson["source"]["pid"].get<int>());
    addRelationMetaProperty(relationBlock, MetaPropertyEdgeLink::PARTITION_ID, edgePid);
}

void JasmineGraphIncrementalLocalStore::addLocalEdgeProperties(RelationBlock* relationBlock, const json& edgeJson) {
    char value[PropertyLink::MAX_VALUE_SIZE] = {};
    char type[RelationBlock::MAX_TYPE_SIZE] = {0};
    if (edgeJson.contains("properties")) {
        auto edgeProperties = edgeJson["properties"];
        for (auto it = edgeProperties.begin(); it != edgeProperties.end(); it++) {
            strcpy(value, it.value().get<std::string>().c_str());
            if (std::string(it.key()) == "type") {
                strcpy(type, it.value().get<std::string>().c_str());
                relationBlock->addLocalRelationshipType(&type[0]);
            }
            relationBlock->addLocalProperty(std::string(it.key()), &value[0]);
        }
    }
}

void JasmineGraphIncrementalLocalStore::addSourceProperties(RelationBlock* relationBlock, const json& sourceJson) {
    char value[PropertyLink::MAX_VALUE_SIZE] = {};
    char label[NodeBlock::LABEL_SIZE] = {0};
    if (sourceJson.contains("properties")) {
        auto sourceProps = sourceJson["properties"];
        for (auto it = sourceProps.begin(); it != sourceProps.end(); it++) {
            strcpy(value, it.value().get<std::string>().c_str());
            if (std::string(it.key()) == "label") {
                strcpy(label, it.value().get<std::string>().c_str());
                relationBlock->getSource()->addLabel(&label[0]);
            }
            relationBlock->getSource()->addProperty(std::string(it.key()), &value[0]);
        }
    }
    std::string sourcePid = std::to_string(sourceJson["pid"].get<int>());
    addNodeMetaProperty(relationBlock->getSource(), MetaPropertyLink::PARTITION_ID,
                        sourcePid);
}

void JasmineGraphIncrementalLocalStore::addDestinationProperties(RelationBlock* relationBlock,
    const json& destinationJson) {
    char value[PropertyLink::MAX_VALUE_SIZE] = {};
    char label[NodeBlock::LABEL_SIZE] = {0};
    if (destinationJson.contains("properties")) {
        auto destinationProps = destinationJson["properties"];
        for (auto it = destinationProps.begin(); it != destinationProps.end(); it++) {
            strcpy(value, it.value().get<std::string>().c_str());
            if (std::string(it.key()) == "label") {
                strcpy(label, it.value().get<std::string>().c_str());
                relationBlock->getDestination()->addLabel(&label[0]);
            }
            relationBlock->getDestination()->addProperty(std::string(it.key()), &value[0]);
        }
    }
    std::string destPId = std::to_string(destinationJson["pid"].get<int>());
    addNodeMetaProperty(relationBlock->getDestination(), MetaPropertyLink::PARTITION_ID,
                        destPId);
}

void JasmineGraphIncrementalLocalStore::addNodeMetaProperty(NodeBlock* nodeBlock,
                                                        std::string propertyKey, std::string propertyValue) {
    char meta[MetaPropertyLink::MAX_VALUE_SIZE] = {};
    strcpy(meta, propertyValue.c_str());
    nodeBlock->addMetaProperty(propertyKey, &meta[0]);
}

void JasmineGraphIncrementalLocalStore::addRelationMetaProperty(RelationBlock* relationBlock,
                                                        std::string propertyKey, std::string propertyValue) {
    char meta[MetaPropertyEdgeLink::MAX_VALUE_SIZE] = {};
    strcpy(meta, propertyValue.c_str());
    relationBlock->addMetaProperty(propertyKey, &meta[0]);
}

std::string JasmineGraphIncrementalLocalStore::resolveOperationType(const json& edgeJson) const {
    static const std::unordered_set<std::string> allowedOps = {TemporalConstants::OP_ADD,
                                                               TemporalConstants::OP_DELETE,
                                                               TemporalConstants::OP_UPDATE};
    auto resolveFromObject = [&](const json& container) -> std::string {
        if (container.is_null()) {
            return "";
        }
        if (container.contains("operationType") && container["operationType"].is_string()) {
            return container["operationType"].get<std::string>();
        }
        return "";
    };

    std::string op = resolveFromObject(edgeJson);
    if (op.empty() && edgeJson.contains("properties")) {
        op = resolveFromObject(edgeJson["properties"]);
    }
    if (op.empty()) {
        return DEFAULT_OPERATION;
    }

    op = Utils::trim_copy(op);
    std::transform(op.begin(), op.end(), op.begin(), ::toupper);
    if (allowedOps.find(op) == allowedOps.end()) {
        incremental_localstore_logger.warn("Unsupported operation type '" + op + "'. Using ADD.");
        return DEFAULT_OPERATION;
    }
    return op;
}

std::string JasmineGraphIncrementalLocalStore::resolveOperationTimestamp(const json& edgeJson) const {
    auto resolveFromObject = [&](const json& container) -> std::string {
        if (container.is_null()) {
            return "";
        }
        // Accept both string and numeric timestamps
        if (container.contains("operationTimestamp")) {
            if (container["operationTimestamp"].is_string()) {
                return container["operationTimestamp"].get<std::string>();
            } else if (container["operationTimestamp"].is_number()) {
                return std::to_string(container["operationTimestamp"].get<long long>());
            }
        }
        if (container.contains("timestamp")) {
            if (container["timestamp"].is_string()) {
                return container["timestamp"].get<std::string>();
            } else if (container["timestamp"].is_number()) {
                return std::to_string(container["timestamp"].get<long long>());
            }
        }
        return "";
    };

    std::string ts = resolveFromObject(edgeJson);
    if (ts.empty() && edgeJson.contains("properties")) {
        ts = resolveFromObject(edgeJson["properties"]);
    }
    if (ts.empty()) {
        // Use UTC epoch milliseconds for consistency
        ts = TemporalConstants::epochMillisToString(TemporalConstants::getCurrentEpochMillis());
    }
    return ts;
}

void JasmineGraphIncrementalLocalStore::logTemporalEvent(const json& edgeJson, const std::string& operationType,
                                                         const std::string& operationTimestamp) {
    if (!temporalLogger) {
        return;
    }
    TemporalEdgeEvent event{operationType, operationTimestamp, edgeJson};
    temporalLogger->log(event);
}

bool JasmineGraphIncrementalLocalStore::handleNodeOperation(const json& nodeJson, const std::string& operationType,
                                                            const std::string& operationTimestamp) {
    std::string nodeId = nodeJson["id"].get<std::string>();
    
    if (operationType == TemporalConstants::OP_ADD) {
        NodeBlock* newNode = this->nm->addNode(nodeId);
        if (!newNode) {
            incremental_localstore_logger.error("Failed to allocate node block for node " + nodeId);
            return false;
        }

        char value[PropertyLink::MAX_VALUE_SIZE] = {};
        char meta[MetaPropertyLink::MAX_VALUE_SIZE] = {};

        if (nodeJson.contains("properties")) {
            auto sourceProps = nodeJson["properties"];
            for (auto it = sourceProps.begin(); it != sourceProps.end(); it++) {
                strcpy(value, it.value().get<std::string>().c_str());
                newNode->addProperty(std::string(it.key()), &value[0]);
            }
        }

        std::string sourcePid = std::to_string(nodeJson["pid"].get<int>());
        strcpy(meta, sourcePid.c_str());
        newNode->addMetaProperty(MetaPropertyLink::PARTITION_ID, &meta[0]);
        addNodeMetaProperty(newNode, TemporalConstants::LAST_OPERATION, operationType);
        addNodeMetaProperty(newNode, TemporalConstants::LAST_OPERATION_TS, operationTimestamp);
        addNodeMetaProperty(newNode, TemporalConstants::STATUS, TemporalConstants::STATUS_ACTIVE);
        addNodeMetaProperty(newNode, TemporalConstants::CREATED_AT, operationTimestamp);
        addNodeMetaProperty(newNode, TemporalConstants::PROPERTY_VERSION, "1");
        return true;
    }
    
    // Handle UPDATE and DELETE operations
    NodeBlock* existingNode = this->nm->get(nodeId);
    if (!existingNode) {
        incremental_localstore_logger.warn("Node " + nodeId + " not found for " + operationType + " operation");
        return false;
    }
    
    if (operationType == TemporalConstants::OP_UPDATE) {
        // Update properties
        if (nodeJson.contains("properties")) {
            char value[PropertyLink::MAX_VALUE_SIZE] = {};
            auto sourceProps = nodeJson["properties"];
            for (auto it = sourceProps.begin(); it != sourceProps.end(); it++) {
                strcpy(value, it.value().get<std::string>().c_str());
                existingNode->addProperty(std::string(it.key()), &value[0]);
            }
        }
        
        // Update temporal metadata
        addNodeMetaProperty(existingNode, TemporalConstants::LAST_OPERATION, operationType);
        addNodeMetaProperty(existingNode, TemporalConstants::LAST_OPERATION_TS, operationTimestamp);
        addNodeMetaProperty(existingNode, TemporalConstants::UPDATED_AT, operationTimestamp);
        addNodeMetaProperty(existingNode, TemporalConstants::STATUS, TemporalConstants::STATUS_ACTIVE);
        
        // Increment property version
        auto meta = existingNode->getAllMetaProperties();
        auto versionIt = meta.find(TemporalConstants::PROPERTY_VERSION);
        int version = versionIt != meta.end() ? std::stoi(versionIt->second) + 1 : 1;
        addNodeMetaProperty(existingNode, TemporalConstants::PROPERTY_VERSION, std::to_string(version));
        
        incremental_localstore_logger.debug("Node " + nodeId + " updated successfully");
        return true;
    }
    
    if (operationType == TemporalConstants::OP_DELETE) {
        // Mark node as deleted
        addNodeMetaProperty(existingNode, TemporalConstants::LAST_OPERATION, operationType);
        addNodeMetaProperty(existingNode, TemporalConstants::LAST_OPERATION_TS, operationTimestamp);
        addNodeMetaProperty(existingNode, TemporalConstants::STATUS, TemporalConstants::STATUS_DELETED);
        addNodeMetaProperty(existingNode, TemporalConstants::DELETED_AT, operationTimestamp);
        
        incremental_localstore_logger.debug("Node " + nodeId + " marked as deleted");
        return true;
    }
    
    incremental_localstore_logger.warn("Unsupported node operation type: " + operationType);
    return false;
}

bool JasmineGraphIncrementalLocalStore::handleEdgeAddition(const json& edgeJson, const std::string& operationType,
                                                           const std::string& operationTimestamp) {
    auto sourceJson = edgeJson["source"];
    auto destinationJson = edgeJson["destination"];
    std::string sId = sourceJson["id"].get<std::string>();
    std::string dId = destinationJson["id"].get<std::string>();

    bool localEdge = isLocalEdge(edgeJson);
    RelationBlock* relation = localEdge ? this->nm->addLocalEdge({sId, dId})
                                        : this->nm->addCentralEdge({sId, dId});
    if (!relation) {
        incremental_localstore_logger.error("Failed to add relation (" + sId + ", " + dId + ")");
        return false;
    }

    if (localEdge) {
        addLocalEdgeProperties(relation, edgeJson);
    } else {
        addCentralEdgeProperties(relation, edgeJson);
    }

    addSourceProperties(relation, sourceJson);
    addDestinationProperties(relation, destinationJson);
    attachTemporalMeta(relation, operationType, operationTimestamp);
    incremental_localstore_logger.debug("Edge (" + sId + ", " + dId + ") processed as ADD operation.");
    return true;
}

bool JasmineGraphIncrementalLocalStore::handleEdgeUpdate(const json& edgeJson, bool isLocal,
                                                         const std::string& operationType,
                                                         const std::string& operationTimestamp) {
    auto sourceJson = edgeJson["source"];
    auto destinationJson = edgeJson["destination"];
    std::string sId = sourceJson["id"].get<std::string>();
    std::string dId = destinationJson["id"].get<std::string>();

    RelationBlock* relation = findRelation(sId, dId, isLocal);
    if (!relation) {
        incremental_localstore_logger.warn("Update requested for missing relation (" + sId + ", " + dId +
                                          "). Creating relation instead.");
        return handleEdgeAddition(edgeJson, DEFAULT_OPERATION, operationTimestamp);
    }

    if (isLocal) {
        addLocalEdgeProperties(relation, edgeJson);
    } else {
        addCentralEdgeProperties(relation, edgeJson);
    }

    addSourceProperties(relation, sourceJson);
    addDestinationProperties(relation, destinationJson);
    attachTemporalMeta(relation, operationType, operationTimestamp);
    incremental_localstore_logger.debug("Edge (" + sId + ", " + dId + ") updated successfully.");
    return true;
}

bool JasmineGraphIncrementalLocalStore::handleEdgeDeletion(const json& edgeJson, bool isLocal,
                                                           const std::string& operationType,
                                                           const std::string& operationTimestamp) {
    auto sourceJson = edgeJson["source"];
    auto destinationJson = edgeJson["destination"];
    std::string sId = sourceJson["id"].get<std::string>();
    std::string dId = destinationJson["id"].get<std::string>();

    RelationBlock* relation = findRelation(sId, dId, isLocal);
    if (!relation) {
        incremental_localstore_logger.warn("Delete requested for missing relation (" + sId + ", " + dId + ")");
        return false;
    }

    attachTemporalMeta(relation, operationType, operationTimestamp);
    incremental_localstore_logger.debug("Edge (" + sId + ", " + dId + ") marked as deleted.");
    return true;
}

RelationBlock* JasmineGraphIncrementalLocalStore::findRelation(const std::string& sId, const std::string& dId,
                                                               bool isLocal) {
    NodeBlock* sourceNode = this->nm->get(sId);
    NodeBlock* destinationNode = this->nm->get(dId);
    if (!sourceNode || !destinationNode) {
        incremental_localstore_logger.warn("Unable to load nodes while searching relation (" + sId + ", " + dId +
                                          ")");
        return nullptr;
    }

    RelationBlock* relation = nullptr;
    if (isLocal) {
        relation = sourceNode->searchLocalRelation(*destinationNode);
    } else {
        relation = sourceNode->searchCentralRelation(*destinationNode);
        if (!relation) {
            relation = sourceNode->searchLocalRelation(*destinationNode);
        }
    }
    return relation;
}

void JasmineGraphIncrementalLocalStore::attachTemporalMeta(RelationBlock* relationBlock,
                                                           const std::string& operationType,
                                                           const std::string& operationTimestamp) {
    if (!relationBlock) {
        return;
    }
    addRelationMetaProperty(relationBlock, TemporalConstants::LAST_OPERATION, operationType);
    addRelationMetaProperty(relationBlock, TemporalConstants::LAST_OPERATION_TS, operationTimestamp);

    if (operationType == TemporalConstants::OP_DELETE) {
        addRelationMetaProperty(relationBlock, TemporalConstants::STATUS, TemporalConstants::STATUS_DELETED);
        addRelationMetaProperty(relationBlock, TemporalConstants::DELETED_AT, operationTimestamp);
    } else if (operationType == TemporalConstants::OP_ADD) {
        addRelationMetaProperty(relationBlock, TemporalConstants::STATUS, TemporalConstants::STATUS_ACTIVE);
        addRelationMetaProperty(relationBlock, TemporalConstants::CREATED_AT, operationTimestamp);
        addRelationMetaProperty(relationBlock, TemporalConstants::PROPERTY_VERSION, "1");
    } else if (operationType == TemporalConstants::OP_UPDATE) {
        addRelationMetaProperty(relationBlock, TemporalConstants::STATUS, TemporalConstants::STATUS_ACTIVE);
        addRelationMetaProperty(relationBlock, TemporalConstants::UPDATED_AT, operationTimestamp);
        
        // Increment property version
        auto meta = relationBlock->getAllMetaProperties();
        auto versionIt = meta.find(TemporalConstants::PROPERTY_VERSION);
        int version = versionIt != meta.end() ? std::stoi(versionIt->second) + 1 : 1;
        addRelationMetaProperty(relationBlock, TemporalConstants::PROPERTY_VERSION, std::to_string(version));
    }
}
