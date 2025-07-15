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

#include <memory>
#include <stdexcept>

#include "../../nativestore/LabelIndexManager.h"
#include "../../nativestore/RelationBlock.h"
#include "../../util/logger/Logger.h"
#include "../../util/Utils.h"
#include "../../nativestore/MetaPropertyLink.h"

Logger incremental_localstore_logger;

JasmineGraphIncrementalLocalStore::JasmineGraphIncrementalLocalStore(unsigned int graphID, unsigned int partitionID,
                                                                     std::string openMode) {
    gc.graphID = graphID;
    gc.partitionID = partitionID;
    gc.maxLabelSize = std::stoi(Utils::getJasmineGraphProperty("org.jasminegraph.nativestore.max.label.size"));
    gc.openMode = openMode;
    this->nm = new NodeManager(gc);
    this->nodeLabelIndexManager = new LabelIndexManager(nm->getDbPrefix() + "_node_label_index_mapping.db", nm->getDbPrefix() + "_node_label_bit_map.db");
    this->localRelationLabelIndexManager = new LabelIndexManager(nm->getDbPrefix() + "local_relation_label_index_mapping.db",nm->getDbPrefix() + "local_relation_label_bit_map.db");
    this->centralRelationLabelIndexManager = new LabelIndexManager(nm->getDbPrefix() + "central_relation_label_index_mapping.db", nm->getDbPrefix() + "central_relation_label_bit_map.db");
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
        if (edgeJson.contains("isNode")) {
            std::string nodeId = edgeJson["id"];
            NodeBlock* newNode = this->nm->addNode(nodeId);

            char value[PropertyLink::MAX_VALUE_SIZE] = {};
            char meta[MetaPropertyLink::MAX_VALUE_SIZE] = {};

            if (edgeJson.contains("properties")) {
                auto sourceProps = edgeJson["properties"];
                for (auto it = sourceProps.begin(); it != sourceProps.end(); it++) {
                    strcpy(value, it.value().get<std::string>().c_str());
                    newNode->addProperty(std::string(it.key()), &value[0]);
                }
            }

            std::string sourcePid = std::to_string(edgeJson["pid"].get<int>());
            strcpy(meta, sourcePid.c_str());
            newNode->addMetaProperty(MetaPropertyLink::PARTITION_ID, &meta[0]);
            return;
        }

        auto sourceJson = edgeJson["source"];
        auto destinationJson = edgeJson["destination"];

        std::string sId = std::string(sourceJson["id"]);
        std::string dId = std::string(destinationJson["id"]);

        bool isLocal = false;
        if (edgeJson["EdgeType"] == "Local") {
            isLocal = true;
        }

        RelationBlock* newRelation;
        if (isLocal) {
            newRelation = this->nm->addLocalEdge({sId, dId});
        } else {
            newRelation = this->nm->addCentralEdge({sId, dId});
        }
        if (!newRelation) {
            return;
        }

        if (isLocal) {
            addLocalEdgeProperties(newRelation, edgeJson);
        } else {
            addCentralEdgeProperties(newRelation, edgeJson);
        }

        addSourceProperties(newRelation, sourceJson);
        addDestinationProperties(newRelation, destinationJson);
        incremental_localstore_logger.debug("Edge (" + sId + ", " + dId + ") Added successfully!");
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
    delete newRelation->getSource();
    delete newRelation->getDestination();
    delete newRelation;

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
    delete newRelation->getSource();
    delete newRelation->getDestination();
    delete newRelation;
    incremental_localstore_logger.debug("Central edge (" + sId + "-> " + dId + " ) added successfully");
}

void JasmineGraphIncrementalLocalStore::addCentralEdgeProperties(RelationBlock* relationBlock, const json& edgeJson) {
    incremental_localstore_logger.debug("Entering addCentralEdgeProperties");
    char value[PropertyLink::MAX_VALUE_SIZE] = {};
    char type[RelationBlock::MAX_TYPE_SIZE] = {0};
    if (edgeJson.contains("properties")) {
        auto edgeProperties = edgeJson["properties"];
        for (auto it = edgeProperties.begin(); it != edgeProperties.end(); it++) {
            strcpy(value, it.value().get<std::string>().c_str());
            incremental_localstore_logger.debug("Processing property key: " + std::string(it.key()) + ", value: " + std::string(value));
            if (std::string(it.key()) == "type") {
                strcpy(type, it.value().get<std::string>().c_str());
                size_t edgeIndex = relationBlock->addr / RelationBlock::CENTRAL_BLOCK_SIZE;
                incremental_localstore_logger.debug("Setting central relationship type: " + std::string(type) + " at edgeIndex: " + std::to_string(edgeIndex));
                relationBlock->addCentralRelationshipType(&type[0], centralRelationLabelIndexManager, edgeIndex);
            }
            relationBlock->addCentralProperty(std::string(it.key()), &value[0]);
        }
    }
    std::string edgePid = std::to_string(edgeJson["source"]["pid"].get<int>());
    incremental_localstore_logger.debug("Adding relation meta property PARTITION_ID: " + edgePid);
    addRelationMetaProperty(relationBlock, MetaPropertyEdgeLink::PARTITION_ID, edgePid);
    incremental_localstore_logger.debug("Exiting addCentralEdgeProperties");
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
                // extract the index by dividing the address by the block size

                size_t edgeIndex = relationBlock->addr / RelationBlock::BLOCK_SIZE;
                relationBlock->addLocalRelationshipType(&type[0], localRelationLabelIndexManager, edgeIndex);
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

                std:string sourceNodeId = relationBlock->getSource()->id;
                size_t nodeIndex = nm->nodeIndex.find(sourceNodeId)->second;
                relationBlock->getSource()->addLabel(&label[0],nodeLabelIndexManager,nodeIndex );

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
                std::string destinationNodeId = relationBlock->getDestination()->id;
                size_t nodeIndex = nm->nodeIndex.find(destinationNodeId)->second;
                relationBlock->getDestination()->addLabel(&label[0], nodeLabelIndexManager, nodeIndex);
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
