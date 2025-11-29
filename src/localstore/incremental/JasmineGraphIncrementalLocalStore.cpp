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

#include "../../nativestore/RelationBlock.h"
#include "../../util/logger/Logger.h"
#include "../../util/Utils.h"
#include "../../nativestore/MetaPropertyLink.h"
#include "../../temporal/TemporalIntegration.h"
#include "../../temporal/TemporalTypes.h"
#include "../../temporal/TemporalPartitionManager.h"

Logger incremental_localstore_logger;

JasmineGraphIncrementalLocalStore::JasmineGraphIncrementalLocalStore(unsigned int graphID, unsigned int partitionID,
                                                                     std::string openMode) {
    gc.graphID = graphID;
    gc.partitionID = partitionID;
    gc.maxLabelSize = std::stoi(Utils::getJasmineGraphProperty("org.jasminegraph.nativestore.max.label.size"));
    gc.openMode = openMode;
    this->nm = new NodeManager(gc);
    
    // Initialize temporal partition manager if temporal integration is active
    if (jasminegraph::TemporalIntegration::instance() != nullptr) {
        jasminegraph::TemporalPartitionManager::Config tmConfig;
        tmConfig.graphId = graphID;
        tmConfig.partitionId = partitionID;
        
        std::string instanceDataFolder = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
        tmConfig.baseDir = instanceDataFolder + "/temporal/g" + std::to_string(graphID);
        
        // Get snapshot interval from properties or use default
        std::string intervalProp = Utils::getJasmineGraphProperty("org.jasminegraph.temporal.snapshot.interval");
        if (!intervalProp.empty()) {
            try {
                int intervalSec = std::stoi(intervalProp);
                tmConfig.snapshotInterval = std::chrono::seconds(intervalSec);
            } catch (const std::exception& e) {
                incremental_localstore_logger.warn("Invalid snapshot interval property, using default 60 seconds");
            }
        }
        
        temporalManager = std::make_unique<jasminegraph::TemporalPartitionManager>(tmConfig);
        incremental_localstore_logger.info("Temporal partition manager initialized for graph " + 
                                         std::to_string(graphID) + ", partition " + std::to_string(partitionID));
    }
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
        
        // Check if temporal streaming is enabled
        bool temporalEnabled = (jasminegraph::TemporalIntegration::instance() != nullptr);
        
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

        // Extract operation type
        std::string operationType = "ADD";  // Default to ADD
        if (edgeJson.contains("OperationType")) {
            operationType = edgeJson["OperationType"];
        }

        incremental_localstore_logger.debug("Processing " + operationType + " operation for edge (" + 
                                          sId + " -> " + dId + ")");

        // If temporal streaming is enabled, use temporal components
        if (temporalEnabled) {
            processTemporalEdge(edgeJson, sId, dId, operationType, isLocal);
            return;
        }

        // Non-temporal processing (legacy mode)
        RelationBlock* newRelation = nullptr;
        
        if (operationType == "DELETE") {
            // Handle delete operation
            incremental_localstore_logger.info("Processing DELETE operation for edge (" + sId + " -> " + dId + ")");
            
            // For DELETE operations, we need to find and remove the edge
            // Note: This is a simplified implementation - in a full system you'd want
            // to implement proper edge deletion in the NodeManager
            if (isLocal) {
                incremental_localstore_logger.warn("DELETE operation for Local edge (" + sId + " -> " + dId + 
                                                  ") - Delete functionality not yet implemented");
            } else {
                incremental_localstore_logger.warn("DELETE operation for Central edge (" + sId + " -> " + dId + 
                                                  ") - Delete functionality not yet implemented");
            }
            return;  // Exit early for DELETE operations
        } else {
            // Handle ADD and EDIT operations (both create/update edges)
            if (isLocal) {
                newRelation = this->nm->addLocalEdge({sId, dId});
            } else {
                newRelation = this->nm->addCentralEdge({sId, dId});
            }
            
            if (!newRelation) {
                incremental_localstore_logger.error("Failed to create edge for " + operationType + 
                                                   " operation: (" + sId + " -> " + dId + ")");
                return;
            }
            
            if (operationType == "EDIT") {
                incremental_localstore_logger.info("Processing EDIT operation for edge (" + sId + " -> " + dId + 
                                                  ") - updating existing edge properties");
            }
        }

        if (isLocal) {
            addLocalEdgeProperties(newRelation, edgeJson);
        } else {
            addCentralEdgeProperties(newRelation, edgeJson);
        }

        addSourceProperties(newRelation, sourceJson);
        addDestinationProperties(newRelation, destinationJson);
        
        // Log temporal and operation information if available
        if (edgeJson.contains("properties")) {
            auto props = edgeJson["properties"];
            if (props.contains("event_timestamp") && props.contains("ingestion_timestamp")) {
                string edgeType = isLocal ? "Local" : "Central";
                incremental_localstore_logger.info("Processed " + operationType + " operation on " + edgeType + 
                                                  " edge (" + sId + " -> " + dId + ") with temporal data - Event: " + 
                                                  props["event_timestamp"].dump() + ", Ingestion: " + 
                                                  props["ingestion_timestamp"].dump());
                
                if (props.contains("processing_latency_ms")) {
                    incremental_localstore_logger.debug("Processing latency: " + props["processing_latency_ms"].dump() + "ms");
                }
            }
        }
        
        incremental_localstore_logger.debug("Edge (" + sId + ", " + dId + ") " + operationType + " operation completed successfully!");
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
    bool hasTemporalData = false;
    
    if (edgeJson.contains("properties")) {
        auto edgeProperties = edgeJson["properties"];
        for (auto it = edgeProperties.begin(); it != edgeProperties.end(); it++) {
            std::string key = it.key();
            
            // Handle temporal properties
            if (key == "event_timestamp" || key == "ingestion_timestamp" || key == "processing_latency_ms") {
                hasTemporalData = true;
                // Convert numeric values to string for storage
                std::string temporalValue;
                if (it.value().is_number()) {
                    temporalValue = std::to_string(it.value().get<uint64_t>());
                } else {
                    temporalValue = it.value().get<std::string>();
                }
                strcpy(value, temporalValue.c_str());
                relationBlock->addCentralProperty(key, &value[0]);
                continue;
            }
            
            // Handle regular properties
            strcpy(value, it.value().get<std::string>().c_str());
            if (key == "type") {
                strcpy(type, it.value().get<std::string>().c_str());
                relationBlock->addCentralRelationshipType(&type[0]);
            }
            relationBlock->addCentralProperty(key, &value[0]);
        }
        
        // Log temporal information if present
        if (hasTemporalData && edgeProperties.contains("event_timestamp") && edgeProperties.contains("ingestion_timestamp")) {
            incremental_localstore_logger.debug("Stored central edge with temporal data - Event: " + 
                                              edgeProperties["event_timestamp"].dump() + 
                                              ", Ingestion: " + edgeProperties["ingestion_timestamp"].dump());
        }
    }
    std::string edgePid = std::to_string(edgeJson["source"]["pid"].get<int>());
    addRelationMetaProperty(relationBlock, MetaPropertyEdgeLink::PARTITION_ID, edgePid);
}

void JasmineGraphIncrementalLocalStore::addLocalEdgeProperties(RelationBlock* relationBlock, const json& edgeJson) {
    char value[PropertyLink::MAX_VALUE_SIZE] = {};
    char type[RelationBlock::MAX_TYPE_SIZE] = {0};
    bool hasTemporalData = false;
    
    if (edgeJson.contains("properties")) {
        auto edgeProperties = edgeJson["properties"];
        for (auto it = edgeProperties.begin(); it != edgeProperties.end(); it++) {
            std::string key = it.key();
            
            // Handle temporal properties
            if (key == "event_timestamp" || key == "ingestion_timestamp" || key == "processing_latency_ms") {
                hasTemporalData = true;
                // Convert numeric values to string for storage
                std::string temporalValue;
                if (it.value().is_number()) {
                    temporalValue = std::to_string(it.value().get<uint64_t>());
                } else {
                    temporalValue = it.value().get<std::string>();
                }
                strcpy(value, temporalValue.c_str());
                relationBlock->addLocalProperty(key, &value[0]);
                continue;
            }
            
            // Handle regular properties
            strcpy(value, it.value().get<std::string>().c_str());
            if (key == "type") {
                strcpy(type, it.value().get<std::string>().c_str());
                relationBlock->addLocalRelationshipType(&type[0]);
            }
            relationBlock->addLocalProperty(key, &value[0]);
        }
        
        // Log temporal information if present
        if (hasTemporalData && edgeProperties.contains("event_timestamp") && edgeProperties.contains("ingestion_timestamp")) {
            incremental_localstore_logger.debug("Stored local edge with temporal data - Event: " + 
                                              edgeProperties["event_timestamp"].dump() + 
                                              ", Ingestion: " + edgeProperties["ingestion_timestamp"].dump());
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

void JasmineGraphIncrementalLocalStore::processTemporalEdge(const json& edgeJson, const std::string& sId, 
                                                           const std::string& dId, const std::string& operationType, 
                                                           bool isLocal) {
    // Get temporal facade instance
    jasminegraph::TemporalFacade* temporalFacade = jasminegraph::TemporalIntegration::instance();
    if (!temporalFacade) {
        incremental_localstore_logger.error("Temporal facade not initialized for temporal processing");
        return;
    }
    
    // Create StreamEdgeRecord from JSON data
    jasminegraph::StreamEdgeRecord streamRecord;
    
    // Set source and destination vertices
    streamRecord.source.id = sId;
    streamRecord.destination.id = dId;
    
    // Extract source properties
    if (edgeJson.contains("source") && edgeJson["source"].contains("properties")) {
        auto sourceProps = edgeJson["source"]["properties"];
        for (auto it = sourceProps.begin(); it != sourceProps.end(); ++it) {
            // Handle different JSON value types
            std::string value;
            if (it.value().is_string()) {
                value = it.value().get<std::string>();
            } else if (it.value().is_number()) {
                value = std::to_string(it.value().get<double>());
            } else if (it.value().is_boolean()) {
                value = it.value().get<bool>() ? "true" : "false";
            } else {
                value = it.value().dump(); // Fallback to JSON string representation
            }
            streamRecord.source.properties[it.key()] = value;
        }
    }
    if (edgeJson.contains("source") && edgeJson["source"].contains("pid")) {
        streamRecord.source.pid = edgeJson["source"]["pid"].get<std::uint32_t>();
    }
    
    // Extract destination properties
    if (edgeJson.contains("destination") && edgeJson["destination"].contains("properties")) {
        auto destProps = edgeJson["destination"]["properties"];
        for (auto it = destProps.begin(); it != destProps.end(); ++it) {
            // Handle different JSON value types
            std::string value;
            if (it.value().is_string()) {
                value = it.value().get<std::string>();
            } else if (it.value().is_number()) {
                value = std::to_string(it.value().get<double>());
            } else if (it.value().is_boolean()) {
                value = it.value().get<bool>() ? "true" : "false";
            } else {
                value = it.value().dump(); // Fallback to JSON string representation
            }
            streamRecord.destination.properties[it.key()] = value;
        }
    }
    if (edgeJson.contains("destination") && edgeJson["destination"].contains("pid")) {
        streamRecord.destination.pid = edgeJson["destination"]["pid"].get<std::uint32_t>();
    }
    
    // Extract edge properties (including temporal metadata)
    if (edgeJson.contains("properties")) {
        auto edgeProps = edgeJson["properties"];
        for (auto it = edgeProps.begin(); it != edgeProps.end(); ++it) {
            // Handle different JSON value types
            std::string value;
            if (it.value().is_string()) {
                value = it.value().get<std::string>();
            } else if (it.value().is_number()) {
                value = std::to_string(it.value().get<double>());
            } else if (it.value().is_boolean()) {
                value = it.value().get<bool>() ? "true" : "false";
            } else {
                value = it.value().dump(); // Fallback to JSON string representation
            }
            streamRecord.properties[it.key()] = value;
        }
    }
    
    // Set operation type
    if (operationType == "ADD") {
        streamRecord.op = jasminegraph::StreamOp::Insert;
    } else if (operationType == "EDIT") {
        streamRecord.op = jasminegraph::StreamOp::Update;
    } else if (operationType == "DELETE") {
        streamRecord.op = jasminegraph::StreamOp::Delete;
    } else {
        incremental_localstore_logger.warn("Unknown operation type: " + operationType + ", defaulting to Insert");
        streamRecord.op = jasminegraph::StreamOp::Insert;
    }
    
    // Extract event timestamp if available
    if (edgeJson.contains("properties") && edgeJson["properties"].contains("event_timestamp")) {
        streamRecord.eventTimeISO8601 = edgeJson["properties"]["event_timestamp"].get<std::string>();
    }
    
    // Add edge type metadata
    streamRecord.properties["edge_type"] = isLocal ? "Local" : "Central";
    streamRecord.properties["partition_id"] = std::to_string(gc.partitionID);
    streamRecord.properties["graph_id"] = std::to_string(gc.graphID);
    
    // Log temporal processing
    incremental_localstore_logger.info("Temporal processing: " + operationType + " operation for " + 
                                     (isLocal ? "Local" : "Central") + " edge (" + sId + " -> " + dId + 
                                     ") in partition " + std::to_string(gc.partitionID));
    
    // Send to temporal partition manager for processing
    if (temporalManager) {
        temporalManager->ingestEdgeRecord(streamRecord);
        
        incremental_localstore_logger.info("Successfully ingested " + operationType + " operation for " + 
                                         (isLocal ? "Local" : "Central") + " edge (" + sId + " -> " + dId + 
                                         ") into temporal partition manager");
        
        // Log statistics
        size_t pendingCount = temporalManager->getPendingRecordCount();
        jasminegraph::SnapshotID currentSnapshot = temporalManager->getCurrentSnapshotId();
        
        incremental_localstore_logger.debug("Temporal stats - Pending records: " + std::to_string(pendingCount) + 
                                          ", Current snapshot: " + std::to_string(currentSnapshot));
        
        // Force flush if we have accumulated many records (optional optimization)
        if (pendingCount > 1000) { // Configurable threshold
            incremental_localstore_logger.info("Triggering early snapshot flush due to high record count: " + 
                                             std::to_string(pendingCount));
            jasminegraph::SnapshotID newSnapshot = temporalManager->flushSnapshot();
            incremental_localstore_logger.info("Created snapshot " + std::to_string(newSnapshot) + 
                                             " for partition " + std::to_string(gc.partitionID));
        }
    } else {
        incremental_localstore_logger.error("Temporal partition manager not initialized for temporal processing");
    }
    
    incremental_localstore_logger.debug("StreamEdgeRecord processed successfully");
    incremental_localstore_logger.debug("Source: " + streamRecord.source.id + " (PID: " + std::to_string(streamRecord.source.pid) + ")");
    incremental_localstore_logger.debug("Destination: " + streamRecord.destination.id + " (PID: " + std::to_string(streamRecord.destination.pid) + ")");
    incremental_localstore_logger.debug("Operation: " + std::to_string(static_cast<int>(streamRecord.op)));
    incremental_localstore_logger.debug("Properties count: " + std::to_string(streamRecord.properties.size()));
}
