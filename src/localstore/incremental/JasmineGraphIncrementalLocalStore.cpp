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
#include <faiss/IndexFlat.h>

#include <memory>
#include <stdexcept>
#include <unordered_set>

#include "../../nativestore/MetaPropertyLink.h"
#include "../../nativestore/RelationBlock.h"
#include "../../nativestore/temporal/TemporalConstants.h"
#include "../../util/Utils.h"
#include "../../util/logger/Logger.h"
#include "../../vectorstore/FaissIndex.h"
#include "../../vectorstore/TextEmbedder.h"

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

JasmineGraphIncrementalLocalStore::JasmineGraphIncrementalLocalStore(
    unsigned int graphID, unsigned int partitionID, std::string openMode,
    bool embedNode) {
    gc.graphID = graphID;
    gc.partitionID = partitionID;
    gc.maxLabelSize = std::stoi(Utils::getJasmineGraphProperty(
        "org.jasminegraph.nativestore.max.label.size"));
    this->embedNode = embedNode;
    this->embedding_requests = new std::vector<EmbeddingRequest>();

    gc.openMode = openMode;
    this->nm = new NodeManager(gc);
    this->temporalLogger = std::make_unique<TemporalEventLogger>(this->nm->getDbPrefix());
    
    if (this->embedNode) {
        incremental_localstore_logger.info("Embedding enabled for the local store");
        this->faissStore =
            FaissIndex::getInstance(std::stoi(Utils::getJasmineGraphProperty(
                                        "org.jasminegraph.vectorstore.dimension")),
                                    this->nm->getDbPrefix() + "_faiss.index");
        this->textEmbedder = new TextEmbedder(
            Utils::getJasmineGraphProperty("org.jasminegraph.vectorstore.embedding."
                                           "ollama.endpoint"),  // Ollama endpoint
            Utils::getJasmineGraphProperty(
                "org.jasminegraph.vectorstore.embedding.model"));
    }
};
bool JasmineGraphIncrementalLocalStore::getAndStoreEmbeddings() {
  std::vector<string> batch_request;
  for (EmbeddingRequest& request : *embedding_requests) {
    batch_request.emplace_back(request.nodeText);
  }
  vector<vector<float>> results = textEmbedder->batch_embed(batch_request);

  for (size_t i = 0; i < results.size(); ++i) {
    faissStore->add(results[i], embedding_requests->at(i).nodeId);
  }
  embedding_requests->clear();
  faissStore->save();
}

std::pair<std::string, unsigned int> JasmineGraphIncrementalLocalStore::getIDs(
    std::string edgeString) {
  try {
    auto edgeJson = json::parse(edgeString);
    if (edgeJson.contains("properties")) {
      auto edgeProperties = edgeJson["properties"];
      return {edgeProperties["graphId"], edgeJson["PID"]};
    }
  } catch (const std::exception&
               e) {  // TODO tmkasun: Handle multiple types of exceptions
    incremental_localstore_logger.log(
        "Error while processing edge data = " + std::string(e.what()) +
            "Could be due to JSON parsing error or error while persisting the "
            "data to disk",
        "error");
  }
  return {"", 0};  // all plath of the function must return
                   // std::pair<std::string, unsigned int>
                   // type object even there is an error
}

void JasmineGraphIncrementalLocalStore::addEdgeFromString(
    std::string edgeString) {
  try {
    auto edgeJson = json::parse(edgeString);
    if (edgeJson.contains("isNode")) {
      std::string nodeId = edgeJson["id"];
      NodeBlock* newNode = this->nm->addNode(nodeId);

<<<<<<< HEAD
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
=======
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
>>>>>>> master
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
    incremental_localstore_logger.debug("Edge (" + sId + ", " + dId +
                                        ") Added successfully!");
  } catch (const json::parse_error &ex) {
    // JSON syntax errors
    incremental_localstore_logger.error(
        "JSON parse error while processing edge string: " + edgeString +
        " | Error: " + std::string(ex.what()));
  }
  catch (const json::type_error &ex) {
    // Wrong JSON types (e.g., expecting string but got int)
    incremental_localstore_logger.error(
        "JSON type error: Invalid JSON attribute types in: " + edgeString +
        " | Error: " + std::string(ex.what()));
  }
  catch (const json::out_of_range &ex) {
    // Missing fields like "source" or "destination"
    incremental_localstore_logger.error(
        "JSON out-of-range error: Missing required keys in: " + edgeString +
        " | Error: " + std::string(ex.what()));
  }
  catch (const std::exception &ex) {
    // All other standard errors (filesystem errors, memory issues, etc.)
    incremental_localstore_logger.error(
        "Unhandled exception while processing edge data: " + edgeString +
        " | Error: " + std::string(ex.what()));
  }
  catch (...) {
    // Non-standard exceptions
    incremental_localstore_logger.error(
        "Unknown fatal error while processing: " + edgeString);
  }
}

void JasmineGraphIncrementalLocalStore::addLocalEdge(std::string edge) {
  auto jsonEdge = json::parse(edge);
  auto jsonSource = jsonEdge["source"];
  auto jsonDestination = jsonEdge["destination"];

  // log the edge information
  if (!jsonSource.contains("id") || !jsonDestination.contains("id")) {
    incremental_localstore_logger.error(
        "Source or destination ID missing in edge data: " + edge);
    return;
  }
  if (!jsonEdge.contains("source") || !jsonEdge.contains("destination")) {
    incremental_localstore_logger.error(
        "Source or destination missing in edge data: " + edge);
    return;
  }
  if (!jsonEdge.contains("properties")) {
    incremental_localstore_logger.error("Properties missing in edge data: " +
                                        edge);
    return;
  }
  if (!jsonSource.contains("pid") || !jsonDestination.contains("pid")) {
    incremental_localstore_logger.error(
        "Partition ID missing in source or destination: " + edge);
    return;
  }

  std::string sId = std::string(jsonSource["id"]);
  std::string dId = std::string(jsonDestination["id"]);
  RelationBlock* newRelation = nullptr;
  if (jsonEdge["properties"].contains("id")) {
    std::string edgeId = std::string(jsonEdge["properties"]["id"]);

    if (this->nm->edgeIndex.find(edgeId) == this->nm->edgeIndex.end()) {
      incremental_localstore_logger.debug("Edge Id not found: " + edgeId);

      newRelation = this->nm->addLocalEdge({sId, dId});
      this->nm->edgeIndex.insert({edgeId, this->nm->nextEdgeIndex});
    } else {
      incremental_localstore_logger.debug("Edge Id already found: " + edgeId);
    }
  } else {
    newRelation = this->nm->addLocalEdge({sId, dId});
  }

  if (newRelation == nullptr) {
    return;
  }

  addLocalEdgeProperties(newRelation, jsonEdge);
  addSourceProperties(newRelation, jsonSource);
  addDestinationProperties(newRelation, jsonDestination);
  delete newRelation->getSource();
  delete newRelation->getDestination();
  delete newRelation;
  incremental_localstore_logger.debug("Local edge (" + sId + "-> " + dId +
                                      " ) added successfully");
}

void JasmineGraphIncrementalLocalStore::addCentralEdge(std::string edge) {
  auto jsonEdge = json::parse(edge);
  auto jsonSource = jsonEdge["source"];
  auto jsonDestination = jsonEdge["destination"];

  std::string sId = std::string(jsonSource["id"]);
  std::string dId = std::string(jsonDestination["id"]);

  RelationBlock* newRelation = nullptr;

  if (jsonEdge["properties"].contains("id")) {
    std::string edgeId = std::string(jsonEdge["properties"]["id"]);

    if (this->nm->edgeIndex.find(edgeId) == this->nm->edgeIndex.end()) {
      incremental_localstore_logger.debug("Edge Id not found: " + edgeId);

      newRelation = this->nm->addCentralEdge({sId, dId});
      this->nm->edgeIndex.insert({edgeId, this->nm->nextEdgeIndex});
    } else {
      incremental_localstore_logger.debug("Edge Id already found: " + edgeId);
    }
  } else {
    newRelation = this->nm->addCentralEdge({sId, dId});
  }

  if (newRelation == nullptr) {
    return;
  }

  addCentralEdgeProperties(newRelation, jsonEdge);
  addSourceProperties(newRelation, jsonSource);
  addDestinationProperties(newRelation, jsonDestination);
  delete newRelation->getSource();
  delete newRelation->getDestination();
  delete newRelation;
  incremental_localstore_logger.debug("Central edge (" + sId + "-> " + dId +
                                      " ) added successfully");
}

void JasmineGraphIncrementalLocalStore::addCentralEdgeProperties(
    RelationBlock* relationBlock, const json& edgeJson) {
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
  addRelationMetaProperty(relationBlock, MetaPropertyEdgeLink::PARTITION_ID,
                          edgePid);
}

void JasmineGraphIncrementalLocalStore::addLocalEdgeProperties(
    RelationBlock* relationBlock, const json& edgeJson) {
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

void JasmineGraphIncrementalLocalStore::addSourceProperties(
    RelationBlock* relationBlock, const json& sourceJson) {
  char value[PropertyLink::MAX_VALUE_SIZE] = {};
  char label[NodeBlock::LABEL_SIZE] = {0};
  std::ostringstream textForEmbedding;

  if (sourceJson.contains("properties")) {
    auto sourceProps = json(sourceJson["properties"]);

    if (!sourceProps.empty()) {
      for (auto it = sourceProps.begin(); it != sourceProps.end(); it++) {
        strcpy(value, it.value().get<std::string>().c_str());
        if (std::string(it.key()) == "label") {
          strcpy(label, it.value().get<std::string>().c_str());
          relationBlock->getSource()->addLabel(&label[0]);
        }
        textForEmbedding << it.key() << ":" << value << " ";
        relationBlock->getSource()->addProperty(std::string(it.key()),
                                                &value[0]);
      }

      if (this->embedNode) {
        std::string nodeText = textForEmbedding.str();
        if (!nodeText.empty()) {
          if (faissStore->getEmbeddingById(sourceJson["id"]).size() == 0) {
            incremental_localstore_logger.error(
                "Node with ID " + sourceJson["id"].get<std::string>() +
                " found . Skipping ");
            return;
          }
          EmbeddingRequest request = {sourceJson["id"].get<std::string>(),
                                      nodeText};
          embedding_requests->emplace_back(request);
        }
      }
    }
  }

  std::string sourcePid = std::to_string(sourceJson["pid"].get<int>());
  addNodeMetaProperty(relationBlock->getSource(),
                      MetaPropertyLink::PARTITION_ID, sourcePid);
}

void JasmineGraphIncrementalLocalStore::addDestinationProperties(
    RelationBlock* relationBlock, const json& destinationJson) {
  char value[PropertyLink::MAX_VALUE_SIZE] = {};
  char label[NodeBlock::LABEL_SIZE] = {0};
  std::ostringstream textForEmbedding;

  if (destinationJson.contains("properties")) {
    auto destinationProps = destinationJson["properties"];
    if (!destinationProps.empty()) {
      for (auto it = destinationProps.begin(); it != destinationProps.end();
           it++) {
        strcpy(value, it.value().get<std::string>().c_str());
        if (std::string(it.key()) == "label") {
          strcpy(label, it.value().get<std::string>().c_str());
          relationBlock->getDestination()->addLabel(&label[0]);
        }
        textForEmbedding << it.key() << ":" << value << " ";

        relationBlock->getDestination()->addProperty(std::string(it.key()),
                                                     &value[0]);
      }
      if (this->embedNode) {
        std::string nodeText = textForEmbedding.str();
        if (!nodeText.empty()) {
          if (faissStore->getEmbeddingById(destinationJson["id"]).empty()) {
            incremental_localstore_logger.error(
                "Node with ID " + destinationJson["id"].get<std::string>() +
                " found . Skipping ");
            return;
          }
          EmbeddingRequest request = {destinationJson["id"].get<std::string>(),
                                      nodeText};
          embedding_requests->emplace_back(request);
        }
      }
    }
  }
  std::string destPId = std::to_string(destinationJson["pid"].get<int>());
  addNodeMetaProperty(relationBlock->getDestination(),
                      MetaPropertyLink::PARTITION_ID, destPId);
}

void JasmineGraphIncrementalLocalStore::addNodeMetaProperty(
    NodeBlock* nodeBlock, std::string propertyKey, std::string propertyValue) {
  incremental_localstore_logger.debug("meta property: " + propertyKey + " " +
                                      propertyValue);
  char meta[MetaPropertyLink::MAX_VALUE_SIZE] = {};
  strcpy(meta, propertyValue.c_str());
  nodeBlock->addMetaProperty(propertyKey, &meta[0]);
}

void JasmineGraphIncrementalLocalStore::addRelationMetaProperty(
    RelationBlock* relationBlock, std::string propertyKey,
    std::string propertyValue) {
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
