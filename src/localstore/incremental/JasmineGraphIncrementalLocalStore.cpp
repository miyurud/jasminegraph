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

#include <faiss/IndexFlat.h>

#include <memory>
#include <stdexcept>

#include "../../nativestore/MetaPropertyLink.h"
#include "../../nativestore/RelationBlock.h"
#include "../../util/Utils.h"
#include "../../util/logger/Logger.h"
#include "../../vectorstore/FaissIndex.h"
#include "../../vectorstore/TextEmbedder.h"

Logger incremental_localstore_logger;
#define BATCH_SIZE 64
JasmineGraphIncrementalLocalStore::JasmineGraphIncrementalLocalStore(
    unsigned int graphID, unsigned int partitionID, std::string openMode,
    bool embedNode) {
  gc.graphID = graphID;
  gc.partitionID = partitionID;
  gc.maxLabelSize = std::stoi(Utils::getJasmineGraphProperty(
      "org.jasminegraph.nativestore.max.label.size"));
  this->embedNode = embedNode;
  this->node_embedding_requests = std::make_unique<std::unordered_map<string, string>>();
  this->edge_embedding_requests = std::make_unique<std::set<string>>();
  this->embeddingQueueMutex = PTHREAD_MUTEX_INITIALIZER;
  this->embeddingQueueCond = PTHREAD_COND_INITIALIZER;



  gc.openMode = openMode;
  this->nm = new NodeManager(gc);
  if (this->embedNode) {
    incremental_localstore_logger.debug("Embedding enabled for the local store");
    this->faissNodeStore =
        FaissIndex::getInstance(std::stoi(Utils::getJasmineGraphProperty(
                                    "org.jasminegraph.vectorstore.dimension")),
                                this->nm->getDbPrefix() + "_faiss.index");
      this->faissEdgeStore =
     FaissIndex::getInstance(std::stoi(Utils::getJasmineGraphProperty(
                                 "org.jasminegraph.vectorstore.dimension")),
                             this->nm->getDbPrefix() + "_faiss_edge.index");
    this->textEmbedder = new TextEmbedder(
        Utils::getJasmineGraphProperty("org.jasminegraph.vectorstore.embedding.ollama.endpoint"),  // Ollama endpoint
        Utils::getJasmineGraphProperty(
            "org.jasminegraph.vectorstore.embedding.model"));
  }
};

void JasmineGraphIncrementalLocalStore::setNodeManger(NodeManager* node_manager) {
    this->nm = node_manager;
}
void JasmineGraphIncrementalLocalStore::getAndStoreEmbeddings() {
    try {
        incremental_localstore_logger.debug(
            "Starting thread for embedding generation and indexing for Partition: " +
            std::to_string(gc.partitionID));

        std::vector<std::string> node_ids;
        std::vector<std::string> node_texts;
        std::vector<std::string> edge_texts;

        node_ids.reserve(BATCH_SIZE);
        node_texts.reserve(BATCH_SIZE);
        edge_texts.reserve(BATCH_SIZE);

        while (true) {
            node_ids.clear();
            node_texts.clear();
            edge_texts.clear();

            /* ==============================
             * WAIT FOR WORK OR SHUTDOWN
             * ============================== */
            pthread_mutex_lock(&embeddingQueueMutex);

            while (!processing_done &&
                   node_embedding_requests->empty() &&
                   edge_embedding_requests->empty()) {
                pthread_cond_wait(&embeddingQueueCond, &embeddingQueueMutex);
            }

            /* Shutdown condition:
             * - processing_done is set
             * - no pending work
             */
            if (processing_done &&
                node_embedding_requests->empty() &&
                edge_embedding_requests->empty()) {
                pthread_mutex_unlock(&embeddingQueueMutex);
                break;
            }

            /* ==============================
             * COLLECT NODE BATCH
             * ============================== */
            auto nit = node_embedding_requests->begin();
            for (size_t i = 0;
                 i < BATCH_SIZE && nit != node_embedding_requests->end();
                 ++i) {
                node_ids.emplace_back(nit->first);
                node_texts.emplace_back(nit->second);
                nit = node_embedding_requests->erase(nit);
            }

            /* ==============================
             * COLLECT EDGE BATCH
             * ============================== */
            auto eit = edge_embedding_requests->begin();
            for (size_t i = 0;
                 i < BATCH_SIZE && eit != edge_embedding_requests->end();
                 ++i) {
                edge_texts.emplace_back(*eit);
                eit = edge_embedding_requests->erase(eit);
            }

            pthread_mutex_unlock(&embeddingQueueMutex);

            /* ==============================
             * NODE EMBEDDINGS
             * ============================== */
            if (!node_texts.empty()) {
                incremental_localstore_logger.debug(
                    "Node embedding batch size: " +
                    std::to_string(node_texts.size()));

                auto node_vectors = textEmbedder->batch_embed(node_texts);

                for (size_t i = 0; i < node_vectors.size(); ++i) {
                    faissNodeStore->add(node_vectors[i], node_ids[i]);
                }
            }

            /* ==============================
             * EDGE EMBEDDINGS
             * ============================== */
            if (!edge_texts.empty()) {
                incremental_localstore_logger.debug(
                    "Edge embedding batch size: " +
                    std::to_string(edge_texts.size()));

                auto edge_vectors = textEmbedder->batch_embed(edge_texts);

                for (size_t i = 0; i < edge_vectors.size(); ++i) {
                    faissEdgeStore->add(edge_vectors[i], edge_texts[i]);
                }
            }
        }
        incremental_localstore_logger.debug(
            "Embedding thread exiting cleanly for Partition: " +
            std::to_string(gc.partitionID));
    } catch (const std::exception &e) {
        incremental_localstore_logger.debug(
            "Error while processing embeddings = " + std::string(e.what()));
    }
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

    addSourceProperties(newRelation, sourceJson, dId);
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
    try {
        auto jsonEdge = json::parse(edge);
        auto jsonSource = jsonEdge["source"];
        auto jsonDestination = jsonEdge["destination"];

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

        newRelation = this->nm->addLocalEdge({sId, dId});

        if (newRelation == nullptr) {
            incremental_localstore_logger.error("Error while adding new Relation: " + edge);

            return;
        }
        incremental_localstore_logger.debug("edge: " + jsonEdge.dump());
        addLocalEdgeProperties(newRelation, jsonEdge);
        addSourceProperties(newRelation, jsonSource, dId);
        addDestinationProperties(newRelation, jsonDestination);
        newRelation->getDestination()->setLocalRelationHead(*newRelation);
        newRelation->getSource()->setLocalRelationHead(*newRelation);

        delete newRelation->getSource();
        delete newRelation->getDestination();
        delete newRelation;
        incremental_localstore_logger.debug("Local edge (" + sId + "-> " + dId +
                                            " ) added successfully");
        if (node_embedding_requests->size() > BATCH_SIZE || edge_embedding_requests->size() > BATCH_SIZE) {
            pthread_cond_signal(&embeddingQueueCond);
        }
    } catch (const std::exception&  e) {
        incremental_localstore_logger.error(e.what());
    }
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
  addSourceProperties(newRelation, jsonSource, jsonDestination["id"].get<std::string>());
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
          string property = it.value().get<std::string>();
        strcpy(type, it.value().get<std::string>().c_str());
          if (!faissEdgeStore->isEmbeddingExist(property) &&
              edge_embedding_requests->find(property) == edge_embedding_requests->end()) {
              incremental_localstore_logger.debug(" Adding embedding request: "+ property);
              pthread_mutex_lock(&embeddingQueueMutex);
              edge_embedding_requests->insert(property);
              pthread_mutex_unlock(&embeddingQueueMutex);
          }
        relationBlock->addLocalRelationshipType(&type[0]);
      }
      relationBlock->addLocalProperty(std::string(it.key()), &value[0]);
    }
  }
}

void JasmineGraphIncrementalLocalStore::addSourceProperties(
    RelationBlock* relationBlock, const json& sourceJson, string destId) {
  char value[PropertyLink::MAX_VALUE_SIZE] = {};
  char label[NodeBlock::LABEL_SIZE] = {0};
  std::ostringstream textForEmbedding;
incremental_localstore_logger.debug("Adding source properties: " + sourceJson.dump());
  if (sourceJson.contains("properties")) {
    auto sourceProps = json(sourceJson["properties"]);

    if (!sourceProps.empty()) {
      for (auto it = sourceProps.begin(); it != sourceProps.end(); it++) {
          string property = it.value().get<std::string>();
          strcpy(value, it.value().get<std::string>().c_str());
          if (std::string(it.key()) == "label") {
          strcpy(label, it.value().get<std::string>().c_str());
          relationBlock->getSource()->addLabel(&label[0]);
         }
          if (it.key() != "id") {
              textForEmbedding << value << "\n";
          }
          relationBlock->getSource()->addProperty(std::string(it.key()),
                                                     &value[0]);
      }

      if (this->embedNode) {
          incremental_localstore_logger.debug("Embedding node:" + textForEmbedding.str());
        std::string nodeText = textForEmbedding.str();
        if (!nodeText.empty()) {
          if (!faissNodeStore->isEmbeddingExist(sourceJson["id"]) &&
              node_embedding_requests->find(sourceJson["id"]) == node_embedding_requests->end()) {
              incremental_localstore_logger.debug(" Adding embedding request: "+ nodeText);
              pthread_mutex_lock(&embeddingQueueMutex);              // node
              node_embedding_requests->insert({sourceJson["id"].get<std::string>(), nodeText});
              pthread_mutex_unlock(&embeddingQueueMutex);
          }
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
          if (it.key() != "id") {
              textForEmbedding << value << "\n";
          }
              relationBlock->getDestination()->addProperty(std::string(it.key()),
                                                           &value[0]);
      }
      if (this->embedNode) {
        std::string nodeText = textForEmbedding.str();
        if (!nodeText.empty()) {
          if (!faissNodeStore->isEmbeddingExist(destinationJson["id"]) &&
              node_embedding_requests->find(destinationJson["id"]) == node_embedding_requests->end()) {
              incremental_localstore_logger.debug(" Adding embedding request: "+ nodeText);
              pthread_mutex_lock(&embeddingQueueMutex);
              node_embedding_requests->insert({destinationJson["id"].get<std::string>(), nodeText});
              pthread_mutex_unlock(&embeddingQueueMutex);
          }
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
