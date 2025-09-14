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
#include <faiss/IndexFlat.h>

#include "../../nativestore/RelationBlock.h"
#include "../../util/logger/Logger.h"
#include "../../util/Utils.h"
#include "../../nativestore/MetaPropertyLink.h"
#include "../../vectorStore/FaissStore.h"
#include "../../vectorStore/LlamaCPPTextEmbedder.h"
#include "../../vectorStore/TextEmbedder.h"
#include "../../vectorStore/TextEmbedder.h"


Logger incremental_localstore_logger;

JasmineGraphIncrementalLocalStore::JasmineGraphIncrementalLocalStore(unsigned int graphID, unsigned int partitionID,
                                                                     std::string openMode, bool embedNode) {
    gc.graphID = graphID;
    gc.partitionID = partitionID;
    gc.maxLabelSize = std::stoi(Utils::getJasmineGraphProperty("org.jasminegraph.nativestore.max.label.size"));
    this->embedNode = embedNode;
    gc.openMode = openMode;
    this->nm = new NodeManager(gc);
    if (this->embedNode )
    {
        incremental_localstore_logger.info("Embedding enabled for the local store");

        this->faissStore =    FaissStore::getInstance(std::stoi(Utils::getJasmineGraphProperty("org.jasminegraph.vectorstore.dimension")),


         this->nm->getDbPrefix()+ "_faiss.index");
        this->textEmbedder = new TextEmbedder(
                   Utils::getJasmineGraphProperty("org.jasminegraph.vectorstore.embedding.ollama.endpoint") , // Ollama endpoint
                   Utils::getJasmineGraphProperty("org.jasminegraph.vectorstore.embedding.model")                     // model name
                );
        // Provide a small sample to train
        // std::vector<std::vector<float>> sampleVectors;
        // for (int i = 0; i < 10; i++) {
        //     sampleVectors.push_back(std::vector<float>(768, i * 0.01f));
        // }
        // faissStore->addBatch(sampleVectors); // trains automatically and adds vectors


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

    // log the edge information
    incremental_localstore_logger.info("Adding local edge: " + edge);
    if (!jsonSource.contains("id") || !jsonDestination.contains("id")) {
        incremental_localstore_logger.error("Source or destination ID missing in edge data: " + edge);
        return;
    }
    if (!jsonEdge.contains("source") || !jsonEdge.contains("destination")) {
        incremental_localstore_logger.error("Source or destination missing in edge data: " + edge);
        return;
    }
    if (!jsonEdge.contains("properties")) {
        incremental_localstore_logger.error("Properties missing in edge data: " + edge);
        return;
    }
    if (!jsonSource.contains("pid") || !jsonDestination.contains("pid")) {
        incremental_localstore_logger.error("Partition ID missing in source or destination: " + edge);
        return;
    }

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
    std::ostringstream textForEmbedding;

    incremental_localstore_logger.info("Adding source properties");
    incremental_localstore_logger.info(sourceJson["properties"].dump());
    if (sourceJson.contains("properties")) {
        incremental_localstore_logger.info("inside if ");
        auto sourceProps = json(sourceJson["properties"]);

        if (!sourceProps.empty())
        {
            for (auto it = sourceProps.begin(); it != sourceProps.end(); it++) {
                strcpy(value, it.value().get<std::string>().c_str());
                if (std::string(it.key()) == "label") {
                    strcpy(label, it.value().get<std::string>().c_str());
                    relationBlock->getSource()->addLabel(&label[0]);

                }
                incremental_localstore_logger.info("canctenated string "+ textForEmbedding.str());
                textForEmbedding << it.key() << ":" << value << " ";

                relationBlock->getSource()->addProperty(std::string(it.key()), &value[0]);
            }


            if (this->embedNode )
            {
                std::string nodeText = textForEmbedding.str();
                if (!nodeText.empty()) {
                    // static LlamaCPPTextEmbedder embedder("/models/nomic-embed-text-v1.5.Q4_K_S.gguf");
                    // std::vector<float> embedding = embedder.embed(nodeText);
                    incremental_localstore_logger.info("Embedding: ");

                    // TextEmbedder te(
                    //             "http://192.168.1.7:11434/api/embeddings", // Ollama endpoint
                    //             "nomic-embed-text"                        // model name
                    //         );

                    auto emb = textEmbedder->embed(nodeText);

                    faiss::idx_t docId = std::stoll(sourceJson["id"].get<std::string>());
                    // long id = faissStore->add(emb);
                    if (faissStore->getEmbeddingById(std::to_string(docId)).size() == 0) {
                        incremental_localstore_logger.error("Node with ID " + sourceJson["id"].get<std::string>() + " found . Skipping ");
                        return;
                    }

                    faissStore->add(emb,sourceJson["id"].get<std::string>() );



                    auto results = faissStore->search(emb, 5);
                    for (auto& [id, dist] : results) {
                        std::cout << "ID: " << id << ", Distance: " << dist << "\n";
                    }
                    // Store embedding into the node (option A: directly attach as property)
                    incremental_localstore_logger.info("Embedding: " + std::to_string(emb[0]) + " ...");

                    // OR (option B: save into external vector DB / file)
                    // embeddingStore.save(relationBlock->getSource()->getId(), embedding);
                }
            }
        }
}

    std::string sourcePid = std::to_string(sourceJson["pid"].get<int>());
    addNodeMetaProperty(relationBlock->getSource(), MetaPropertyLink::PARTITION_ID,
                        sourcePid);
}

void JasmineGraphIncrementalLocalStore::addDestinationProperties(RelationBlock* relationBlock,
    const json& destinationJson)
{
    char value[PropertyLink::MAX_VALUE_SIZE] = {};
    char label[NodeBlock::LABEL_SIZE] = {0};
    std::ostringstream textForEmbedding;

    if (destinationJson.contains("properties"))
    {
        auto destinationProps = destinationJson["properties"];
        if (!destinationProps.empty())
        {
            for (auto it = destinationProps.begin(); it != destinationProps.end(); it++) {
                strcpy(value, it.value().get<std::string>().c_str());
                if (std::string(it.key()) == "label") {
                    strcpy(label, it.value().get<std::string>().c_str());
                    relationBlock->getDestination()->addLabel(&label[0]);
                }
                textForEmbedding << it.key() << ":" << value << " ";

                relationBlock->getDestination()->addProperty(std::string(it.key()), &value[0]);
            }

            // std::string destPId = std::to_string(destinationJson["pid"].get<int>());
            // addNodeMetaProperty(relationBlock->getDestination(), MetaPropertyLink::PARTITION_ID,
            //                     destPId);
            if (this->embedNode )
            {
                std::string nodeText = textForEmbedding.str();
                if (!nodeText.empty()) {
                    // static LlamaCPPTextEmbedder embedder("/models/nomic-embed-text-v1.5.Q4_K_S.gguf");
                    // std::vector<float> embedding = embedder.embed(nodeText);
                    incremental_localstore_logger.info("Embedding: ");

                    // TextEmbedder te(
                    //             "http://192.168.1.7:11434/api/embeddings", // Ollama endpoint
                    //             "nomic-embed-text"                        // model name
                    //         );

                    auto emb = textEmbedder->embed(nodeText);

                    faiss::idx_t docId = std::stoll(destinationJson["id"].get<std::string>());
                    // long id = faissStore->add(emb);
                    if (faissStore->getEmbeddingById(std::to_string(docId)).size() == 0) {
                        incremental_localstore_logger.error("Node with ID " + destinationJson["id"].get<std::string>() + " found . Skipping ");
                        return;
                    }

                    faissStore->add(emb,destinationJson["id"].get<std::string>() );


                    auto results = faissStore->search(emb, 5);
                    for (auto& [id, dist] : results) {
                        std::cout << "ID: " << id << ", Distance: " << dist << "\n";
                    }
                    // Store embedding into the node (option A: directly attach as property)
                    incremental_localstore_logger.info("Embedding: " + std::to_string(emb[0]) + " ...");

                    // OR (option B: save into external vector DB / file)
                    // embeddingStore.save(relationBlock->getSource()->getId(), embedding);
                }
            }

        }
    }
    std::string destPId = std::to_string(destinationJson["pid"].get<int>());
    addNodeMetaProperty(relationBlock->getDestination(), MetaPropertyLink::PARTITION_ID,
                        destPId);
}

void JasmineGraphIncrementalLocalStore::addNodeMetaProperty(NodeBlock* nodeBlock,
                                                        std::string propertyKey, std::string propertyValue) {
    incremental_localstore_logger.debug( "meta property: " + propertyKey + " " + propertyValue );
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