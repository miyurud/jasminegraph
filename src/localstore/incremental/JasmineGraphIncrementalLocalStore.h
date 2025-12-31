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

#include <memory>
#include <nlohmann/json.hpp>
#include <string>

#include "../../vectorstore/FaissIndex.h"
#include "../../vectorstore/TextEmbedder.h"
using json = nlohmann::json;

#include "../../nativestore/NodeManager.h"
#include "../../nativestore/temporal/TemporalEventLogger.h"
#ifndef Incremental_LocalStore
#define Incremental_LocalStore

struct EmbeddingRequest {
    std::string nodeId;
    std::string nodeText;
};

class JasmineGraphIncrementalLocalStore {
 public:
    GraphConfig gc;
    NodeManager* nm;
    std::unique_ptr<TemporalEventLogger> temporalLogger;
    FaissIndex* faissStore;
    TextEmbedder* textEmbedder;
    std::vector<EmbeddingRequest>* embedding_requests;
    // batch texts to embed

    bool embedNode;
    void addEdgeFromString(std::string edgeString);
    static std::pair<std::string, unsigned int> getIDs(std::string edgeString);
    JasmineGraphIncrementalLocalStore(unsigned int graphID = 0, unsigned int partitionID = 0,
                                      std::string openMode = "trunk", bool embedNode = false);
    ~JasmineGraphIncrementalLocalStore() { delete nm; }
    bool getAndStoreEmbeddings();
    void addLocalEdge(std::string edge);
    void addCentralEdge(std::string edge);
    void addNodeMetaProperty(NodeBlock* nodeBlock, std::string propertyKey, std::string propertyValue);
    void addRelationMetaProperty(RelationBlock* relationBlock, std::string propertyKey, std::string propertyValue);
    void addLocalEdgeProperties(RelationBlock* relationBlock, const json& edgeJson);
    void addCentralEdgeProperties(RelationBlock* relationBlock, const json& edgeJson);
    void addSourceProperties(RelationBlock* relationBlock, const json& sourceJson);
    void addDestinationProperties(RelationBlock* relationBlock, const json& destinationJson);

 private:
    std::string resolveOperationType(const json& edgeJson) const;
    std::string resolveOperationTimestamp(const json& edgeJson) const;
   void logTemporalEvent(const json& edgeJson, const std::string& operationType,
                    const std::string& operationTimestamp);
    bool handleNodeOperation(const json& nodeJson, const std::string& operationType,
                             const std::string& operationTimestamp);
    bool handleEdgeAddition(const json& edgeJson, const std::string& operationType,
                            const std::string& operationTimestamp);
    bool handleEdgeUpdate(const json& edgeJson, bool isLocal, const std::string& operationType,
                          const std::string& operationTimestamp);
    bool handleEdgeDeletion(const json& edgeJson, bool isLocal, const std::string& operationType,
                            const std::string& operationTimestamp);
    RelationBlock* findRelation(const std::string& sId, const std::string& dId, bool isLocal);
    void attachTemporalMeta(RelationBlock* relationBlock, const std::string& operationType,
                            const std::string& operationTimestamp);
};

#endif
