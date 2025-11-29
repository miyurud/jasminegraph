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

#include <nlohmann/json.hpp>
#include <string>
using json = nlohmann::json;

#include "../../nativestore/NodeManager.h"
#ifndef Incremental_LocalStore
#define Incremental_LocalStore

// Forward declaration
namespace jasminegraph {
    class TemporalPartitionManager;
}

class JasmineGraphIncrementalLocalStore {
 public:
    GraphConfig gc;
    NodeManager *nm;
    std::unique_ptr<jasminegraph::TemporalPartitionManager> temporalManager;
    void addEdgeFromString(std::string edgeString);
    static std::pair<std::string, unsigned int> getIDs(std::string edgeString);
    JasmineGraphIncrementalLocalStore(unsigned int graphID = 0,
                                      unsigned int partitionID = 0, std::string openMode = "trunk");
    void addLocalEdge(std::string edge);
    void addCentralEdge(std::string edge);
    void addNodeMetaProperty(NodeBlock* nodeBlock, std::string propertyKey, std::string propertyValue);
    void addRelationMetaProperty(RelationBlock* relationBlock, std::string propertyKey, std::string propertyValue);
    void addLocalEdgeProperties(RelationBlock* relationBlock, const json& edgeJson);
    void addCentralEdgeProperties(RelationBlock* relationBlock, const json& edgeJson);
    void addSourceProperties(RelationBlock* relationBlock, const json& sourceJson);
    void addDestinationProperties(RelationBlock* relationBlock, const json& destinationJson);
    
private:
    void processTemporalEdge(const json& edgeJson, const std::string& sId, const std::string& dId, 
                            const std::string& operationType, bool isLocal);
};

#endif
