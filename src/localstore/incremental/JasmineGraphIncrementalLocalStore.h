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

#include <string>
#include <nlohmann/json.hpp>
#include <unordered_map>

using json = nlohmann::json;

#include "../../nativestore/NodeManager.h"
#ifndef Incremental_LocalStore
#define Incremental_LocalStore


class JasmineGraphIncrementalLocalStore
{
   public:
    GraphConfig gc;
    NodeManager *nm;
    std::string addNodeFromString(std::string edgeString);
    std::string addEdgeFromString(std::string edgeString);
    static std::pair<std::string, std::string> getIDs(std::string edgeString );
    JasmineGraphIncrementalLocalStore(unsigned int graphID , unsigned int partitionID );

    std::string addCentralEdgeFromString(std::string edgeString);

    std::string addGraphEdgeFromString(std::string edgeString);

    std::string print_node_index();

//    std::unordered_map<std::string,  NodeManager*> nodeManagerIndex;
};

#endif
