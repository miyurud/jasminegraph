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
#include <stdexcept>
#include "JasmineGraphIncrementalLocalStore.h"
#include <nlohmann/json.hpp>
using json = nlohmann::json;

#include "../../util/logger/Logger.h"

Logger incremental_localstore_logger;

JasmineGraphIncrementalLocalStore::JasmineGraphIncrementalLocalStore(unsigned int graphID, unsigned int partitionID){
    gc.graphID = graphID;
    gc.partitionID = partitionID;
    gc.maxLabelSize = 10;
    gc.openMode = "trunk";
    this->nm = new NodeManager(gc);
};

std::string JasmineGraphIncrementalLocalStore::addEdgeFromString(std::string edgeString)
{
    try {
        auto edge = json::parse(edgeString);
        NodeBlock *sourceNode = this->nm->addNode(edge["id"]);
    } catch (const std::exception &) {
        incremental_localstore_logger.log("Error malformed JSON attributes!", "error");
        //TODO tmkasun: handle JSON errors
    }
}