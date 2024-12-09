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

Logger incremental_localstore_logger;

JasmineGraphIncrementalLocalStore::JasmineGraphIncrementalLocalStore(unsigned int graphID, unsigned int partitionID,
                                                                     std::string openMode) {
    gc.graphID = graphID;
    gc.partitionID = partitionID;
    gc.maxLabelSize = std::stoi(Utils::getJasmineGraphProperty("org.jasminegraph.nativestore.max.label.size"));
    gc.openMode = openMode;
    this->nm = new NodeManager(gc);
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
    return {"", 0}; // all plath of the function must return std::pair<std::string, unsigned int> type object even there is an error
}

void JasmineGraphIncrementalLocalStore::addEdgeFromString(std::string edgeString) {
    try {
        auto edgeJson = json::parse(edgeString);

        auto sourceJson = edgeJson["source"];
        auto destinationJson = edgeJson["destination"];

        std::string sId = std::string(sourceJson["id"]);
        std::string dId = std::string(destinationJson["id"]);

        RelationBlock* newRelation;
        if (edgeJson["EdgeType"] == "Central") {
            newRelation = this->nm->addCentralEdge({sId, dId});
        } else {
            newRelation = this->nm->addLocalEdge({sId, dId});
        }
        if (!newRelation) {
            return;
        }
        char value[PropertyLink::MAX_VALUE_SIZE] = {};

        if (edgeJson.contains("properties")) {
            auto edgeProperties = edgeJson["properties"];
            for (auto it = edgeProperties.begin(); it != edgeProperties.end(); it++) {
                strcpy(value, it.value().get<std::string>().c_str());
                if (edgeJson["EdgeType"] == "Central") {
                    newRelation->addCentralProperty(std::string(it.key()), &value[0]);
                } else {
                    newRelation->addLocalProperty(std::string(it.key()), &value[0]);
                }
            }
        }

        if (sourceJson.contains("properties")) {
            auto sourceProps = sourceJson["properties"];
            for (auto it = sourceProps.begin(); it != sourceProps.end(); it++) {
                strcpy(value, it.value().get<std::string>().c_str());
                newRelation->getSource()->addProperty(std::string(it.key()), &value[0]);
            }
        }
        if (destinationJson.contains("properties")) {
            auto destProps = destinationJson["properties"];
            for (auto it = destProps.begin(); it != destProps.end(); it++) {
                strcpy(value, it.value().get<std::string>().c_str());
                newRelation->getDestination()->addProperty(std::string(it.key()), &value[0]);
            }
        }

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
