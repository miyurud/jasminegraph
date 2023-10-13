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
#include <stdio.h>
#include <iostream>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <string>

#include "../../nativestore/RelationBlock.h"
#include "../../util/logger/Logger.h"
#include "../../nativestore/CtypesLibrary.h"


Logger incremental_localstore_logger;
CtypesLibrary lib;

JasmineGraphIncrementalLocalStore::JasmineGraphIncrementalLocalStore(unsigned int graphID, unsigned int partitionID) {
    gc.graphID = graphID;
    gc.partitionID = partitionID;
    gc.maxLabelSize = 43;   // TODO tmkasun: read from .properties file
    gc.openMode = "trunc";  // TODO tmkasun: read from .properties file
    this->nm = new NodeManager(gc);
    std::string graphIdentifier = graphID + "_" + partitionID;
    lib.nodeManagerIndex.insert({graphIdentifier, this->nm});

};

std::pair<std::string, std::string> JasmineGraphIncrementalLocalStore::getIDs(std::string edgeString) {
    try {

        auto edgeJson = json::parse(edgeString);
        if (edgeJson.contains("graphIdentifier")) {
            auto graphIdentifier = edgeJson["graphIdentifier"];
            std::string graphId = std::string(graphIdentifier["graphId"]);
            std::string pId = to_string(graphIdentifier["pid"]);
            return {graphId, pId};
        }


    } catch (const std::exception &) {  // TODO tmkasun: Handle multiple types of exceptions
        incremental_localstore_logger.log(
                "Error while processing node ID data = " + edgeString +
                "Could be due to JSON parsing error or error while persisting the data to disk",
                "error");
    }
}

std::string JasmineGraphIncrementalLocalStore::addGraphEdgeFromString(std::string edgeString) {
    try {
        auto edgeJson = json::parse(edgeString);
        auto sourceJson = edgeJson["source"];
        auto destinationJson = edgeJson["destination"];
        auto graphIdentifierJson = edgeJson["graphIdentifier"];

        std::string sId = std::string(sourceJson["id"]);
        long source_pid = sourceJson["pid"];
        std::string dId = std::string(destinationJson["id"]);
        long destination_pid = destinationJson["pid"];

        long edge_pid = graphIdentifierJson["pid"];


        if (source_pid == destination_pid) {
            char valueSource[PropertyLink::MAX_VALUE_SIZE] = {};
            char valueDes[PropertyLink::MAX_VALUE_SIZE] = {};

//                    store source nodeBlock
            if (nm->nodeIndex.find(sId) == nm->nodeIndex.end()) {
                NodeBlock *sourceBlock = this->nm->addNode(sId);
                if (sourceJson.contains("properties")) {
                    auto sourceProps = sourceJson["properties"];
                    for (auto it = sourceProps.begin(); it != sourceProps.end(); it++) {
                        strcpy(valueSource, it.value().get<std::string>().c_str());
                        sourceBlock->addProperty(std::string(it.key()), &valueSource[0]);
                    }
                }

            }
        //        store destination nodeBlock
            if (nm->nodeIndex.find(dId) == nm->nodeIndex.end()) {
                NodeBlock *destinationBlock = this->nm->addNode(dId);
                if (destinationJson.contains("properties")) {
                    auto destinationProps = destinationJson["properties"];
                    for (auto it = destinationProps.begin(); it != destinationProps.end(); it++) {
                        strcpy(valueDes, it.value().get<std::string>().c_str());
                        destinationBlock->addProperty(std::string(it.key()), &valueDes[0]);
                    }
                }
            }

//            NodeBlock *sourceBlock = this->nm->addNode(sId);
//            NodeBlock *destinationBlock = this->nm->addNode(dId);

            RelationBlock *newRelation = this->nm->addEdge({sId, dId});
            if (newRelation) {
                char value[PropertyLink::MAX_VALUE_SIZE] = {};

                if (edgeJson.contains("properties")) {
                    auto edgeProperties = edgeJson["properties"];
                    for (auto it = edgeProperties.begin(); it != edgeProperties.end(); it++) {
                        strcpy(value, it.value().get<std::string>().c_str());
                        newRelation->addProperty(std::string(it.key()), &value[0]);
                    }
                }


            }
        } else {
//            if (source_pid == edge_pid) {
//                if (nm->nodeIndex.find(sId) == nm->nodeIndex.end()) {
//                    NodeBlock *sourceBlock = this->nm->addNode(sId);
//                    char value[PropertyLink::MAX_VALUE_SIZE] = {};
//
//                    if (sourceJson.contains("properties")) {
//                        auto sourceProps = sourceJson["properties"];
//                        for (auto it = sourceProps.begin(); it != sourceProps.end(); it++) {
//                            strcpy(value, it.value().get<std::string>().c_str());
//                            sourceBlock->addProperty(std::string(it.key()), &value[0]);
//                        }
//                    }
//                }
//            } else {
//                if (nm->nodeIndex.find(dId) == nm->nodeIndex.end()) {
//                    NodeBlock *destinationBlock = this->nm->addNode(dId);
//                    char value[PropertyLink::MAX_VALUE_SIZE] = {};
//
//                    if (destinationJson.contains("properties")) {
//                        auto destinationProps = destinationJson["properties"];
//                        for (auto it = destinationProps.begin(); it != destinationProps.end(); it++) {
//                            strcpy(value, it.value().get<std::string>().c_str());
//                            destinationBlock->addProperty(std::string(it.key()), &value[0]);
//                        }
//                    }
//                }
//            }
//            RelationBlock *newCentralRelation = this->nm->addCentralEdge({sId, dId});
//            if (newCentralRelation) {
//                char value[PropertyLink::MAX_VALUE_SIZE] = {};
//
//                if (edgeJson.contains("properties")) {
//                    auto edgeProperties = edgeJson["properties"];
//                    for (auto it = edgeProperties.begin(); it != edgeProperties.end(); it++) {
//                        strcpy(value, it.value().get<std::string>().c_str());
//                        newCentralRelation->addCentralProperty(std::string(it.key()), &value[0]);
//                    }
//                }
//            }
        }
    }
    catch (const std::exception &) {  // TODO tmkasun: Handle multiple types of exceptions
        incremental_localstore_logger.log(
                "Error while processing all data = " + edgeString +
                "Could be due to JSON parsing error or error while persisting the data to disk",
                "error");
        incremental_localstore_logger.log("Error malformed JSON attributes!", "error");
        // TODO tmkasun: handle JSON errors
    }
}

extern "C" {
std::string JasmineGraphIncrementalLocalStore::print_node_index() {
    std::cout << "edgeRef" << std::endl;

//    NodeBlock *nodeBlockPointer = NULL;
//    if (nm->nodeIndex.find(nodeId) == nm->nodeIndex.end()) {
//        // Not found
//        return "Not found";
//    }
//    unsigned int nodeIndex = nm->nodeIndex[nodeId];
//    return std::to_string(nodeIndex);

}
}
