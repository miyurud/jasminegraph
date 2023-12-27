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

Logger incremental_localstore_logger;

JasmineGraphIncrementalLocalStore::JasmineGraphIncrementalLocalStore(unsigned int graphID, unsigned int partitionID, std::string openMode) {
    gc.graphID = graphID;
    gc.partitionID = partitionID;
    gc.maxLabelSize = 43;   // TODO tmkasun: read from .properties file
    gc.openMode = openMode;  // TODO tmkasun: read from .properties file
    //gc.openMode = "app";  // TODO tmkasun: read from .properties file
    this->nm = new NodeManager(gc);
};

std::pair<std::string, bool> JasmineGraphIncrementalLocalStore::getIDs(std::string edgeString) {
    try {
        auto edgeJson = json::parse(edgeString);

        auto sourceJson = edgeJson["source"];
        auto destinationJson = edgeJson["destination"];
        unsigned int s_pid = sourceJson["pid"];
        unsigned int d_pid = destinationJson["pid"];
        bool isCentralEdge = false;

        if (s_pid != d_pid){
            isCentralEdge= true;
        }
        if (sourceJson.contains("properties")) {
            auto edgeProperties = sourceJson["properties"];
            //auto pid = edgeJson["pid"];
            return {edgeProperties["graphId"], isCentralEdge};
        }
    } catch (const std::exception&) {  // TODO tmkasun: Handle multiple types of exceptions
        incremental_localstore_logger.log(
            "Error while processing edge data = " + edgeString +
                "Could be due to JSON parsing error or error while persisting the data to disk",
            "error");
    }
}

void JasmineGraphIncrementalLocalStore::addLocalEdgeFromString(std::string edgeString) {
    try {
        auto edgeJson = json::parse(edgeString);

        auto sourceJson = edgeJson["source"];
        auto destinationJson = edgeJson["destination"];

        std::string sId = std::string(sourceJson["id"]);
        std::string dId = std::string(destinationJson["id"]);

        RelationBlock* newRelation = this->nm->addEdge({sId, dId});
        if (!newRelation) {
            return;
        }
        char value[PropertyLink::MAX_VALUE_SIZE] = {};

        if (edgeJson.contains("properties")) {
            auto edgeProperties = edgeJson["properties"];
            for (auto it = edgeProperties.begin(); it != edgeProperties.end(); it++) {
                strcpy(value, it.value().get<std::string>().c_str());
                newRelation->addProperty(std::string(it.key()), &value[0]);
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
        incremental_localstore_logger.log("Added successfully!", "Info");
    } catch (const std::exception&) {  // TODO tmkasun: Handle multiple types of exceptions
        incremental_localstore_logger.log(
            "Error while processing edge data = " + edgeString +
                "Could be due to JSON parsing error or error while persisting the data to disk",
            "error");
        incremental_localstore_logger.log("Error malformed JSON attributes!", "error");
        // TODO tmkasun: handle JSON errors
    }
}

void JasmineGraphIncrementalLocalStore::addCentralEdgeFromString(std::string edgeString) {
    try {
        auto edgeJson = json::parse(edgeString);

        auto sourceJson = edgeJson["source"];
        auto destinationJson = edgeJson["destination"];

        std::string sId = std::string(sourceJson["id"]);
        std::string dId = std::string(destinationJson["id"]);

        RelationBlock* newRelation = this->nm->addCentralEdge({sId, dId});
        if (!newRelation) {
            return;
        }
        char value[PropertyLink::MAX_VALUE_SIZE] = {};

        if (edgeJson.contains("properties")) {
            auto edgeProperties = edgeJson["properties"];
            for (auto it = edgeProperties.begin(); it != edgeProperties.end(); it++) {
                strcpy(value, it.value().get<std::string>().c_str());
                newRelation->addCentralProperty(std::string(it.key()), &value[0]);
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
        incremental_localstore_logger.log("Added successfully!", "Info");
    } catch (const std::exception&) {  // TODO tmkasun: Handle multiple types of exceptions
        incremental_localstore_logger.log(
                "Error while processing edge data = " + edgeString +
                "Could be due to JSON parsing error or error while persisting the data to disk",
                "error");
        incremental_localstore_logger.log("Error malformed JSON attributes!", "error");
        // TODO tmkasun: handle JSON errors
    }
}

NodeManager* JasmineGraphIncrementalLocalStore::getNodeManager(){
    return this->nm;
}