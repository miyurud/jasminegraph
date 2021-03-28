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
        // NodeBlock* destinationNode = nodeManagers.at(destinationJson["pid"].get<int>())->addNode(dId);
    } catch (const std::exception &) {
        incremental_localstore_logger.log("Error malformed JSON attributes!", "error");
        // TODO handle JSON errors
    }
}