//
// Created by kumarawansha on 12/13/24.
//

#include "InstanceHandler.h"

InstanceHandler::InstanceHandler(std::map<std::string,
        JasmineGraphIncrementalLocalStore*>& incrementalLocalStoreMap)
        : incrementalLocalStoreMap(incrementalLocalStoreMap) { };


void InstanceHandler::handleRequest(const std::string &nodeString){
        

        instance_logger.info(std::to_string(incrementalLocalStoreMap.size()));
        for(auto incrementalLocalStore: incrementalLocalStoreMap){
                instance_logger.info(std::to_string(incrementalLocalStore.second->nm->getGraphID()));
        }
}
