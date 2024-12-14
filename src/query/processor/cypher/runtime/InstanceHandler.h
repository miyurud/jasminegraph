//
// Created by kumarawansha on 12/13/24.
//

#ifndef JASMINEGRAPH_INSTANCEHANDLER_H
#define JASMINEGRAPH_INSTANCEHANDLER_H

#include <map>
#include <string>
#include "../../../../localstore/incremental/JasmineGraphIncrementalLocalStore.h"
#include "../../../../util/logger/Logger.h"

class InstanceHandler
{
public:
    InstanceHandler(std::map<std::string, JasmineGraphIncrementalLocalStore *> &incrementalLocalStoreMap);

    void handleRequest(const std::string &nodeString);
    Logger instance_logger;

private:
    std::map<std::string,
             JasmineGraphIncrementalLocalStore *> &incrementalLocalStoreMap;
};

#endif // JASMINEGRAPH_INSTANCEHANDLER_H
