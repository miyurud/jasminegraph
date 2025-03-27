//
// Created by kumarawansha on 12/13/24.
//

#ifndef JASMINEGRAPH_INSTANCEHANDLER_H
#define JASMINEGRAPH_INSTANCEHANDLER_H

#include <map>
#include <string>
#include <algorithm>
#include <vector>
#include <future>
#include <sstream>
#include "../../../../localstore/incremental/JasmineGraphIncrementalLocalStore.h"
#include "../../../../util/logger/Logger.h"
#include "../../../../util/Utils.h"
#include "../../../../nativestore/RelationBlock.h"
#include "OperatorExecutor.h"

class InstanceHandler
{
public:
    Logger instance_logger;
    InstanceHandler(std::map<std::string, JasmineGraphIncrementalLocalStore *> &incrementalLocalStoreMap);
    void handleRequest(int connFd, bool *loop_exit_p, GraphConfig gc, string masterIP,
                       std::string queryJson);
    void dataPublishToMaster(int connFd, bool *loop_exit_p, std::string message);


private:
    std::map<std::string,
             JasmineGraphIncrementalLocalStore *> &incrementalLocalStoreMap;
};

#endif // JASMINEGRAPH_INSTANCEHANDLER_H
