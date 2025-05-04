/**
Copyright 2025 JasmineGraph Team
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

class InstanceHandler {
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

#endif  // JASMINEGRAPH_INSTANCEHANDLER_H
