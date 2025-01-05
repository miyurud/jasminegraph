//
// Created by kumarawansha on 1/2/25.
//

#ifndef JASMINEGRAPH_OPERATOREXECUTOR_H
#define JASMINEGRAPH_OPERATOREXECUTOR_H
#include "../../../../nativestore/NodeManager.h"
#include "InstanceHandler.h"
#include "sharedBuffer.h"
#include <string>
#include <vector>

using namespace  std;

class InstanceHandler;

class OperatorExecutor {
 public:
    OperatorExecutor(NodeManager* nodeManager, string queryPlan);
    void start(int connFd, bool *loop_exit_p, InstanceHandler* instanceHandler);
    void AllnodeScan(SharedBuffer &buffer, string jsonPlan);
    void ProduceResult(SharedBuffer &buffer, string jsonPlan);
    string  queryPlan;
    NodeManager* nm;
    json query;
    static std::unordered_map<std::string, std::function<void(OperatorExecutor &, SharedBuffer &, std::string)>> methodMap;
    static void initializeMethodMap();
};


#endif //JASMINEGRAPH_OPERATOREXECUTOR_H
