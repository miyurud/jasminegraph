//
// Created by kumarawansha on 1/2/25.
//

#ifndef JASMINEGRAPH_OPERATOREXECUTOR_H
#define JASMINEGRAPH_OPERATOREXECUTOR_H
#include "../../../../nativestore/NodeManager.h"
#include "InstanceHandler.h"
#include "../util/SharedBuffer.h"
#include <string>
#include <vector>

using namespace  std;

class OperatorExecutor {
 public:
    OperatorExecutor(GraphConfig gc, string queryPlan);
    void AllnodeScan(SharedBuffer &buffer, string jsonPlan, GraphConfig gc);
    void ProduceResult(SharedBuffer &buffer, string jsonPlan, GraphConfig gc);
    void Filter(SharedBuffer &buffer, string jsonPlan, GraphConfig gc);
    string  queryPlan;
    GraphConfig gc;
    json query;
    static std::unordered_map<std::string, std::function<void(OperatorExecutor &, SharedBuffer &, std::string, GraphConfig)>> methodMap;
    static void initializeMethodMap();
};


#endif //JASMINEGRAPH_OPERATOREXECUTOR_H
