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
    OperatorExecutor(GraphConfig gc, string queryPlan, string masterIP);
    void AllNodeScan(SharedBuffer &buffer, string jsonPlan, GraphConfig gc);
    void ProduceResult(SharedBuffer &buffer, string jsonPlan, GraphConfig gc);
    void Filter(SharedBuffer &buffer, string jsonPlan, GraphConfig gc);
    void ExpandAll(SharedBuffer &buffer, string jsonPlan, GraphConfig gc);
    void UndirectedRelationshipTypeScan(SharedBuffer &buffer, string jsonPlan, GraphConfig gc);
    void UndirectedAllRelationshipScan(SharedBuffer &buffer, string jsonPlan, GraphConfig gc);
    void NodeByIdSeek(SharedBuffer &buffer, string jsonPlan, GraphConfig gc);
    void EargarAggregation(SharedBuffer &buffer, string jsonPlan, GraphConfig gc);
    void Projection(SharedBuffer &buffer, string jsonPlan, GraphConfig gc);
    void Distinct(SharedBuffer &buffer, string jsonPlan, GraphConfig gc);
    void OrderBy(SharedBuffer &buffer, string jsonPlan, GraphConfig gc);
    string masterIP;
    string  queryPlan;
    GraphConfig gc;
    json query;
    static std::unordered_map<std::string, std::function<void(OperatorExecutor &, SharedBuffer &, std::string, GraphConfig)>> methodMap;
    static void initializeMethodMap();
};


#endif //JASMINEGRAPH_OPERATOREXECUTOR_H
