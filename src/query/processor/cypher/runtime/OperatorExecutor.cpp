//
// Created by kumarawansha on 1/2/25.
//

#include "OperatorExecutor.h"
#include "InstanceHandler.h"

OperatorExecutor::OperatorExecutor(NodeManager* nodeManager, std::string queryPlan):
    queryPlan(queryPlan), nm(nodeManager){
    this->query = json::parse(queryPlan);
};

void OperatorExecutor::initializeMethodMap() {
    methodMap["AllNodeScan"] = [](OperatorExecutor &executor, SharedBuffer &buffer, std::string jsonPlan) {
        executor.AllnodeScan(buffer, jsonPlan);
    };

    methodMap["ProduceResult"] = [](OperatorExecutor &executor, SharedBuffer &buffer, std::string jsonPlan) {
        executor.ProduceResult(buffer, jsonPlan); // Ignore the unused string parameter
    };
}

void OperatorExecutor::start(int connFd, bool *loop_exit_p, InstanceHandler *instanceHandler) {
    this->initializeMethodMap();
    SharedBuffer sharedBuffer(5);
    auto method = OperatorExecutor::methodMap[this->query["Operator"]];
    // Launch the method in a new thread
    std::thread result(method, std::ref(*this), std::ref(sharedBuffer), this->queryPlan);
    result.detach(); // Detach the thread to let it run independently
    while(true) {
        string raw = sharedBuffer.get();
        if(raw == "-1"){
            break;
        }
        instanceHandler->dataPublishToMaster(connFd, loop_exit_p, raw);
    }
}

void OperatorExecutor::AllnodeScan(SharedBuffer &buffer,std::string jsonPlan) {
    json query = json::parse(jsonPlan);
    NodeManager* nodeManager = this->nm;
    for (auto it : nodeManager->nodeIndex) {
        json nodeData;
        auto nodeId = it.first;
        NodeBlock *node = nodeManager->get(nodeId);
        std::map<std::string, char*> properties = node->getAllProperties();
        for (auto property: properties){
            nodeData[property.first] = property.second;
        }
        buffer.add(nodeData.dump());
    }
}

void OperatorExecutor::ProduceResult(SharedBuffer &buffer, std::string jsonPlan) {
    json query = json::parse(jsonPlan);
    query = json::parse(query["NextOperator"]);
    SharedBuffer sharedBuffer(5);

    auto method = OperatorExecutor::methodMap[query["Operator"]];
    // Launch the method in a new thread
    std::thread result(method, std::ref(*this), std::ref(sharedBuffer), query["NextOperator"]);
    result.detach(); // Detach the thread to let it run independently
    while(true) {
        string raw = sharedBuffer.get();
        buffer.add(raw);
        if(raw == "-1"){
            break;
        }
    }
}