//
// Created by kumarawansha on 1/2/25.
//

#include "OperatorExecutor.h"
#include "InstanceHandler.h"
#include "../util/Const.h"
#include "Helpers.h"
#include <thread>

std::unordered_map<std::string, std::function<void(OperatorExecutor&, SharedBuffer&, std::string, GraphConfig)>> OperatorExecutor::methodMap;
OperatorExecutor::OperatorExecutor(GraphConfig gc, std::string queryPlan):
    queryPlan(queryPlan), gc(gc){
    this->query = json::parse(queryPlan);
};

void OperatorExecutor::initializeMethodMap() {
    methodMap["AllNodeScan"] = [](OperatorExecutor &executor, SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
        executor.AllnodeScan(buffer, jsonPlan, gc);
    };

    methodMap["ProduceResult"] = [](OperatorExecutor &executor, SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
        executor.ProduceResult(buffer, jsonPlan, gc); // Ignore the unused string parameter
    };

    methodMap["Filter"] = [](OperatorExecutor &executor, SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
        executor.Filter(buffer, jsonPlan, gc); // Ignore the unused string parameter
    };
}


void OperatorExecutor::AllnodeScan(SharedBuffer &buffer,std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    NodeManager nodeManager(gc);

    for (auto it : nodeManager.nodeIndex) {
        json nodeData;
        auto nodeId = it.first;
        NodeBlock *node = nodeManager.get(nodeId);
        std::string value(node->getMetaPropertyHead()->value);
        if(value == to_string(gc.partitionID)){
            std::map<std::string, char*> properties = node->getAllProperties();
            for (auto property: properties){
                nodeData[property.first] = property.second;
            }
            json data;
            string variable = query["variables"];
            data[variable] = nodeData;
            buffer.add(data.dump());
        }
    }
    buffer.add("-1");
}

void OperatorExecutor::ProduceResult(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    SharedBuffer sharedBuffer(5);
    std::string nextOpt = query["NextOperator"];
    json next = json::parse(nextOpt);
    auto method = OperatorExecutor::methodMap[next["Operator"]];
    // Launch the method in a new thread
    std::thread result(method, std::ref(*this), std::ref(sharedBuffer), query["NextOperator"], gc);
    result.detach(); // Detach the thread to let it run independently

    while(true) {
        string raw = sharedBuffer.get();
        buffer.add(raw);
        if(raw == "-1"){
            break;
        }
    }
}

void OperatorExecutor::Filter(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    SharedBuffer sharedBuffer(5);
    std::string nextOpt = query["NextOperator"];
    json next = json::parse(nextOpt);
    auto method = OperatorExecutor::methodMap[next["Operator"]];
    // Launch the method in a new thread
    std::thread result(method, std::ref(*this), std::ref(sharedBuffer), query["NextOperator"], gc);
    result.detach(); // Detach the thread to let it run independently

    auto condition = query["condition"];
    FilterHelper FilterHelper(condition.dump());
    cout<<condition.dump()<<endl;
    while(true) {
        string raw = sharedBuffer.get();
        if (FilterHelper.evaluate(raw)){
            buffer.add(raw);
        }
        if(raw == "-1"){
            buffer.add(raw);
            break;
        }
    }
}