//
// Created by kumarawansha on 12/13/24.
//

#include "InstanceHandler.h"
#include "../../../../server/JasmineGraphInstanceProtocol.h"


InstanceHandler::InstanceHandler(std::map<std::string,
        JasmineGraphIncrementalLocalStore*>& incrementalLocalStoreMap)
        : incrementalLocalStoreMap(incrementalLocalStoreMap) { };


void InstanceHandler::handleRequest(int connFd, bool *loop_exit_p,
                                    JasmineGraphIncrementalLocalStore* incrementalLocalStoreInstance,
                                    std::string queryJson){
    NodeManager* nodeManager = incrementalLocalStoreInstance->nm;
    instance_logger.info(std::to_string(incrementalLocalStoreInstance->nm->getGraphID()));
    std::list<NodeBlock*> graph = nodeManager->getGraph();
    OperatorExecutor OperatorExecutor(nodeManager, queryJson);
    OperatorExecutor.start(connFd, loop_exit_p, this);
}

void InstanceHandler::dataPublishToMaster(int connFd, bool *loop_exit_p, std::string message) {
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::QUERY_DATA_START)) {
        *loop_exit_p = true;
        return;
    }

    std::string start_ack(JasmineGraphInstanceProtocol::QUERY_DATA_ACK.length(), 0);
    int return_status = recv(connFd, &start_ack, JasmineGraphInstanceProtocol::QUERY_DATA_ACK.length(), 0);
    if (return_status > 0) {
        instance_logger.info("Received data start ack: "+start_ack);
    } else {
        instance_logger.info("Error while reading start ack");
        *loop_exit_p = true;
        return;
    }

    int message_length = message.length();
    int converted_number = htonl(message_length);
    instance_logger.info("Sending content length"+to_string(converted_number));
    if(!Utils::send_int_wrapper(connFd, &converted_number, sizeof(converted_number))){
        *loop_exit_p = true;
        return;
    }

    std::string length_ack(JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK.length(), 0);
    return_status = recv(connFd, &start_ack, JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK.length(), 0);
    if (return_status > 0) {
        instance_logger.info("Received content length ack: "+length_ack);
    } else {
        instance_logger.info("Error while reading content length ack");
        *loop_exit_p = true;
        return;
    }

    if (!Utils::send_str_wrapper(connFd, message)) {
        *loop_exit_p = true;
        return;
    }

    std::string success_ack(JasmineGraphInstanceProtocol::GRAPH_DATA_SUCCESS.length(), 0);
    return_status = recv(connFd, &success_ack, JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK.length(), 0);
    if (return_status > 0) {
        instance_logger.info("Received content length ack: "+success_ack);
    } else {
        instance_logger.info("Error while reading content length ack");
        *loop_exit_p = true;
        return;
    }

}
