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

#include "InstanceHandler.h"
#include "../../../../server/JasmineGraphInstanceProtocol.h"


InstanceHandler::InstanceHandler(std::map<std::string,
        JasmineGraphIncrementalLocalStore*>& incrementalLocalStoreMap)
        : incrementalLocalStoreMap(incrementalLocalStoreMap) { };


void InstanceHandler::handleRequest(int connFd, bool *loop_exit_p,
                                    GraphConfig gc, string masterIP,
                                    std::string queryJson) {
    OperatorExecutor operatorExecutor(gc, queryJson, masterIP);
    operatorExecutor.initializeMethodMap();
    SharedBuffer sharedBuffer(5);
    auto method = OperatorExecutor::methodMap[operatorExecutor.query["Operator"]];
    // Launch the method in a new thread
    std::thread result(method, std::ref(operatorExecutor), std::ref(sharedBuffer),
                       std::string(operatorExecutor.queryPlan), gc);
    auto startTime = std::chrono::high_resolution_clock::now();
    while (true) {
        string raw = sharedBuffer.get();
        if (raw == "-1") {
            this->dataPublishToMaster(connFd, loop_exit_p, raw);
            auto endTime = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
            int totalTime = duration.count();
            json timeJson = {
                    {"time", totalTime},
            };
            this->dataPublishToMaster(connFd, loop_exit_p, timeJson.dump());
            result.join();
            break;
        }
        this->dataPublishToMaster(connFd, loop_exit_p, raw);
    }
}

void InstanceHandler::dataPublishToMaster(int connFd, bool *loop_exit_p, std::string message) {
    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::QUERY_DATA_START)) {
        *loop_exit_p = true;
        return;
    }

    std::string start_ack(JasmineGraphInstanceProtocol::QUERY_DATA_ACK.length(), 0);
    int return_status = recv(connFd, &start_ack[0], JasmineGraphInstanceProtocol::QUERY_DATA_ACK.length(), 0);
    if (return_status > 0) {
        instance_logger.debug("Received data start ack: "+start_ack);
    } else {
        instance_logger.error("Error while reading start ack");
        *loop_exit_p = true;
        return;
    }

    int message_length = message.length();
    int converted_number = htonl(message_length);
    instance_logger.debug("Sending content length"+to_string(converted_number));
    if (!Utils::send_int_wrapper(connFd, &converted_number, sizeof(converted_number))) {
        *loop_exit_p = true;
        return;
    }

    std::string length_ack(JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK.length(), 0);
    return_status = recv(connFd, &length_ack[0], JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK.length(), 0);
    if (return_status > 0) {
        instance_logger.debug("Received content length ack: "+length_ack);
    } else {
        instance_logger.error("Error while reading content length ack");
        *loop_exit_p = true;
        return;
    }

    if (!Utils::send_str_wrapper(connFd, message)) {
        *loop_exit_p = true;
        return;
    }

    std::string success_ack(JasmineGraphInstanceProtocol::GRAPH_DATA_SUCCESS.length(), 0);
    return_status = recv(connFd, &success_ack[0], JasmineGraphInstanceProtocol::GRAPH_DATA_SUCCESS.length(), 0);
    if (return_status > 0) {
        instance_logger.debug("Received success ack: "+ success_ack);
    } else {
        instance_logger.error("Error while reading content length ack");
        *loop_exit_p = true;
        return;
    }
}
