/**
Copyright 2026 JasmineGraph Team
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

#include "AgentPlanExecutor.h"

#include <atomic>
#include <nlohmann/json.hpp>
#include <thread>

#include "../../../../../src/util/Utils.h"
#include "../../../../server/JasmineGraphServer.h"

using json = nlohmann::json;

Logger agent_executor_logger;

AgentPlanExecutor::AgentPlanExecutor() {}

AgentPlanExecutor::AgentPlanExecutor(SQLiteDBInterface* db, PerformanceSQLiteDBInterface* perfDb,
                                     JobRequest jobRequest) {
    this->sqlite = db;
    this->perfDB = perfDb;
    this->request = jobRequest;
}

void AgentPlanExecutor::execute() {
    int uniqueId = getuid();
    std::string workerListStr;
    std::string masterIP = request.getMasterIP();
    std::string query = request.getParameter("query");
    std::string llmRunner = request.getParameter("llm_runner");
    std::string inferenceEngine = request.getParameter("llm_engine");
    std::string llmModel = request.getParameter("llm_model");
    std::string graphId = request.getParameter(Conts::PARAM_KEYS::GRAPH_ID);
    std::string canCalibrateString = request.getParameter(Conts::PARAM_KEYS::CAN_CALIBRATE);
    std::string autoCalibrateString = request.getParameter(Conts::PARAM_KEYS::AUTO_CALIBRATION);

    int numberOfPartitions = std::stoi(request.getParameter(Conts::PARAM_KEYS::NO_OF_PARTITIONS));
    int connFd = std::stoi(request.getParameter(Conts::PARAM_KEYS::CONN_FILE_DESCRIPTOR));
    bool* loop_exit = reinterpret_cast<bool*>(
        static_cast<std::uintptr_t>(std::stoull(request.getParameter(Conts::PARAM_KEYS::LOOP_EXIT_POINTER))));

    bool canCalibrate = Utils::parseBoolean(canCalibrateString);
    bool autoCalibrate = Utils::parseBoolean(autoCalibrateString);

    auto begin = chrono::high_resolution_clock::now();

    JasmineGraphServer::worker designatedWorker = JasmineGraphServer::getDesignatedWorker();

    // Open Socket
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        throw std::runtime_error("Failed to create socket");
    }

    if (designatedWorker.hostname.find('@') != std::string::npos) {
        designatedWorker.hostname = Utils::split(designatedWorker.hostname, '@')[1];
    }

    struct hostent* server = gethostbyname(designatedWorker.hostname.c_str());
    if (!server) {
        close(sockfd);
        throw std::runtime_error("Unknown host: " + designatedWorker.hostname);
    }

    struct sockaddr_in serv_addr{};
    serv_addr.sin_family = AF_INET;
    bcopy((char*)server->h_addr, (char*)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(designatedWorker.port);

    agent_executor_logger.info("Connecting to designated worker: " + designatedWorker.hostname + ":" +
                               std::to_string(designatedWorker.port));

    if (Utils::connect_wrapper(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        close(sockfd);
        throw std::runtime_error("Connection to worker failed");
    }

    // Handshake
    char buffer[FED_DATA_LENGTH + 1];
    if (!Utils::performHandshake(sockfd, buffer, FED_DATA_LENGTH, masterIP)) {
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        throw std::runtime_error("Handshake failed");
    }

    auto sendField = [&](const std::string& fieldName, const std::string& value) {
        agent_executor_logger.info("Sending " + fieldName + " length=" + std::to_string(value.size()));

        int32_t len = htonl(value.size());

        if (send(sockfd, &len, sizeof(len), 0) != sizeof(len)) {
            throw std::runtime_error("Failed to send length for " + fieldName);
        }

        char ack[64]{0};
        int n = recv(sockfd, ack, sizeof(ack), 0);
        if (n <= 0) {
            throw std::runtime_error("No ACK for " + fieldName);
        }

        agent_executor_logger.info("ACK received for " + fieldName + ": " + Utils::trim_copy(ack));

        if (send(sockfd, value.data(), value.size(), 0) != (ssize_t)value.size()) {
            throw std::runtime_error("Failed to send value for " + fieldName);
        }
    };

    std::vector<JasmineGraphServer::worker> workers = JasmineGraphServer::getWorkers(numberOfPartitions);
    size_t workerCount = workers.size();
    for (size_t i = 0; i < workerCount; i++) {
        workerListStr +=
            workers[i].hostname + ":" + std::to_string(workers[i].port) + ":" + std::to_string(workers[i].dataPort);
        if (i + 1 < workers.size()) {
            workerListStr += ",";
        }
    }

    // Protocol sequence
    Utils::send_str_wrapper(sockfd, "initiate-agent-plan");
    sendField("graphId", graphId);
    sendField("query", query);
    sendField("llmRunner", llmRunner);
    sendField("llmEngine", inferenceEngine);
    sendField("llmModel", llmModel);
    sendField("workerList", workerListStr);

    // // --- Receive results line-by-line until -1 ---
    std::string lineBuffer;
    char c;

    while (true) {
        ssize_t bytesRead = recv(sockfd, &c, 1, 0);
        if (bytesRead <= 0) {
            agent_executor_logger.error("Connection closed or error while reading");
            *loop_exit = true;
            break;
        }

        if (c == '\n') {
            if (!lineBuffer.empty() && lineBuffer.back() == '\r') {
                lineBuffer.pop_back();
            }

            std::string line = Utils::trim_copy(lineBuffer);
            lineBuffer.clear();

            if (line.empty()) {
                continue;
             }

            try {
                json payload = json::parse(line);

                if (payload.contains("type") && payload["type"] == "end") {
                    agent_executor_logger.info("End-of-answer marker received");
                    break;
                }

                if (payload.contains("type") && payload["type"] == "answer_chunk") {
                    std::string dataStr = payload["data"];
                    nlohmann::json innerJson = nlohmann::json::parse(dataStr);
                    std::string chunkContent = innerJson["answer"];

                    ssize_t n = write(connFd, chunkContent.c_str(), chunkContent.size());
                    write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());

                    if (n < 0) {
                        agent_executor_logger.error("Error writing to frontend socket");
                        *loop_exit = true;
                        close(sockfd);
                        return;
                    }
                }
            } catch (const std::exception& e) {
                agent_executor_logger.error("JSON parse error: " + std::string(e.what()));
            }
        } else {
            lineBuffer.push_back(c);
        }
    }

    Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::OK);
    close(sockfd);

    workerResponded = true;
    JobResponse jobResponse;
    jobResponse.setJobId(request.getJobId());
    responseVector.push_back(jobResponse);

    responseVectorMutex.lock();
    responseMap[request.getJobId()] = jobResponse;
    responseVectorMutex.unlock();

    auto end = chrono::high_resolution_clock::now();
    auto dur = end - begin;
    auto msDuration = std::chrono::duration_cast<std::chrono::milliseconds>(dur).count();

    std::string durationString = std::to_string(msDuration);
    agent_executor_logger.info("###AGENT-EXECUTOR### Execution Completed. Total time taken: " + durationString + " ms");

    if (canCalibrate || autoCalibrate) {
        Utils::updateSLAInformation(perfDB, graphId, numberOfPartitions, msDuration, CYPHER,
                                    Conts::SLA_CATEGORY::LATENCY);
        isStatCollect = false;
    }

    processStatusMutex.lock();
    for (auto processCompleteIterator = processData.begin(); processCompleteIterator != processData.end();
         ++processCompleteIterator) {
        ProcessInfo processInformation = *processCompleteIterator;
        if (processInformation.id == uniqueId) {
            processData.erase(processInformation);
            break;
        }
    }
    processStatusMutex.unlock();
}

int AgentPlanExecutor::getUid() {
    static std::atomic<std::uint32_t> uid{0};
    return ++uid;
}
