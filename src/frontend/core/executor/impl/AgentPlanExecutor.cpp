#include "AgentPlanExecutor.h"
#include "../../../../../src/rag/agent/AgentProtocol.h"
#include "../../../../server/JasmineGraphServer.h"
#include "../../../../../src/rag/agent/PlanDecoder.h"
#include "../../../../../src/util/Utils.h"
#include <nlohmann/json.hpp>
#include <atomic>
#include <thread>

using json = nlohmann::json;

Logger agent_executor_logger;

AgentPlanExecutor::AgentPlanExecutor() {}

AgentPlanExecutor::AgentPlanExecutor(SQLiteDBInterface *db,
                                     PerformanceSQLiteDBInterface *perfDb,
                                     JobRequest jobRequest) {
    this->sqlite = db;
    this->perfDB = perfDb;
    this->request = jobRequest;
}

void AgentPlanExecutor::execute() {
    agent_executor_logger.info("[AGENT EXECUTOR] Starting");
    AgentRequestContext agentRequestCtx;
    int uniqueId = getuid();
    std::string masterIP = request.getMasterIP();
    std::string query = request.getParameter("query");
    std::string llmRunner = request.getParameter("llm_runner");
    std::string inferenceEngine = request.getParameter("llm_engine");
    std::string llmModel = request.getParameter("llm_model");
    std::string graphId = request.getParameter(Conts::PARAM_KEYS::GRAPH_ID);

    agentRequestCtx.query = query;
    agentRequestCtx.llmRunner = llmRunner;
    agentRequestCtx.llmEngine = inferenceEngine;
    agentRequestCtx.llmModel = llmModel;
    agentRequestCtx.graphId = graphId;

    int numberOfPartitions = std::stoi(request.getParameter(Conts::PARAM_KEYS::NO_OF_PARTITIONS));
    int connFd = std::stoi(request.getParameter(Conts::PARAM_KEYS::CONN_FILE_DESCRIPTOR));
    bool *loop_exit = reinterpret_cast<bool *>(static_cast<std::uintptr_t>(std::stoull(
        request.getParameter(Conts::PARAM_KEYS::LOOP_EXIT_POINTER))));

    // JasmineGraphServer::worker designatedWorker = JasmineGraphServer::getDesignatedWorker();
    JasmineGraphServer::worker designatedWorker{"10.8.100.22", 7780, 7781};

    // Open Socket
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        throw std::runtime_error("Failed to create socket");
    }

    if (designatedWorker.hostname.find('@') != std::string::npos)
    {
        designatedWorker.hostname = Utils::split(designatedWorker.hostname, '@')[1];
    }

    struct hostent *server = gethostbyname(designatedWorker.hostname.c_str());
    if (!server) {
        close(sockfd);
        throw std::runtime_error("Unknown host: " + designatedWorker.hostname);
    }

    struct sockaddr_in serv_addr{};
    serv_addr.sin_family = AF_INET;
    bcopy(
        (char *)server->h_addr,
        (char *)&serv_addr.sin_addr.s_addr,
        server->h_length);
    serv_addr.sin_port = htons(designatedWorker.port);

    agent_executor_logger.info("[AGENT EXECUTOR] Connecting to " + designatedWorker.hostname + ":" + std::to_string(designatedWorker.port));

    if (Utils::connect_wrapper(
            sockfd,
            (struct sockaddr *)&serv_addr,
            sizeof(serv_addr)) < 0) {
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

    auto sendField = [&](const std::string &fieldName,
                         const std::string &value) {
        agent_executor_logger.info(
            "Sending " + fieldName + " length=" + std::to_string(value.size()));

        int32_t len = htonl(value.size());

        if (send(sockfd, &len, sizeof(len), 0) != sizeof(len)) {
            throw std::runtime_error("Failed to send length for " + fieldName);
        }

        char ack[64]{0};
        int n = recv(sockfd, ack, sizeof(ack), 0);
        if (n <= 0) {
            throw std::runtime_error("No ACK for " + fieldName);
        }

        agent_executor_logger.info(
            "ACK received for " + fieldName + ": " +
            Utils::trim_copy(ack));

        if (send(sockfd, value.data(), value.size(), 0) != (ssize_t)value.size()) {
            throw std::runtime_error("Failed to send value for " + fieldName);
        }
    };

    // Protocol sequence
    Utils::send_str_wrapper(sockfd, "initiate-agent-plan");
    sendField("graphId", graphId);
    sendField("query", query);
    sendField("llmRunner", llmRunner);
    sendField("llmEngine", inferenceEngine);
    sendField("llmModel", llmModel);

    // Receive plan JSON
    char planBuf[64 * 1024];
    int bytes = recv(sockfd, planBuf, sizeof(planBuf), 0);
    if (bytes <= 0) {
        close(sockfd);
        throw std::runtime_error("No plan received from worker");
    }

    std::string planJson(planBuf, bytes);
    planJson = Utils::trim_copy(planJson);

    agent_executor_logger.info("[DESIGNATED WORKER → AGENT PLAN EXECUTOR] Raw message: '" + planJson + "'");

    Utils::send_str_wrapper(sockfd, "ok");
    close(sockfd);

    // std::string planStr = AgentProtocol::getPlan(agentRequestCtx);
    // agent_executor_logger.info("Executing Agent Plan" + planStr);

    // json jsonPlan = json::parse(planStr);
    // DecodedPlan decodedPlan = PlanDecoder::decode(jsonPlan);

    // // ---- Execute SBS objectives ----
    // if (decodedPlan.sbsPlan)
    // {
    //     for (const auto &obj : decodedPlan.sbsPlan->objectives)
    //     {
    //         agent_executor_logger.info(
    //             "Executing SBS objective: " + obj.id + " -> " + obj.query);
    //         // dispatch SEMANTIC_BEAM_SEARCH job here
    //     }
    // }
    // const auto& workerList = JasmineGraphServer::getWorkers(numberOfPartitions);
    // std::vector<std::unique_ptr<SharedBuffer>> bufferPool;
    // bufferPool.reserve(numberOfPartitions);
}

int AgentPlanExecutor::getUid() {
    static std::atomic<std::uint32_t> uid{0};
    return ++uid;
}
