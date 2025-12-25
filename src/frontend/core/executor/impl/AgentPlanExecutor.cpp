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
                                     JobRequest jobRequest)
{
    this->sqlite = db;
    this->perfDB = perfDb;
    this->request = jobRequest;
}

void AgentPlanExecutor::execute()
{
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

    JasmineGraphServer::worker designatedWorker = JasmineGraphServer::getDesignatedWorker();

    // Open Socket
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0)
    {
        throw std::runtime_error("Failed to create socket");
    }

    if (designatedWorker.hostname.find('@') != std::string::npos)
    {
        worker.hostname = Utils::split(worker.hostname, '@')[1];
    }

    struct hostent *server = gethostbyname(worker.hostname.c_str());
    if (!server)
    {
        close(sockfd);
        throw std::runtime_error("Unknown host: " + worker.hostname);
    }

    struct sockaddr_in serv_addr{};
    serv_addr.sin_family = AF_INET;
    bcopy(
        (char *)server->h_addr,
        (char *)&serv_addr.sin_addr.s_addr,
        server->h_length);
    serv_addr.sin_port = htons(worker.port);

    if (Utils::connect_wrapper(
            sockfd,
            (struct sockaddr *)&serv_addr,
            sizeof(serv_addr)) < 0)
    {
        close(sockfd);
        throw std::runtime_error("Connection to worker failed");
    }

    // Handshake
    char buffer[FED_DATA_LENGTH + 1];
    if (!Utils::performHandshake(sockfd, buffer, FED_DATA_LENGTH, masterIP))
    {
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        throw std::runtime_error("Handshake failed");
    }

    auto sendAndExpectOk = [&](const std::string &msg)
    {
        if (!Utils::send_str_wrapper(sockfd, msg))
        {
            throw std::runtime_error("Send failed: " + msg);
        }
        char resp[128]{0};
        int n = recv(sockfd, resp, sizeof(resp), 0);
        if (n <= 0 || Utils::trim_copy(resp) != "ok")
        {
            throw std::runtime_error("Unexpected response for: " + msg);
        }
    };

    // Protocol sequence
    sendAndExpectOk("initiate-agent-plan");
    sendAndExpectOk(query);
    sendAndExpectOk(graphId);
    sendAndExpectOk(inferenceEngine);
    sendAndExpectOk(llmModel);

    // Receive plan JSON
    char planBuf[64 * 1024];
    int bytes = recv(sockfd, planBuf, sizeof(planBuf), 0);
    if (bytes <= 0)
    {
        close(sockfd);
        throw std::runtime_error("No plan received from worker");
    }

    std::string planJson(planBuf, bytes);
    planJson = Utils::trim_copy(planJson);

    Utils::send_str_wrapper(sockfd, "ok");
    close(sockfd);

    agent_executor_logger.info("Received AgentExecutionPlan");

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

int AgentPlanExecutor::getUid()
{
    static std::atomic<std::uint32_t> uid{0};
    return ++uid;
}