#include "AgentPlanExecutor.h"
#include "../../../../../src/rag/agent/AgentProtocol.h"
#include "../../../../server/JasmineGraphServer.h"
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

    agent_executor_logger.info("For agent plan received: " + graphId + llmModel + inferenceEngine + llmRunner + query);

    std::string planStr = AgentProtocol::getPlan(agentRequestCtx);
    agent_executor_logger.info("Executing Agent Plan" + planStr);

    // const auto& workerList = JasmineGraphServer::getWorkers(numberOfPartitions);
    // std::vector<std::unique_ptr<SharedBuffer>> bufferPool;
    // bufferPool.reserve(numberOfPartitions);
}

int AgentPlanExecutor::getUid()
{
    static std::atomic<std::uint32_t> uid{0};
    return ++uid;
}