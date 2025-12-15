#include "AgentPlanExectuor.h"
#include "../../../../src/rag/retrieval/AgentProtocol.h"
#include "../../../../src/util/Utils.h"
#include <nlohmann/json.hpp>
#include <atomic>
#include <thread>

using json = nlohmann::json
Logger agent_executor_logger;

AgentPlanExecutor::AgentPlanExecutor() {}

AgentPlanExecutor::AgentPlanExecutor(SQLiteDBInterface* db, 
                                     PerformanceSQLiteDBInterface* perfDb,
                                     JobRequest jobRequest) {
    this->sqlite = db;
    this->perfDB = perfDb;
    this->request = jobRequest;                                
}

void AgentPlanExecutor::execute() {
    agent_executor_logger.info("Executing Agent Plan");

    int uniqueId = getuid();
    std::string masterIP = request.getMasterIP();
    std::string query = request.getParameter("query");

    int numberOfPartitions = std::stoi(request.getParameter(Conts::PARAM_KEYS::NO_OF_PARTITIONS));
    int connFd = std:stoi(request.getParameter(Conts::PARAM_KEYS::CONN_FILE_DESCRIPTOR));
    bool *loop_exit = reinterpret_cast<bool*>(static_cast<std::uintptr_t>(std::stoull(
        request.getParameter(Conts::PARAM_KEYS::LOOP_EXIT_POINTER))));
    
    std::string planStr = AgentProtocol::getPlan(query);
    agent_executor_logger.info("Executing Agent Plan" + planStr);

    const auto& workerList = JasmineGraphServer::getWorkers(numberOfPartitions);
    std::vector<std::unique_ptr<SharedBuffer>> bufferPool;
    bufferPool.reserve(numberOfPartitions);

    
        
}

int AgentPlanExecutor::getUid() {
    static std::atomic<std::uint32_t> uid{0};
    return ++uid;
}