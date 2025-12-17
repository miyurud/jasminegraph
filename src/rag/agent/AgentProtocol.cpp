#include "AgentProtocol.h"
#include "Agent.h"
#include "../../util/logger/Logger.h"
#include "../../util/Utils.h"

#include <memory>

Logger agent_protocol_logger;

std::string AgentProtocol::getPlan(const std::string& query) {
    
    static std::unique_ptr<Agent> agent=nullptr;
    if (!agent){
        std::string model = "";
        std::string host = "";
        agent_protocol_logger.info("Initializing Agent with model=" + model + ", host=" + host);
        agent.reset(new Agent(model, host));
    }
    agent_protocol_logger.debug("Generating plan for query: " + query);

    return agent->generatePlan(query);
}