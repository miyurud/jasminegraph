#include "AgentProtocol.h"
#include "Agent.h"
#include "../../util/logger/Logger.h"
#include "../../util/Utils.h"

#include <memory>

Logger agent_protocol_logger;

std::string AgentProtocol::getPlan(const AgentRequestContext& agentRequestCtx) {
    
    static std::unique_ptr<Agent> agent=nullptr;
    if (!agent){
        agent_protocol_logger.info(
            "Initializing Agent | model=" + agentRequestCtx.llmModel +
            " | runner=" + agentRequestCtx.llmRunner +
            " | engine=" + agentRequestCtx.llmEngine
        );
        //agent.reset(new Agent(model, host));
        agent.reset(new Agent(
            agentRequestCtx.llmModel,
            agentRequestCtx.llmRunner   // host / backend selector
        ));
    }
    agent_protocol_logger.debug(
        "Generating plan for graph=" + agentRequestCtx.graphId +
        " | query=" + agentRequestCtx.query
    );

    return agent->generatePlan(agentRequestCtx.query);
}