#include "AgentProtocol.h"
#include "Agent.h"
#include "../../util/logger/Logger.h"
#include "../../util/Utils.h"

#include <memory>

Logger agent_protocol_logger;

std::string AgentProtocol::getPlan(const AgentRequestContext& agentRequesCtx) {
    
    static std::unique_ptr<Agent> agent=nullptr;
    if (!agent){
        agent_protocol_logger.info(
            "Initializing Agent | model=" + agentRequestCtx.llm_model +
            " | runner=" + agentRequestCtx.llm_runner +
            " | engine=" + agentRequestCtx.llm_engine
        );
        //agent.reset(new Agent(model, host));
        agent.reset(new Agent(
            agentRequestCtx.llm_model,
            agentRequestCtx.llm_runner   // host / backend selector
        ));
    }
    agent_protocol_logger.debug(
        "Generating plan for graph=" + agentRequestCtx.graph_id +
        " | query=" + agentRequestCtx.query
    );

    return agent->generatePlan(agentRequestCtx.query);
}