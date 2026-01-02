#include "AgentProtocol.h"
#include "Agent.h"
#include "../../util/logger/Logger.h"
#include "../../util/Utils.h"

#include <memory>

Logger agent_protocol_logger;

std::string AgentProtocol::getPlan(const AgentRequestContext &agentRequestCtx)
{
    static std::unique_ptr<Agent> agent = nullptr;
    if (!agent)
    {
        agent_protocol_logger.info(
            "Initializing Agent | model=" + agentRequestCtx.llmModel +
            " | runner=" + agentRequestCtx.llmRunner +
            " | engine=" + agentRequestCtx.llmEngine);
        agent.reset(new Agent(
            agentRequestCtx.llmModel,
            agentRequestCtx.llmRunner,
            agentRequestCtx.llmEngine));
    }
    return agent->generatePlan(agentRequestCtx.query);
}

std::string AgentProtocol::getResponse(const AgentRequestContext &ctx, const std::string &retrievedData)
{
    static std::unique_ptr<Agent> agent = nullptr;
    if (!agent)
    {
        agent_protocol_logger.info(
            "Initializing Agent for response | model=" + ctx.llmModel +
            " | runner=" + ctx.llmRunner +
            " | engine=" + ctx.llmEngine);
        agent.reset(new Agent(
            ctx.llmModel,
            ctx.llmRunner,
            ctx.llmEngine));
    }
    return agent->generateResponse(ctx.query, retrievedData);
}