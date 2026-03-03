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

#include "AgentProtocol.h"

#include <memory>

#include "../../util/Utils.h"
#include "../../util/logger/Logger.h"
#include "Agent.h"

Logger agent_protocol_logger;

std::string AgentProtocol::getPlan(const AgentRequestContext& agentRequestCtx) {
    static std::unique_ptr<Agent> agent = nullptr;
    if (!agent) {
        agent_protocol_logger.info("Initializing Agent | model=" + agentRequestCtx.llmModel +
                                   " | runner=" + agentRequestCtx.llmRunner + " | engine=" + agentRequestCtx.llmEngine);
        agent.reset(new Agent(agentRequestCtx.llmModel, agentRequestCtx.llmRunner, agentRequestCtx.llmEngine));
    }
    return agent->generatePlan(agentRequestCtx.query);
}

std::string AgentProtocol::getResponse(const AgentRequestContext& ctx, const std::string& retrievedData) {
    static std::unique_ptr<Agent> agent = nullptr;
    if (!agent) {
        agent_protocol_logger.info("Initializing Agent for response | model=" + ctx.llmModel +
                                   " | runner=" + ctx.llmRunner + " | engine=" + ctx.llmEngine);
        agent.reset(new Agent(ctx.llmModel, ctx.llmRunner, ctx.llmEngine));
    }
    return agent->generateResponse(ctx.query, retrievedData);
}
