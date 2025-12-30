#pragma once
#include <string>

struct AgentRequestContext {
    std::string query;
    std::string graphId;

    std::string llmRunner;
    std::string llmEngine;
    std::string llmModel;
};

class AgentProtocol {
    public:
    static std::string getPlan(const AgentRequestContext& agentRequestCtx);
    static std::string getResponse(const AgentRequestContext& ctx, const std::string& retrievedData);
};