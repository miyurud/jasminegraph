#pragma once
#include <string>

class AgentProtocol {
    public:
    static std::string getPlan(const std::string& query);
};