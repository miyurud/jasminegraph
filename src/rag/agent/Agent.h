#pragma once
#include <string>
#include <nlohmann/json.hpp>

class Agent {
public:
    Agent(const std::string& modelName, const std::string& host);
    ~Agent();

    static int getUid();
    std::string generatePlan(const std::string &query);

private:
    struct Impl;
    Impl* p;
};