#pragma once
#include <string>
#include <nlohmann/json.hpp>

class Planner {
public:
    Planner(const std::string& modelName, const std::string& host, const std::string& engine);
    ~Planner();

    nlohmann::json build(const std::string& query);

private:
    std::string model;
    std::string host;
    std::string engine;

    nlohmann::json buildSemanticBeamSearchPlan(const std::string& query);

};