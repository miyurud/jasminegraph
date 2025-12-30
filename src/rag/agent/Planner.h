#pragma once
#include <string>
#include <nlohmann/json.hpp>

class Planner {
public:
    Planner(const std::string& modelName, const std::string& host);
    ~Planner();

    nlohmann::json build(const std::string& query);

private:
    std::string model;
    std::string host;

    nlohmann::json buildSemanticBeamSearchPlan(const std::string& query);

};