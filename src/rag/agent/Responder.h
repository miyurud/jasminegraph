#pragma once
#include <string>
#include <nlohmann/json.hpp>

class Responder {
public:
    Responder(const std::string& model, const std::string& host, const std::string& engine);
    ~Responder();
    nlohmann::json generateResponse(
        const std::string& query,
        const nlohmann::json& executionResult
    );

private:
    std::string model;
    std::string host;
    std::string engine;
};