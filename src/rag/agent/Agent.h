#pragma once
#include <nlohmann/json.hpp>
#include <string>

class Agent {
    public:
        Agent(const std::string& modelName, const std::string& host, const std::string& engine);
        ~Agent();

        static int getUid();
        std::string generatePlan(const std::string& query);
        std::string generateResponse(const std::string& query, const std::string& retrievedData);

    private:
        struct Impl;
        Impl* p;
};