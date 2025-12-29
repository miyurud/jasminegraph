#pragma once
#include <string>
#include <vector>
#include <optional>

enum class ExecutorType {
    SBS,
    CYPHER
};

struct SBSObjective {
    std::string id;
    std::string query;
    std::string searchType;
};

struct SBSPlan {
    std::string planType;
    std::vector<SBSObjective> objectives;
};

struct DecodedPlan {
    int planId;
    std::optional<SBSPlan> sbsPlan;
};

