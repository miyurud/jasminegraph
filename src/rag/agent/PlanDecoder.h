#pragma once
#include "PlanTypes.h"
#include <nlohmann/json.hpp>

class PlanDecoder {
public:
    static DecodedPlan decode(const nlohmann::json& jsonPlan);
};
