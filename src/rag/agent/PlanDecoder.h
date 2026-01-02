#pragma once
#include <nlohmann/json.hpp>

#include "PlanTypes.h"

class PlanDecoder {
 public:
    static DecodedPlan decode(const nlohmann::json& jsonPlan);
};
