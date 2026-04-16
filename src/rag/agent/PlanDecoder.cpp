/**
Copyright 2026 JasmineGraph Team
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

#include "PlanDecoder.h"

#include "../../util/logger/Logger.h"

using json = nlohmann::json;

Logger plan_decoder_logger;

DecodedPlan PlanDecoder::decode(const json& jsonPlan) {
    DecodedPlan decoded;

    if (!jsonPlan.contains("plan_id")) {
        throw std::runtime_error("Plan missing plan_id");
    }

    decoded.planId = jsonPlan["plan_id"].get<int>();

    if (jsonPlan.contains("sbs_plan")) {
        const json& sbs = jsonPlan["sbs_plan"];

        SBSPlan sbsPlan;
        sbsPlan.planType = sbs.value("plan_type", "DIRECT");

        if (!sbs.contains("objectives") || !sbs["objectives"].is_array()) {
            plan_decoder_logger.error("Invalid sbs_plan.objectives");
            throw std::runtime_error("Invalid sbs_plan.objectives");
        }

        for (const auto& obj : sbs["objectives"]) {
            SBSObjective planObj;
            planObj.id = obj.value("id", "");
            planObj.query = obj.value("query", "");
            planObj.searchType = obj.value("search_type", "SEMANTIC_BEAM_SEARCH");

            if (planObj.id.empty() || planObj.query.empty()) {
                plan_decoder_logger.warn("Skipping invalid SBS objective");
                continue;
            }

            sbsPlan.objectives.push_back(planObj);
        }

        decoded.sbsPlan = sbsPlan;
    }

    return decoded;
}

