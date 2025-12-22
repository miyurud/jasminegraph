#include <PlanDecoder.h>
#include "../../util/logger/Logger.h"

using json = nlohmann::json;

Logger plan_decoder_logger;

DecodedPlan PlanDecoder::decode(const json &jsonPlan)
{
    DecodedPlan decoded;

    if (!jsonPlan.contains("plan_id")) {
        throw std::runtime_error("Plan missing plan_id");
    }
    
    decoded.planId = jsonPlan["plan_id"].get<int>();

    if (jsonPlan.contains("sbs_plan"))
    {
        const json &sbs = jsonPlan["sbs_plan"];

        SBSPlan sbsPlan;
        sbsPlan.planType = sbs.value("plan_type", "DIRECT");

        if (!sbs.contains("objectives") || !sbs["objectives"].is_array())
        {
            plan_decoder_logger.error("Invalid sbs_plan.objectives");
            throw std::runtime_error("Invalid sbs_plan.objectives");
        }

        for (const auto &obj : sbs["objectives"])
        {
            SBSObjective planObj;
            planObj.id = obj.value("id", "");
            planObj.query = obj.value("query", "");
            planObj.searchType = obj.value("search_type", "SEMANTIC_BEAM_SEARCH");

            if (planObj.id.empty() || planObj.description.empty())
            {
                plan_decoder_logger.warn("Skipping invalid SBS objective");
                continue;
            }

            sbsPlan.objectives.push_back(planObj);
        }

        decoded.sbsPlan = sbsPlan;

        plan_decoder_logger.info(
            "Decoded plan_id=" + std::to_string(decoded.planId) +
            ", SBS=" + std::to_string(decoded.sbsPlan.has_value()));
    }

    return decoded;
}