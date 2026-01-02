#include "Planner.h"

#include <curl/curl.h>

#include <chrono>
#include <nlohmann/json.hpp>
#include <thread>

#include "../../util/logger/Logger.h"
#include "../util/LLMUtils.h"

using json = nlohmann::json;

Logger planner_logger;

static const std::string SBS_PLANNER_PROMPT = R"(
You are a Semantic Beam Search Planner in an agentic retrieval system.

Your task is to analyze a user query and decide whether it should be handled as:
1) DIRECT - a single semantic beam search
2) DECOMPOSED - multiple semantic beam searches over smaller retrieval objectives

Important constraints:
- Semantic beam search is greedy and local.
- Decomposition should only be used when it improves recall or reasoning.
- Do NOT decompose simple, factual, or single-entity queries.
- Every plan MUST produce one or more objectives with the SAME STRUCTURE.
- Even DIRECT plans must produce exactly ONE objective.

Decompose the query ONLY if one or more of the following are true:
- The query implies causality or impact (e.g., effect, influence, outcome)
- The query involves multiple entities or domains
- The query requires comparison, aggregation, or reasoning across steps
- The query cannot be answered by retrieving a single coherent document

DO NOT:
- Generate Cypher queries
- Explain your reasoning
- Add extra fields
- Output markdown
- Output anything other than valid JSON

Output STRICT JSON ONLY in the following format:

{
  "plan_type": "DIRECT" | "DECOMPOSED",
  "objectives": [
    {
      "id": "obj1",
      "query": "A precise retrieval objective phrased as a standalone information need",
      "search_type": "SEMANTIC_BEAM_SEARCH"
    }
  ]
}

Guidelines for objectives:
- Each objective must be independently retrievable using semantic similarity
- Objectives should be minimal and non-overlapping
- Objectives should NOT assume answers from other objectives
- Use clear, concrete language
)";

Planner::Planner(const std::string& modelName, const std::string& host, const std::string& engine)
    : model(modelName), host(host), engine(engine) {
}

Planner::~Planner() {
}

static std::string stripMarkdown(const std::string& s) {
    std::string out = s;

    auto start = out.find("```");
    if (start != std::string::npos) {
        start = out.find("\n", start);
        if (start != std::string::npos) {
            out = out.substr(start + 1);
        }
    }

    auto end = out.rfind("```");
    if (end != std::string::npos) {
        out = out.substr(0, end);
    }

    return out;
}

json Planner::build(const std::string& query) {
    json semanticBeamSearchPlan = buildSemanticBeamSearchPlan(query);

    return json{
        {"sbs_plan", semanticBeamSearchPlan},
    };
}

json Planner::buildSemanticBeamSearchPlan(const std::string& query) {
    std::string prompt = SBS_PLANNER_PROMPT + std::string("\nUser Query:\n") + query;

    const int maxRetries = 3;
    const int baseDelaySeconds = 2;
    int attempt = 0;
    std::string llmResponse;

    while (attempt < maxRetries) {
        llmResponse = LLMUtils::callLLM(prompt, host, model, engine);
        if (!llmResponse.empty())
            break;

        planner_logger.info("Retrying LLM call in " + std::to_string(baseDelaySeconds * (attempt + 1)) + " seconds...");
        std::this_thread::sleep_for(std::chrono::seconds(baseDelaySeconds * (attempt + 1)));
        attempt++;
    }

    try {
        json cleanPlan = json::parse(llmResponse);
        planner_logger.info(
            "Generated Semantic Plan: " + cleanPlan.dump());
        return cleanPlan;
    } catch (...) {
        planner_logger.error("LLM output not valid JSON, returning fallback plan.");
        json fallback;
        fallback["objectives"] = json::array({{{"id", "obj1"},
                                               {"query", prompt},
                                               {"search_type", "SEMANTIC_BEAM_SEARCH"}}});
        fallback["plan_type"] = "DIRECT";

        return fallback;
    }
}