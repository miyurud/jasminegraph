#include "Planner.h"
#include "../../util/logger/Logger.h"
#include <curl/curl.h>
#include <nlohmann/json.hpp>
#include <thread>
#include <chrono>

using json = nlohmann::json;

Logger planner_logger;

static const std::string SBS_PLANNER_PROMPT = "
You are a Semantic Beam Search Planner in an agentic retrieval system.

Your task is to analyze a user query and decide whether it should be handled as:
1) DIRECT — a single semantic beam search
2) DECOMPOSED — multiple semantic beam searches over smaller retrieval objectives

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
      "description": "A precise retrieval objective phrased as a standalone information need",
      "search_type": "SEMANTIC_BEAM"
    }
  ]
}

Guidelines for objectives:
- Each objective must be independently retrievable using semantic similarity
- Objectives should be minimal and non-overlapping
- Objectives should NOT assume answers from other objectives
- Use clear, concrete language
";

Planner::Planner(const std::string& modelName, const std::string& host)
    : model(modelName), host(host) {
    curl_global_init(CURL_GLOBAL_DEFAULT);
}

Planner::~Planner() {
    curl_global_cleanup();
}

// helper to capture CURL response
static size_t CurlWriteCallback(void* contents, size_t size, size_t nmemb, void* userp) {
    ((std::string*)userp)->append((char*)contents, size * nmemb);
    return size * nmemb;
}

json Planner::build(const std::string& query) {

    json semanticBeamSearchPlan = buildSemanticBeamSearchPlan(query);

    return json{
        {"sbs_plan", semanticBeamSearchPlan},
    };
}

std::string Planner::callLLM(const std::string& prompt) {
    std::string result;
    CURL* curl = curl_easy_init();
    if (!curl) {
        planner_logger.error("Failed to initialize CURL");
        return "";
    }

    curl_easy_setopt(curl, CURLOPT_URL, (host + "/api/generate").c_str());
    curl_easy_setopt(curl, CURLOPT_POST, 1L);

    json requestJson;
    requestJson["model"] = model;
    requestJson["prompt"] = prompt;
    requestJson["max_tokens"] = 8000;
    requestJson["stream"] = false;

    std::string postFields = requestJson.dump();

    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, postFields.c_str());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, postFields.size());

    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, CurlWriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &result);

    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK) {
        planner_logger.error("CURL error: " + std::string(curl_easy_strerror(res)));
    }

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    return result;
}

json Planner::buildSemanticBeamSearchPlan(const std::string& query) {

    std::string prompt = SBS_PLANNER_PROMPT + std::string("\nUser Query:\n") + query;

    const int maxRetries = 3;
    const int baseDelaySeconds = 2;
    int attempt = 0;
    std::string llmResponse;

    while (attempt < maxRetries) {
        llmResponse = callLLM(prompt);
        planner_logger.info("LLM response on attempt " + std::to_string(attempt + 1) + ": " + llmResponse);
        if (!llmResponse.empty()) break;

        planner_logger.info("Retrying LLM call in " + std::to_string(baseDelaySeconds * (attempt + 1)) + " seconds...");
        std::this_thread::sleep_for(std::chrono::seconds(baseDelaySeconds * (attempt + 1)));
        attempt++;
    }

    try {
        return json::parse(llmResponse);
    } catch (...) {
        planner_logger.error("LLM output not valid JSON, returning fallback plan.");
        // minimal fallback plan
        json fallback;
        fallback["model"] = "DEFAULT";
        fallback["steps"] = json::array({
            {{"step_id", "s1"}, {"op", "SCAN"}, {"params", {{"root", ""}}}},
            {{"step_id", "s2"}, {"op", "TRAVERSE"}, {"params", {{"depth", 1}}}}
        });
        fallback["merge_strategy"] = "concat";
        return fallback;
    }

    return json{
        {
            "query_plan",
            {
                {"plan_type", "DIRECT"},
                {"objectives", json::array({
                    {
                        {"id", "obj1"},
                        {"description", query},
                        {"search_type", "SEMANTIC_BEAM"}
                    }
                })}
            }
        }
    };
}