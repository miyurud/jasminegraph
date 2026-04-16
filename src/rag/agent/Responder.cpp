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

#include "Responder.h"

#include <chrono>
#include <thread>

#include "../../util/logger/Logger.h"
#include "../util/LLMUtils.h"

using json = nlohmann::json;

Logger responder_logger;

static const std::string RESPONDER_PROMPT = R"(
You are an intelligent question-answering system.

You will be given:
1. A **User Query**
2. **Retrieved Data** consisting of one or more traversal paths from a knowledge graph.

Each traversal path contains:
- `pathNodes`: an ordered list of nodes visited during traversal
- `pathRels`: an ordered list of relationships connecting the nodes
- `score`: a relevance score (higher means more relevant)

Your task:
- Use ONLY the information present in the Retrieved Data to answer the User Query.
- Interpret each traversal path as factual evidence.
- Combine information from multiple paths if needed.
- Prefer higher-scored paths when multiple paths provide overlapping information.
- Do NOT invent facts or use external knowledge.
- If the Retrieved Data does not contain enough information to answer the query, explicitly say so.

Guidelines:
- Reason over the nodes and relationships to infer the answer.
- Paraphrase graph facts into clear, natural language.
- Be concise, factual, and directly answer the query.
- Do not mention graph structure, node IDs, partition IDs, or scores in the final response.

Output:
Provide a clear and complete answer to the User Query based on the Retrieved Data.
)";

Responder::Responder(const std::string& model, const std::string& host, const std::string& engine)
    : model(model), host(host), engine(engine) {}

json Responder::generateResponse(const std::string& query, const json& executionResult) {
    std::string prompt = RESPONDER_PROMPT + std::string("\n\nUser Query:\n") + query +
                         std::string("\n\nRetrieved Data:\n") + executionResult.dump(2) +
                         std::string("\n\nFinal Answer:");

    responder_logger.debug("Responder Prompt: " + prompt);

    int attempt = 0;
    std::string llmResponse;

    while (attempt < Conts::LLM_MAX_TRY) {
        llmResponse = LLMUtils::callLLM(prompt, host, model, engine);
        if (!llmResponse.empty())
            break;

        responder_logger.info("Retrying LLM response generation in " +
                              std::to_string(Conts::LLM_RETRY_SLEEP_TIME_S * (++attempt)) + " seconds...");

        std::this_thread::sleep_for(std::chrono::seconds(Conts::LLM_RETRY_SLEEP_TIME_S * (attempt)));
    }

    if (llmResponse.empty()) {
        responder_logger.error("LLM failed to generate a response after " + std::to_string(Conts::LLM_MAX_TRY)
                                + " attempts");

        llmResponse = "I'm sorry, I couldn't generate a response at this time.";
    }

    return {{"query", query}, {"answer", llmResponse}};
}

