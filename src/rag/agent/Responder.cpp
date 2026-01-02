#include "Responder.h"
#include "../../util/logger/Logger.h"
#include "../util/LLMUtils.h"
#include <thread>
#include <chrono>

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

Responder::Responder(const std::string &model, const std::string &host)
    : model(model), host(host)
{
}

json Responder::generateResponse(
    const std::string &query,
    const json &executionResult)
{
    std::string prompt =
        RESPONDER_PROMPT +
        std::string("\n\nUser Query:\n") + query +
        std::string("\n\nRetrieved Data:\n") + executionResult.dump(2) +
        std::string("\n\nFinal Answer:");

    responder_logger.info("Responder Prompt: " + prompt);
    const int maxRetries = 3;
    const int baseDelaySeconds = 2;
    int attempt = 0;
    std::string llmResponse;

    while (attempt < maxRetries)
    {
        llmResponse = LLMUtils::callLLM(prompt, host, model);
        if (!llmResponse.empty())
            break;

        responder_logger.info(
            "Retrying LLM response generation in " +
            std::to_string(baseDelaySeconds * (attempt + 1)) +
            " seconds...");

        std::this_thread::sleep_for(
            std::chrono::seconds(baseDelaySeconds * (attempt + 1)));
        attempt++;
    }

    try
    {
        json raw = json::parse(llmResponse);

        if (!raw.contains("response"))
        {
            responder_logger.error("LLM response missing 'response'");
            return {
                {"query", query},
                {"answer", "Sorry, I couldn't generate a response at this time."}};
        }

        std::string answer = raw["response"].get<std::string>();

        responder_logger.info("Generated user response successfully: " + answer);

        return {
            {"query", query},
            {"answer", answer}};
    }
    catch (...)
    {
        responder_logger.error("Responder LLM output not valid JSON");

        return {
            {"query", query},
            {"answer", "Sorry, something went wrong while generating the answer."}};
    }
}
