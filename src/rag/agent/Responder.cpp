#include "Responder.h"

using json = nlohmann::json;

Responder::Responder(const std::string& model, const std::string& host) {
    // init LLM client
}

json Responder::generate(
    const std::string& query,
    const json& executionResult
) {
    std::string prompt =
        "User Query:\n" + query +
        "\n\nRetrieved Data:\n" + executionResult.dump(2) +
        "\n\nAnswer clearly for the user:";

    std::string llmOutput = callLLM(prompt);

    return {
        {"query", query},
        {"answer", llmOutput}
    };
}
