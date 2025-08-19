#include "OllamaTupleStreamer.h"
#include <curl/curl.h>
#include <nlohmann/json.hpp>
#include <iostream>

#include "../../util/logger/Logger.h"

using json = nlohmann::json;
Logger ollama_tuple_streamer_logger;

OllamaTupleStreamer::OllamaTupleStreamer(const std::string& modelName)
    : model(modelName) {}

size_t OllamaTupleStreamer::StreamCallback(char* ptr, size_t size, size_t nmemb, void* userdata) {
    size_t totalSize = size * nmemb;
    StreamContext* ctx = static_cast<StreamContext*>(userdata);

    std::string incoming(ptr, totalSize);
    // ollama_tuple_streamer_logger.info("Received stream chunk: " + incoming);

    size_t start = 0;
    while (true) {
        size_t pos = incoming.find("\n", start);
        if (pos == std::string::npos) {
            break;
        }

        std::string line = incoming.substr(start, pos - start); start = pos + 1;
        if (line.empty())
        {
            ollama_tuple_streamer_logger.info("Skipping empty line"); continue;
        }



        try {
            auto j = json::parse(line);

            // Completed tuple
            if (j.value("done", false)) {
                // std::string partial = j.value("response", "");
                // ollama_tuple_streamer_logger.info("recieved done partial: "+ ctx->current_tuple );

                    // std::string tupleStr = ctx->current_tuple;
                    ctx->buffer->add("-1");
                    ctx->current_tuple.clear();

                break;

            }
            // Partial tuple (may contain multiple newlines)
            else if (j.contains("response")) {
                std::string partial = j["response"];
                size_t s = 0;
                // ollama_tuple_streamer_logger.info("partial: "+ partial );

                // while (true) {
                    size_t e = partial.find("#", s);
                    if (e == std::string::npos)
                    {
                        ctx->current_tuple += partial; // append remaining
                        // ollama_tuple_streamer_logger.info("Current tuple: " + ctx-> current_tuple);
                        // ollama_tuple_streamer_logger.info("Appending remaining partial: " + partial);


                    } else
                        {
                        ctx->current_tuple += partial.substr(0, e); // append up to the #

                            // ctx->current_tuple += partial;
                            // ollama_tuple_streamer_logger.info("65: " + ctx->current_tuple);
                            std::string tupleStr = ctx->current_tuple;
                            ctx->buffer->add(tupleStr);
                            ctx->current_tuple.clear();
                            s = e + 1;
                        }


            }
        } catch (...) {
            ollama_tuple_streamer_logger.info("Malformed/partial JSON ignored: " + line);
        }
    }

    return totalSize;
}


void OllamaTupleStreamer::streamChunk(const std::string& chunkKey,
                                      const std::string& chunkText,
                                      SharedBuffer& tupleBuffer) {
    CURL* curl = curl_easy_init();
    if (!curl) {
        std::cerr << "Failed to initialize CURL\n";
        return;
    }

    StreamContext ctx{chunkKey, &tupleBuffer};

    // Use 127.0.0.1 explicitly to avoid IPv6 localhost issues
    curl_easy_setopt(curl, CURLOPT_URL, "http://192.168.1.7:11434/api/generate");
    curl_easy_setopt(curl, CURLOPT_POST, 1L);

    json j;
    j["model"] = model;
//     j["prompt"] = R"(ChatPromptTemplate.from_messages([
//     ("system",
//      "You are an expert information extractor specialized in knowledge graph construction. "
//      "Your task is to extract all possible subject-predicate-object triples from the given text and return them strictly as JSON objects seperated with a # symbol. "
//      "Each triple must be represented as a JSON object containing 'source', 'destination', and 'properties', following the schema provided. "
//      "Output each triple as a JSON object in the schema below and separate each JSON object with a # symbol. Do not include any explanation, prefix, suffix, or formatting outside the array."),
//     ("human",
//      """
// Extract all subject-predicate-object triples from the following text.
//
// Format:
// [
//   {{
//     "source": {{
//       "id": "<unique_node_id>",
//       "properties": {{
//         "id": "<unique_node_id>",
//         "label": "<EntityType>",
//         "name": "<EntityName>"
//       }}
//     }},
//     "destination": {{
//       "id": "<unique_node_id>",
//       "properties": {{
//         "id": "<unique_node_id>",
//         "label": "<EntityType>",
//         "name": "<EntityName>"
//       }}
//     }},
//     "properties": {{
//       "id": "<unique_relationship_id>",
//       "type": "<Predicate>",
//       "description": "<Human-readable description of the triple>"
//     }}
//   }}
// ]
//
// Instructions:
// - Output must be pure JSON objects seperated by # . Do not include any comments, headers, or text.
// - Use consistent and unique IDs across nodes and relationships. unique Ids should be the concatenation between the label and name with all small caps with a underscore
// - Populate all fields accurately, including labels and descriptions.
// - Extract as many meaningful triples as possible.
//
// Text:
// \"\"\" )" + chunkText + R"( \"\"\"
//
// JSON objects:
// """)
// ]) )";

  j["prompt"]  =
    "You are an expert information extractor specialized in knowledge graph construction.\n"
    "Extract all subject-predicate-object triples from the following text.\n"
    "Output each triple as a JSON object separated by #.\n"
    "Use this format:\n"
     "  {\n"
     "    \"source\": {\n"
     "      \"id\": \"<unique_node_id>\",\n"
     "      \"properties\": {\n"
     "        \"id\": \"<unique_node_id>\",\n"
     "        \"label\": \"<EntityType>\",\n"
     "        \"name\": \"<EntityName>\"\n"
     "      }\n"
     "    },\n"
     "    \"destination\": {\n"
     "      \"id\": \"<unique_node_id>\",\n"
     "      \"properties\": {\n"
     "        \"id\": \"<unique_node_id>\",\n"
     "        \"label\": \"<EntityType>\",\n"
     "        \"name\": \"<EntityName>\"\n"
     "      }\n"
     "    },\n"
     "    \"properties\": {\n"
     "      \"id\": \"<unique_relationship_id>\",\n"
     "      \"type\": \"<Predicate>\",\n"
     "      \"description\": \"<Human-readable description of the triple>\"\n"
     "    }\n"
     "  }\n"
    "Instructions:\n"
     "- Output must be pure JSON objects separated by #.\n"
     "- Use consistent and unique IDs (concatenate label + name in lowercase with underscores).\n"
     "- Populate all fields accurately, including labels and descriptions.\n"
     "- Extract as many meaningful triples as possible.\n\n"
    "Example:\n"
    "Text: 'Barack Obama was born in Honolulu. He served as the 44th President of the United States.'\n\n"
    "JSON objects:\n"
    "{\n"
    "  \"source\": {\n"
    "    \"id\": \"person_barack_obama\",\n"
    "    \"properties\": {\n"
    "      \"id\": \"person_barack_obama\",\n"
    "      \"label\": \"Person\",\n"
    "      \"name\": \"Barack Obama\"\n"
    "    }\n"
    "  },\n"
    "  \"destination\": {\n"
    "    \"id\": \"location_honolulu\",\n"
    "    \"properties\": {\n"
    "      \"id\": \"location_honolulu\",\n"
    "      \"label\": \"Location\",\n"
    "      \"name\": \"Honolulu\"\n"
    "    }\n"
    "  },\n"
    "  \"properties\": {\n"
    "    \"id\": \"relationship_barack_obama_born_in_honolulu\",\n"
    "    \"type\": \"born_in\",\n"
    "    \"description\": \"Barack Obama was born in Honolulu\"\n"
    "  }\n"
    "}\n"
    "#\n"
    "{\n"
    "  \"source\": {\n"
    "    \"id\": \"person_barack_obama\",\n"
    "    \"properties\": {\n"
    "      \"id\": \"person_barack_obama\",\n"
    "      \"label\": \"Person\",\n"
    "      \"name\": \"Barack Obama\"\n"
    "    }\n"
    "  },\n"
    "  \"destination\": {\n"
    "    \"id\": \"organization_united_states\",\n"
    "    \"properties\": {\n"
    "      \"id\": \"organization_united_states\",\n"
    "      \"label\": \"Organization\",\n"
    "      \"name\": \"United States\"\n"
    "    }\n"
    "  },\n"
    "  \"properties\": {\n"
    "    \"id\": \"relationship_barack_obama_president_of_united_states\",\n"
    "    \"type\": \"president_of\",\n"
    "    \"description\": \"Barack Obama served as the 44th President of the United States\"\n"
    "  }\n"
    "}\n\n"
    "Now process the following text:\n" + chunkText + ".\n\nJSON objects:\n";
    j["stream"] = true;
    std::string postFields = j.dump();

    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, postFields.c_str());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, postFields.size());

    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, StreamCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &ctx);

    // Optional: increase timeout in case the server is slow
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 0L); // 0 = no timeout
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L); // keep the connection alive

    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK) {
        std::cerr << "Curl error: " << curl_easy_strerror(res) << "\n";
    }

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
}
