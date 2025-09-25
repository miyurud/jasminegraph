#include "VLLMTupleStreamer.h"
#include <curl/curl.h>
#include <nlohmann/json.hpp>
#include <iostream>
#include <thread>
#include <chrono>
#include <stack>

#include "../../util/logger/Logger.h"

using json = nlohmann::json;
Logger vllm_tuple_streamer_logger;

// ---------------- Constructor ----------------
VLLMTupleStreamer::VLLMTupleStreamer(const std::string& modelName, const std::string& host)
    : model(modelName), host(host) {
    curl_global_init(CURL_GLOBAL_DEFAULT);
    vllm_tuple_streamer_logger.info("Initialized VLLMTupleStreamer with model: " + modelName + ", host: " + host);
}

// ---------------- Stream Callback ----------------
size_t VLLMTupleStreamer::StreamCallback(char* ptr, size_t size, size_t nmemb, void* userdata) {
    size_t totalSize = size * nmemb;
    StreamContext* ctx = static_cast<StreamContext*>(userdata);

    std::string incoming(ptr, totalSize);
    size_t start = 0;

    while (start < incoming.size()) {
        size_t pos = incoming.find("\n", start);
        if (pos == std::string::npos) pos = incoming.size(); // last line
        std::string line = incoming.substr(start, pos - start);
        start = pos + 1;
        // vllm_tuple_streamer_logger.info(line);

        if (line.empty()) continue;

        // Handle end of stream
        if (line == "data: [DONE]") {
            if (!ctx->current_tuple.empty()) {
                ctx->buffer->add(ctx->current_tuple); // flush remaining partial
                ctx->current_tuple.clear();
            }
            ctx->buffer->add("-1"); // signal end
            break;
        }

        // Only parse lines starting with "data: "
        const std::string prefix = "data: ";
        if (line.rfind(prefix, 0) != 0) continue;

        std::string jsonPart = line.substr(prefix.size());
        try {
            auto j = json::parse(jsonPart);

            if (j.contains("choices") && !j["choices"].empty()) {
                // std::string text = j["choices"][0].value("text", "");
                 std::string partial = j["choices"][0].value("text", "");
                size_t s = 0;
                // vllm_tuple_streamer_logger.info("partial: "+ partial );

                // while (true) {
                    // size_t e = partial.find("#", s);
                    // if (e == std::string::npos)
                    // {
                    //     ctx->current_tuple += partial; // append remaining
                    //     vllm_tuple_streamer_logger.info("Current tuple: " + ctx-> current_tuple);
                    //     vllm_tuple_streamer_logger.info("Appending remaining partial: " + partial);
                    //
                    //
                    // } else
                    //     {
                    //     ctx->current_tuple += partial.substr(0, e); // append up to the #
                    //
                    //         // ctx->current_tuple += partial;
                    //         vllm_tuple_streamer_logger.info("65: " + ctx->current_tuple);
                    //     std::string tupleStr;
                    //     try
                    //     {
                    //         // validate
                    //         json::parse(ctx->current_tuple);
                    //         tupleStr = ctx->current_tuple;
                    //         ctx->buffer->add(tupleStr);
                    //         ctx->current_tuple.clear();
                    //         s = e + 1;
                    //     }
                    //     catch (const std::exception& exception )
                    //     {
                    //         vllm_tuple_streamer_logger.error( exception.what() );
                    //         vllm_tuple_streamer_logger.info("86 Malformed/partial JSON ignored: " + ctx->current_tuple);
                    //
                    //         ctx->current_tuple.clear();
                    //         s = e + 1;
                    //
                    //     }
                    //
                    //
                    //     }


                // for (char c : partial) {
                //     vllm_tuple_streamer_logger.info("Partial: "+ partial+ " C: "+c);
                //     if (c == '{') {
                //         vllm_tuple_streamer_logger.info("pushing");
                //         ctx->braceStack.push(c);
                //         ctx->current_tuple.push_back(c);
                //     } else if (c == '}') {
                //         vllm_tuple_streamer_logger.info("popping");
                //         ctx->current_tuple.push_back(c);
                //         if (! ctx->braceStack.empty())  ctx->braceStack.pop();
                //
                //         if ( ctx->braceStack.empty()) {
                //             try {
                //                 ctx->buffer->add(ctx->current_tuple);
                //                 vllm_tuple_streamer_logger.info("✅ Adding complete tuple: " + ctx->current_tuple);
                //
                //             } catch (const std::exception& ex) {
                //                 vllm_tuple_streamer_logger.error("❌ JSON parse failed: " + std::string(ex.what()));
                //             }
                //             ctx->current_tuple.clear();
                //         }
                //     } else if (! ctx->braceStack.empty()) {
                //         vllm_tuple_streamer_logger.info("adding ");
                //         ctx->current_tuple.push_back(c);
                //     }
                // }

                size_t i = 0;
                // int braceDepth = 0;

                while (i < partial.size()) {
                    size_t bpos = partial.find_first_of("{}", i);
                    if (bpos == std::string::npos) {
                        if (ctx->braceDepth > 0) {
                            ctx->current_tuple.append(partial, i, std::string::npos);
                        }
                        break;
                    }

                    if (ctx->braceDepth > 0) {
                        ctx->current_tuple.append(partial, i, bpos - i);
                    }

                    char c = partial[bpos];
                    if (c == '{') {
                        ctx->braceDepth++;
                        ctx->current_tuple.push_back(c);
                    } else { // '}'
                        ctx->braceDepth--;
                        ctx->current_tuple.push_back(c);

                        if (ctx->braceDepth == 0) {
                            try {
                                ctx->buffer->add(ctx->current_tuple);
                                vllm_tuple_streamer_logger.info("✅ Adding complete tuple: " + ctx->current_tuple);
                            } catch (const std::exception& ex) {
                                vllm_tuple_streamer_logger.error("❌ JSON parse failed: " + std::string(ex.what()));
                            }
                            ctx->current_tuple.clear();
                        }
                    }
                    i = bpos + 1;
                }
                // --- End optimized brace scanning ---
            }


        } catch (const std::exception& ex) {
            vllm_tuple_streamer_logger.error("JSON parse error: " + std::string(ex.what()));
        }
    }

    return totalSize;
}

// ---------------- Stream Chunk ----------------
void VLLMTupleStreamer::streamChunk(const std::string& chunkKey,
                                    const std::string& chunkText,
                                    SharedBuffer& tupleBuffer) {
    const int maxRetries = 10;
    const int baseDelaySeconds = 50;
    int attempt = 0;
    CURLcode res;

    StreamContext ctx{chunkKey, &tupleBuffer, "", true };

    do {
        CURL* curl = curl_easy_init();
        if (!curl) {
            std::cerr << "Failed to initialize CURL\n";
            return;
        }

        std::string url = host + "/v1/completions";
        // std::string url = host + "/v1/chat/completions";

        vllm_tuple_streamer_logger.info("Connecting to VLLM server at: " + host);

        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_POST, 1L);

        // Build JSON request
        json j;
        j["model"] = model;

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
    "Text: 'Barack Obama was born in Honolulu on August 4, 1961. He served as the 44th President of the United States.'\n\n"
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
    "    \"description\": \"Barack Obama was born in Honolulu on August 4, 1961\"\n"
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
        j["max_tokens"] = 3000;

        std::string postFields = j.dump();
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, postFields.c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, postFields.size());

        struct curl_slist* headers = nullptr;
        headers = curl_slist_append(headers, "Content-Type: application/json");
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, StreamCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &ctx);

        curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 50L);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, 600L);
        curl_easy_setopt(curl, CURLOPT_LOW_SPEED_LIMIT, 1L);
        curl_easy_setopt(curl, CURLOPT_LOW_SPEED_TIME, 120L);
        curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
        curl_easy_setopt(curl, CURLOPT_TCP_KEEPIDLE, 60L);
        curl_easy_setopt(curl, CURLOPT_TCP_KEEPINTVL, 30L);

        res = curl_easy_perform(curl);

        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);

        if (res != CURLE_OK && attempt < maxRetries - 1) {
            int waitTime = baseDelaySeconds * attempt;
            std::cerr << "Retrying in " << waitTime << " seconds...\n";
            std::this_thread::sleep_for(std::chrono::seconds(waitTime));
        }

        attempt++;
    } while ((res != CURLE_OK && attempt < maxRetries) || !ctx.isSuccess);

    if (res != CURLE_OK) {
        std::cerr << "Failed after " << maxRetries << " attempts.\n";
        ctx.buffer->add("-1");
    }
}

// ---------------- Non-Streaming Chunk ----------------
void VLLMTupleStreamer::processChunk(
    const std::string& chunkKey,
    const std::string& chunkText,
    SharedBuffer& tupleBuffer) {

    const int maxRetries = 5;
    const int baseDelaySeconds = 5;
    int attempt = 0;
    CURLcode res;

    do {
        CURL* curl = curl_easy_init();
        if (!curl) {
            std::cerr << "Failed to initialize CURL\n";
            return;
        }

        std::string url = host + "/v1/completions";
        vllm_tuple_streamer_logger.info("Connecting (non-stream) to VLLM server at: " + host);

        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_POST, 1L);

        // Build JSON request (same prompt as your streaming version)
        json j;
        j["model"] = model;
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
    "Text: 'Barack Obama was born in Honolulu on August 4, 1961. He served as the 44th President of the United States.'\n\n"
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
    "    \"description\": \"Barack Obama was born in Honolulu on August 4, 1961\"\n"
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
        j["stream"] = false; // non-streaming
        j["max_tokens"] = 3000;

        std::string postFields = j.dump();
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, postFields.c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, postFields.size());

        struct curl_slist* headers = nullptr;
        headers = curl_slist_append(headers, "Content-Type: application/json");
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

        // Capture response into std::string
        std::string responseBuffer;
        auto WriteCallback = [](char* ptr, size_t size, size_t nmemb, void* userdata) -> size_t {
            std::string* resp = static_cast<std::string*>(userdata);
            resp->append(ptr, size * nmemb);
            return size * nmemb;
        };

        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &responseBuffer);

        // curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 50L);
        // curl_easy_setopt(curl, CURLOPT_TIMEOUT, 600L);
        // curl_easy_setopt(curl, CURLOPT_LOW_SPEED_LIMIT, 1L);
        // curl_easy_setopt(curl, CURLOPT_LOW_SPEED_TIME, 120L);
        // curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
        // curl_easy_setopt(curl, CURLOPT_TCP_KEEPIDLE, 60L);
        // curl_easy_setopt(curl, CURLOPT_TCP_KEEPINTVL, 30L);

        res = curl_easy_perform(curl);

        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);

        if (res == CURLE_OK) {
            try {
                json respJson = json::parse(responseBuffer);

                vllm_tuple_streamer_logger.info("Raw response: " + respJson.dump(2));
                if (respJson.contains("choices") && !respJson["choices"].empty()) {
                    std::string text = respJson["choices"][0].value("text", "");
                    vllm_tuple_streamer_logger.info("Received non-streamed text: " + text);

                    // Split tuples by '#'
                    size_t start = 0;
                    while (true) {
                        size_t pos = text.find('#', start);
                        std::string tupleStr = (pos == std::string::npos)
                            ? text.substr(start)
                            : text.substr(start, pos - start);

                        if (!tupleStr.empty()) {
                            try {
                                json::parse(tupleStr); // validate JSON
                                tupleBuffer.add(tupleStr);
                                vllm_tuple_streamer_logger.info("✅ Added tuple: " + tupleStr);
                            } catch (const std::exception& ex) {
                                vllm_tuple_streamer_logger.error("❌ Malformed JSON tuple: " + std::string(ex.what()));
                            }
                        }

                        if (pos == std::string::npos) break;
                        start = pos + 1;
                    }

                    // signal completion
                    tupleBuffer.add("-1");
                    return;
                }
            } catch (const std::exception& ex) {
                vllm_tuple_streamer_logger.error("JSON parse error: " + std::string(ex.what()));
            }
        } else {
            if (attempt < maxRetries - 1) {
                int waitTime = baseDelaySeconds * attempt;
                std::cerr << "Retrying in " << waitTime << " seconds...\n";
                std::this_thread::sleep_for(std::chrono::seconds(waitTime));
            }
        }

        attempt++;
    } while (attempt < maxRetries);

    std::cerr << "Failed after " << maxRetries << " attempts.\n";
    tupleBuffer.add("-1");
}

