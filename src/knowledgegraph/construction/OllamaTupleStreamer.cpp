#include "VLLMTupleStreamer.h"
#include <curl/curl.h>
#include <nlohmann/json.hpp>
#include <iostream>
#include <thread>
#include <chrono>
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
                std::string text = j["choices"][0].value("text", "");
                ctx->current_tuple += text;  // accumulate partial text

                // Split tuples by #
                size_t s = 0;
                while (true) {
                    size_t e = ctx->current_tuple.find("#", s);
                    if (e == std::string::npos) break;

                    std::string tupleStr = ctx->current_tuple.substr(0, e);
                    ctx->buffer->add(tupleStr); // send tuple to buffer
                    ctx->current_tuple.erase(0, e + 1);
                }
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
    StreamContext ctx{chunkKey, &tupleBuffer, "", true};

    do {
        CURL* curl = curl_easy_init();
        if (!curl) {
            std::cerr << "Failed to initialize CURL\n";
            return;
        }

        std::string url = host + "/v1/completions";
        vllm_tuple_streamer_logger.info("Connecting to VLLM server at: " + host);

        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_POST, 1L);

        // Build JSON request
        json j;
        j["model"] = model;
        j["prompt"] = chunkText;
        j["stream"] = true;

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
