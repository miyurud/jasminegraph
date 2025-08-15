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
                ollama_tuple_streamer_logger.info("recieved done partial: "+ ctx->current_tuple );
                    std::string tupleStr = "(" + ctx->chunkKey + ", " + ctx->current_tuple + ")";
                    ctx->buffer->add(tupleStr);
                    ctx->current_tuple.clear();

            }
            // Partial tuple (may contain multiple newlines)
            else if (j.contains("response")) {
                std::string partial = j["response"];
                size_t s = 0;
                ollama_tuple_streamer_logger.info("partial: "+ partial );

                // while (true) {
                    size_t e = partial.find("#", s);
                    if (e == std::string::npos)
                    {
                        ctx->current_tuple += partial; // append remaining
                        ollama_tuple_streamer_logger.info("Current tuple: " + ctx-> current_tuple);
                        ollama_tuple_streamer_logger.info("Appending remaining partial: " + partial);


                    } else
                        {
                            ctx->current_tuple += partial;
                            ollama_tuple_streamer_logger.info("65: " + ctx->current_tuple);
                            std::string tupleStr = "(" + ctx->chunkKey + ", " + ctx->current_tuple + ")";
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
    curl_easy_setopt(curl, CURLOPT_URL, "http://10.10.8.89:11434/api/generate");
    curl_easy_setopt(curl, CURLOPT_POST, 1L);

    json j;
    j["model"] = model;
    j["prompt"] = "Convert the following text into tuples seperated by # :\n" + chunkText;
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
