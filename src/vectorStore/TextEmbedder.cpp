//
// Created by sajeenthiran on 2025-08-16.
//

#include "TextEmbedder.h"
#include "TextEmbedder.h"
#include <curl/curl.h>
#include <iostream>
#include <nlohmann/json.hpp>
#include <thread>
#include <chrono>
using json = nlohmann::json;
// ---------------- HTTP Client ----------------
static size_t WriteToString(void* contents, size_t size, size_t nmemb, void* userp) {
    size_t total = size * nmemb;
    std::string* s = static_cast<std::string*>(userp);
    s->append(static_cast<char*>(contents), total);
    return total;
}

HttpClient::HttpClient() {
    curl_global_init(CURL_GLOBAL_ALL);
}

HttpClient::~HttpClient() {
    curl_global_cleanup();
}

std::string HttpClient::post(const std::string& url,
                             const std::string& body,
                             const std::vector<std::string>& headers) {
    std::string response;

    CURL* curl = curl_easy_init();
    if (!curl) throw std::runtime_error("curl_easy_init failed");

    curl_easy_setopt(curl, CURLOPT_SSLVERSION, CURL_SSLVERSION_TLSv1_2);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);


    struct curl_slist* header_list = nullptr;
    for (const auto& h : headers) {
        header_list = curl_slist_append(header_list, h.c_str());
    }

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, header_list);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long)body.size());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteToString);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);


    // Hardcoded timeouts
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 5L);   // 5s to connect
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 300L);        // 5 minutes max per request

    CURLcode res = curl_easy_perform(curl);
    long code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);

    curl_slist_free_all(header_list);
    curl_easy_cleanup(curl);

    if (res != CURLE_OK) {
        throw std::runtime_error(std::string("curl error: ") + curl_easy_strerror(res));
    }
    if (code < 200 || code >= 300) {
        throw std::runtime_error("HTTP " + std::to_string(code) + " body: " + response);
    }
    return response;
}

// ---------------- TextEmbedder ----------------
TextEmbedder::TextEmbedder(const std::string& endpoint, const std::string& model_name)
    : endpoint(endpoint), model_name(model_name) {}


std::vector<std::vector<float>> TextEmbedder::batch_embed(const std::vector<std::string>& texts) {
    std::vector<std::vector<float>> embeddings;
    json req = {
        {"model",  model_name},
        {"input", texts}
    };

    std::cout << "Embedding request: " << req.dump() << std::endl;

    int max_retries = 3;
    int delay_ms = 1000; // 1 second
    std::string res;

    for (int attempt = 1; attempt <= max_retries; ++attempt) {
        try {
            // Override shorter timeout for large batches
            res = http.post(endpoint + "/v1/embeddings", req.dump(),
                            {"Content-Type: application/json"});


            std::cout << "Response (attempt " << attempt << "): " << res << std::endl;

            auto j = json::parse(res);

            if (j.contains("data")) {
                const auto& arr = j["data"];
                for (auto& item : arr) {
                    const auto& emb = item["embedding"];
                    std::vector<float> out;
                    out.reserve(emb.size());
                    for (auto& x : emb) out.push_back((float)x.get<double>());
                    embeddings.push_back(std::move(out));
                }
                return embeddings;
            }

            throw std::runtime_error("Unexpected embedding response: " + j.dump());
        } catch (const std::exception& ex) {
            std::cerr << "Batch embedding failed (attempt " << attempt << "): "
                      << ex.what() << std::endl;

            if (attempt < max_retries) {
                std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
                delay_ms *= 2; // exponential backoff
            } else {
                throw; // rethrow last error
            }
        }
    }

    throw std::runtime_error("All batch embedding attempts failed unexpectedly.");
}


std::vector<float> TextEmbedder::embed(const std::string& text) {
    json req = {
        {"model",  model_name},
        {"prompt", text}
    };

    // log the request
    std::cout << "Embedding request: " << req.dump() << std::endl;

    int max_retries = 3;
    int delay_ms = 1000; // 1 second gap between retries

    for (int attempt = 1; attempt <= max_retries; ++attempt) {
        try {
            std::string res = http.post(endpoint + "/api/embeddings", req.dump(),
                                        {"Content-Type: application/json"});
            auto j = json::parse(res);

            // log response
            std::cout << "Response (attempt " << attempt << "): " << res << std::endl;

            // Ollama format: { "embedding": [ ... ] }
            if (j.contains("embedding")) {
                const auto& arr = j["embedding"];
                std::vector<float> out;
                out.reserve(arr.size());
                for (auto& x : arr) out.push_back((float)x.get<double>());
                return out;
            }

            // OpenAI format: { "data": [ { "embedding": [ ... ] } ] }
            if (j.contains("data")) {
                const auto& arr = j["data"][0]["embedding"];
                std::vector<float> out;
                out.reserve(arr.size());
                for (auto& x : arr) out.push_back((float)x.get<double>());
                return out;
            }

            throw std::runtime_error("Unexpected embedding response: " + j.dump());
        } catch (const std::exception& ex) {
            std::cerr << "Embedding request failed (attempt " << attempt << "): "
                      << ex.what() << std::endl;

            if (attempt < max_retries) {
                std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
                delay_ms *= 2; // exponential backoff (optional)
            } else {
                throw; // rethrow last error after max retries
            }
        }
    }

    throw std::runtime_error("All embedding attempts failed unexpectedly.");
}
