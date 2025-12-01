/**
Copyright 2025 JasmineGraph Team
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
#include "TextEmbedder.h"

#include <curl/curl.h>

#include <chrono>
#include <iostream>
#include <nlohmann/json.hpp>
#include <thread>
#include "../util/logger/Logger.h"

#include "TextEmbedder.h"
using json = nlohmann::json;
Logger text_embedder_logger;

// ---------------- HTTP Client ----------------
static size_t WriteToString(void* contents, size_t size, size_t nmemb,
                            void* userp) {
  size_t total = size * nmemb;
  std::string* s = static_cast<std::string*>(userp);
  s->append(static_cast<char*>(contents), total);
  return total;
}

HttpClient::HttpClient() { curl_global_init(CURL_GLOBAL_ALL); }

HttpClient::~HttpClient() { curl_global_cleanup(); }

std::string HttpClient::post(const std::string& url, const std::string& body,
                             const std::vector<std::string>& headers) {
  std::string response;

  CURL* curl = curl_easy_init();
  if (!curl) throw std::runtime_error("curl_easy_init failed");
  curl_easy_setopt(curl, CURLOPT_SSLVERSION, CURL_SSLVERSION_DEFAULT);
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
  curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 5L);  // 5s to connect
  curl_easy_setopt(curl, CURLOPT_TIMEOUT, 300L);  // 5 minutes max per request

  CURLcode res = curl_easy_perform(curl);
  long code = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);

  curl_slist_free_all(header_list);
  curl_easy_cleanup(curl);

  if (res != CURLE_OK) {
    throw std::runtime_error(std::string("curl error: ") +
                             curl_easy_strerror(res));
  }
  return response;
}

// ---------------- TextEmbedder ----------------
TextEmbedder::TextEmbedder(const std::string& endpoint,
                           const std::string& model_name)
    : endpoint(endpoint), model_name(model_name) {}

std::vector<std::vector<float>> TextEmbedder::batch_embed(
    const std::vector<std::string>& texts) {
  std::vector<std::vector<float>> embeddings;
  json req = {{"model", model_name}, {"input", texts}};

  int max_retries = 3;
  int delay_ms = 1000;  // 1 second
  std::string res;

  for (int attempt = 1; attempt <= max_retries; ++attempt) {
    try {
      // Override shorter timeout for large batches
      res = http.post(endpoint + "/v1/embeddings", req.dump(),
                      {"Content-Type: application/json"});

      auto jsonResponse = json::parse(res);

      if (jsonResponse.contains("data")) {
        const auto& arr = jsonResponse["data"];
        for (auto& item : arr) {
          const auto& emb = item["embedding"];
          std::vector<float> out;
          out.reserve(emb.size());
          for (auto& x : emb) out.push_back((float)x.get<double>());
          embeddings.push_back(std::move(out));
        }
        return embeddings;
      }

      throw std::runtime_error("Unexpected embedding response: " + jsonResponse.dump());
    } catch (const std::exception& ex) {
      text_embedder_logger.error("Batch embedding failed: " + std::string(ex.what()));

      if (attempt < max_retries) {
        std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
        delay_ms *= 2;  // exponential backoff
      } else {
        throw;  // rethrow last error
      }
    }
  }

  throw std::runtime_error("All batch embedding attempts failed unexpectedly.");
}

std::vector<float> TextEmbedder::embed(const std::string& text) {
  json req = {{"model", model_name}, {"prompt", text}};

  int max_retries = 3;
  int delay_ms = 1000;  // 1 second gap between retries

  for (int attempt = 1; attempt <= max_retries; ++attempt) {
    try {
      std::string res = http.post(endpoint + "/api/embeddings", req.dump(),
                                  {"Content-Type: application/json"});
      auto jsonLine = json::parse(res);

      if (jsonLine.contains("embedding")) {
        const auto& arr = jsonLine["embedding"];
        std::vector<float> out;
        out.reserve(arr.size());
        for (auto& x : arr) out.push_back((float)x.get<double>());
        return out;
      }

      // OpenAI format: { "data": [ { "embedding": [ ... ] } ] }
      if (jsonLine.contains("data")) {
        const auto& arr = jsonLine["data"][0]["embedding"];
        std::vector<float> out;
        out.reserve(arr.size());
        for (auto& x : arr) out.push_back((float)x.get<double>());
        return out;
      }

      throw std::runtime_error("Unexpected embedding response: " + jsonLine.dump());
    } catch (const std::exception& ex) {
      text_embedder_logger.error("Embedding failed: " +std::string(ex.what()));


      if (attempt < max_retries) {
        std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
        delay_ms *= 2;  // exponential backoff (optional)
      } else {
        throw;  // rethrow last error after max retries
      }
    }
  }

  throw std::runtime_error("All embedding attempts failed unexpectedly.");
}
