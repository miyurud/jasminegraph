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

#include "LLMUtils.h"

#include <curl/curl.h>

#include <chrono>
#include <nlohmann/json.hpp>
#include <thread>

#include "../../util/logger/Logger.h"

Logger llmutil_logger;

using json = nlohmann::json;

namespace LLMUtils {

struct CurlGlobalInit {
    CurlGlobalInit() { curl_global_init(CURL_GLOBAL_DEFAULT); }
    ~CurlGlobalInit() { curl_global_cleanup(); }
};

static CurlGlobalInit curlInit;

static size_t CurlWriteCallback(void* contents, size_t size, size_t nmemb, void* userp) {
    ((std::string*)userp)->append((char*)contents, size * nmemb);
    return size * nmemb;
}

static std::string stripMarkdown(const std::string& s) {
    std::string out = s;

    auto start = out.find("```");
    if (start != std::string::npos) {
        start = out.find("\n", start);
        if (start != std::string::npos) {
            out = out.substr(start + 1);
        }
    }

    auto end = out.rfind("```");
    if (end != std::string::npos) {
        out = out.substr(0, end);
    }

    return out;
}

std::string callLLM(const std::string& prompt, const std::string& host, const std::string& model,
                    const std::string& engine) {
    std::string result;
    CURL* curl = curl_easy_init();
    if (!curl)
        return "";

    std::string url;
    if (engine == "vllm") {
        url = host + "/v1/chat/completions";
    } else {
        url = host + "/api/generate";
    }

    // TLS + connection settings
    curl_easy_setopt(curl, CURLOPT_SSLVERSION, CURL_SSLVERSION_DEFAULT);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_POST, 1L);

    json requestJson;
    if (engine == "vllm") {
        requestJson["model"] = model;
        requestJson["messages"] = json::array({{{"role", "user"}, {"content", prompt}}});
        requestJson["max_tokens"] = 10000;
        requestJson["stream"] = false;
    } else {
        requestJson["model"] = model;
        requestJson["prompt"] = prompt;
        requestJson["max_tokens"] = 8000;
        requestJson["stream"] = false;
    }

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
        llmutil_logger.error("CURL error: " + std::string(curl_easy_strerror(res)));
    }

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    try {
        json raw = json::parse(result);
        std::string text;

        if (engine == "vllm") {
            if (!raw.contains("choices") || raw["choices"].empty())
                return "";

            text = raw["choices"][0]["message"]["content"].get<std::string>();
        } else {
            if (!raw.contains("response"))
                return "";

            text = raw["response"].get<std::string>();
        }

        return stripMarkdown(text);
    } catch (const std::exception& e) {
        llmutil_logger.error("LLM response parse error: " + std::string(e.what()));
    }
    return "";
}
}  // namespace LLMUtils
