#include "LLMUtils.h"
#include <curl/curl.h>
#include <nlohmann/json.hpp>
#include <thread>
#include <chrono>
#include "../../util/logger/Logger.h"

Logger llmutil_logger;

using json = nlohmann::json;

namespace LLMUtils
{

    struct CurlGlobalInit
    {
        CurlGlobalInit()  { curl_global_init(CURL_GLOBAL_DEFAULT); }
        ~CurlGlobalInit() { curl_global_cleanup(); }
    };

    static CurlGlobalInit curlInit;

    static size_t CurlWriteCallback(void *contents, size_t size, size_t nmemb, void *userp)
    {
        ((std::string *)userp)->append((char *)contents, size * nmemb);
        return size * nmemb;
    }

    std::string callLLM(const std::string &prompt, const std::string &host, const std::string &model)
    {
        std::string result;
        CURL *curl = curl_easy_init();
        if (!curl)
            return "";

        curl_easy_setopt(curl, CURLOPT_URL, (host + "/api/generate").c_str());
        curl_easy_setopt(curl, CURLOPT_POST, 1L);

        json requestJson;
        requestJson["model"] = model;
        requestJson["prompt"] = prompt;
        requestJson["max_tokens"] = 8000;
        requestJson["stream"] = false;

        std::string postFields = requestJson.dump();

        struct curl_slist *headers = nullptr;
        headers = curl_slist_append(headers, "Content-Type: application/json");
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, postFields.c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, postFields.size());

        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, CurlWriteCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &result);

        CURLcode res = curl_easy_perform(curl);
        if (res != CURLE_OK)
        {
            llmutil_logger.error("CURL error: " + std::string(curl_easy_strerror(res)));
        }

        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);

        return result;
    }

}
