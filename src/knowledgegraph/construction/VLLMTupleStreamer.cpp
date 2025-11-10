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
#include "VLLMTupleStreamer.h"

#include <curl/curl.h>

#include <chrono>
#include <nlohmann/json.hpp>
#include <thread>

#include "../../util/Utils.h"
#include "../../util/logger/Logger.h"
#include "Prompt.h"

using json = nlohmann::json;
Logger vllm_tuple_streamer_logger;

// ---------------- Constructor ----------------
VLLMTupleStreamer::VLLMTupleStreamer(const std::string& modelName,
                                     const std::string& host)
    : model(modelName), host(host) {
  curl_global_init(CURL_GLOBAL_DEFAULT);
  vllm_tuple_streamer_logger.info("Initialized VLLMTupleStreamer with model: " +
                                  modelName + ", host: " + host);
}

// ---------------- Stream Callback ----------------
size_t VLLMTupleStreamer::StreamCallback(char* ptr, size_t size, size_t nmemb,
                                         void* userdata) {
  size_t totalSize = size * nmemb;
  StreamContext* ctx = static_cast<StreamContext*>(userdata);

  std::string incoming(ptr, totalSize);
  size_t start = 0;
  while (start < incoming.size()) {
    size_t pos = incoming.find("\n", start);
    if (pos == std::string::npos) pos = incoming.size();
    std::string line = incoming.substr(start, pos - start);
    start = pos + 1;

    if (line.empty()) continue;

    // End of stream
    if (line == "data: [DONE]") {
      if (!ctx->current_tuple.empty()) {
        ctx->buffer->add(ctx->current_tuple);
        ctx->current_tuple.clear();
      }
      ctx->buffer->add("-1");  // Signal end
      break;
    }

    // Only process lines starting with "data: "
    const std::string prefix = "data: ";
    if (line.rfind(prefix, 0) != 0) continue;

    std::string jsonPart = line.substr(prefix.size());
    try {
      auto j = json::parse(jsonPart);
      if (j.contains("choices") && !j["choices"].empty()) {
        std::string partial = j["choices"][0]["delta"].value("content", "");
        vllm_tuple_streamer_logger.info("Partial: " + partial);

        size_t i = 0;
        while (i < partial.size()) {
          size_t bpos = partial.find_first_of("[]", i);
          if (bpos == std::string::npos) {
            if (ctx->braceDepth >= 2) {
              ctx->current_tuple.append(partial, i, std::string::npos);
            }
            break;
          }
          if (ctx->braceDepth >= 2) {
            ctx->current_tuple.append(partial, i, bpos - i);
          }

          char c = partial[bpos];
          if (c == '[') {
            ctx->braceDepth++;
            if (ctx->braceDepth >= 2) ctx->current_tuple.push_back(c);
          } else {
            // ']'
            ctx->braceDepth--;
            ctx->current_tuple.push_back(c);

            if (ctx->braceDepth == 1) {
              vllm_tuple_streamer_logger.info("Current tuple: " +
                                              ctx->current_tuple);
              try {
                auto triple = json::parse(ctx->current_tuple);
                if (triple.is_array() && triple.size() == 5) {
                  std::string subject = triple[0].get<std::string>();
                  std::string predicate = triple[1].get<std::string>();
                  std::string object = triple[2].get<std::string>();
                  std::string subject_type = triple[3].get<std::string>();
                  std::string object_type = triple[4].get<std::string>();

                  std::string subject_id =
                      Utils::canonicalize(subject + "_" + subject_type);
                  std::string object_id =
                      Utils::canonicalize(object + "_" + object_type);
                  std::string edge_id = Utils::canonicalize(
                      subject_id + "_" + predicate + "_" + object_id);

                  json formattedTriple = {
                      {"source",
                       {{"id", subject_id},
                        {"properties",
                         {{"id", subject_id},
                          {"label", subject_type},
                          {"name", subject}}}}},
                      {"destination",
                       {{"id", object_id},
                        {"properties",
                         {{"id", object_id},
                          {"label", object_type},
                          {"name", object}}}}},
                      {"properties",
                       {{"id", edge_id},
                        {"type", predicate},
                        {"description",
                         subject + " " + predicate + " " + object}}}};

                  ctx->buffer->add(formattedTriple.dump());
                  vllm_tuple_streamer_logger.debug(
                      "✅ Added formatted triple: " + formattedTriple.dump());
                }
              } catch (const std::exception& ex) {
                vllm_tuple_streamer_logger.error(
                    "❌ JSON array parse failed: " + std::string(ex.what()));
              }
              ctx->current_tuple.clear();
            }
          }
          i = bpos + 1;
        }
      }
    } catch (const std::exception& ex) {
      vllm_tuple_streamer_logger.error("JSON parse error: " +
                                       std::string(ex.what()));
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
      vllm_tuple_streamer_logger.error("Failed to initialize CURL");
      return;
    }

    // TLS settings
    curl_easy_setopt(curl, CURLOPT_SSLVERSION, CURL_SSLVERSION_MAX_DEFAULT);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);

    // Timeouts and keepalive
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 50L);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 600L);
    curl_easy_setopt(curl, CURLOPT_LOW_SPEED_LIMIT, 1L);
    curl_easy_setopt(curl, CURLOPT_LOW_SPEED_TIME, 120L);
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPIDLE, 60L);
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPINTVL, 30L);

    std::string url = host + "/v1/chat/completions";
    vllm_tuple_streamer_logger.info("Connecting to VLLM server at: " + host);

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_POST, 1L);

    // JSON request
    json j;
    j["model"] = model;
    j["messages"] = {{{"role", "system"},
                      {"content",
                       "You are an expert information extractor specialized in "
                       "knowledge graph construction."}},
                     {{"role", "user"},
                      {"content", Prompts::KNOWLEDGE_EXTRACTION +
                                      "\nNow process the following text:\n" +
                                      chunkText + "\n\nArray:"}}};

    j["stream"] = true;
    j["max_tokens"] = 10000;

    std::string postFields = j.dump();
    vllm_tuple_streamer_logger.info("Post fields: " + postFields);

    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, postFields.c_str());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, postFields.size());

    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, StreamCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &ctx);

    res = curl_easy_perform(curl);

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    if (res != CURLE_OK && attempt < maxRetries - 1) {
      vllm_tuple_streamer_logger.error("CURL response code: " +
                                       std::to_string(res));
      int waitTime = baseDelaySeconds * attempt;
      vllm_tuple_streamer_logger.info("Retrying in " +
                                      std::to_string(waitTime) + " seconds...");
      std::this_thread::sleep_for(std::chrono::seconds(waitTime));
    }

    attempt++;
  } while ((res != CURLE_OK && attempt < maxRetries) || !ctx.isSuccess);

  if (res != CURLE_OK) {
    vllm_tuple_streamer_logger.error("Failed after " +
                                     std::to_string(maxRetries) + " attempts.");
    ctx.buffer->add("-1");
  }
}
