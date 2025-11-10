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
#pragma once
#include <nlohmann/json.hpp>
#include <stdexcept>
#include <string>
#include <vector>
// Simple HTTP client wrapper for POST requests
class HttpClient {
 public:
  HttpClient();
  ~HttpClient();

  std::string post(const std::string& url, const std::string& body,
                   const std::vector<std::string>& headers);
};

// Text embedding client
class TextEmbedder {
 public:
  TextEmbedder(const std::string& endpoint, const std::string& model_name);
  std::vector<std::vector<float>> batch_embed(
      const std::vector<std::string>& texts);

  // Get embedding vector for given text
  std::vector<float> embed(const std::string& text);

 private:
  std::string endpoint;
  std::string model_name;
  HttpClient http;
};
