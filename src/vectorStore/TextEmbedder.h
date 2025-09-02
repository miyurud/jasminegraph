//
// Created by sajeenthiran on 2025-08-16.
//

#pragma once
#include <string>
#include <vector>
#include <stdexcept>
#include <nlohmann/json.hpp>
// Simple HTTP client wrapper for POST requests
class HttpClient {
public:
    HttpClient();
    ~HttpClient();

    std::string post(const std::string& url,
                     const std::string& body,
                     const std::vector<std::string>& headers);
};

// Text embedding client
class TextEmbedder {
public:
    TextEmbedder(const std::string& endpoint, const std::string& model_name);
    std::vector<std::vector<float>> batch_embed(const std::vector<std::string>& texts);

    // Get embedding vector for given text
    std::vector<float> embed(const std::string& text);

private:
    std::string endpoint;
    std::string model_name;
    HttpClient http;
};

