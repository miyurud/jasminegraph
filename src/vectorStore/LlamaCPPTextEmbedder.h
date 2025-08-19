//
// Created by sajeenthiran on 2025-08-15.
//

#ifndef JASMINEGRAPH_TEXTEMBEDDER_H
#define JASMINEGRAPH_TEXTEMBEDDER_H

#pragma once
#include <string>
#include <vector>
class LlamaCPPTextEmbedder {
public:
    LlamaCPPTextEmbedder(const std::string& modelPath);
    ~LlamaCPPTextEmbedder();

    // Single text embedding
    std::vector<float> embed(const std::string& text);

    // Batch embedding: one vector per text
    std::vector<std::vector<float>> embedBatch(const std::vector<std::string>& texts);

private:
    struct Impl;
    Impl* pImpl;
};
#endif //JASMINEGRAPH_TEXTEMBEDDER_H