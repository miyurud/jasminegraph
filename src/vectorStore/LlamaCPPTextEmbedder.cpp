//
// Created by sajeenthiran on 2025-08-15
//

#include "TextEmbedder.h"

#include "llama.h"  // from latest llama.cpp include folder
#include <stdexcept>
#include <iostream>

struct LlamaCPPTextEmbedder::Impl {
    llama_model* model;
    llama_context* ctx;
    llama_context_params ctx_params;

    Impl(const std::string& modelPath) {
        llama_backend_init(); // initialize backend

        ctx_params = llama_context_default_params();
        ctx_params.embeddings = true; // enable embeddings
        ctx_params.n_ctx = 512;

        llama_model_params mparams = llama_model_default_params();
        model = llama_load_model_from_file(modelPath.c_str(), mparams);
        if (!model) {
            throw std::runtime_error("Failed to load model: " + modelPath);
        }

        ctx = llama_new_context_with_model(model, ctx_params);
        if (!ctx) {
            llama_free_model(model);
            throw std::runtime_error("Failed to create llama context");
        }
    }

    ~Impl() {
        llama_free(ctx);
        llama_free_model(model);
        llama_backend_free();
    }

    std::vector<float> embed(const std::string& text) {
        // Tokenize
        std::vector<llama_token> tokens(text.size() + 8);
        int n_tokens = llama_tokenize( llama_model_get_vocab(model), text.c_str(), text.length(), tokens.data(), tokens.size(), true,true);
        if (n_tokens < 0) {
            throw std::runtime_error("Tokenization failed");
        }
        tokens.resize(n_tokens);

        // Evaluate (n_threads = 4 for example)
        // if (llama_eval(ctx, tokens.data(), tokens.size(), 0, 4) != 0) {
        //     throw std::runtime_error("Eval failed");
        // }

        int embd_size = llama_n_embd(model);
        const float* embd_data = llama_get_embeddings(ctx);
        if (!embd_data) {
            throw std::runtime_error("No embedding data returned");
        }

        return std::vector<float>(embd_data, embd_data + embd_size);
    }

    std::vector<std::vector<float>> embedBatch(const std::vector<std::string>& texts) {
        std::vector<std::vector<float>> results;
        results.reserve(texts.size());

        for (const auto& text : texts) {
            results.push_back(embed(text));
        }
        return results;
    }
};

LlamaCPPTextEmbedder::LlamaCPPTextEmbedder(const std::string& modelPath)
    : pImpl(new Impl(modelPath)) {}

LlamaCPPTextEmbedder::~LlamaCPPTextEmbedder() {
    delete pImpl;
}

std::vector<float> LlamaCPPTextEmbedder::embed(const std::string& text) {
    return pImpl->embed(text);
}

std::vector<std::vector<float>> LlamaCPPTextEmbedder::embedBatch(const std::vector<std::string>& texts) {
    return pImpl->embedBatch(texts);
}
