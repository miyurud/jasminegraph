#include "FaissStore.h"
#include <fstream>
#include <stdexcept>
#include <cstdio>
#include <algorithm>
#include <iostream>
#include <faiss/index_io.h>
#include <faiss/IndexIVFPQ.h>
#include <faiss/IndexFlat.h>

FaissStore::FaissStore(int embeddingDim, const std::string& filepath,
                       size_t nlist_, size_t m_, size_t nbits_)
    : dim(embeddingDim), filePath(filepath), nlist(nlist_), m(m_), nbits(nbits_)
{
    loadOrCreate();
}

FaissStore::~FaissStore() {
    try {
        save(); // auto-save on destruction
    } catch (const std::exception& e) {
        fprintf(stderr, "[FaissStore] Auto-save failed: %s\n", e.what());
    }
}
FaissStore* FaissStore::getInstance(int embeddingDim, const std::string& filepath,
                                    size_t nlist_, size_t m_, size_t nbits_)
{
    // Thread-safe lazy initialization (C++11 and later)
    static FaissStore instance(embeddingDim, filepath, nlist_, m_, nbits_);
    return &instance;
}
void FaissStore::loadOrCreate() {
    std::ifstream f(filePath);
    if (f.good()) {
        // Load index from disk using memory-mapped read
        faiss::Index* loaded = faiss::read_index(filePath.c_str(), faiss::IO_FLAG_MMAP);
        // index.reset(dynamic_cast<faiss::IndexIVFPQ*>(loaded));
        if (!index) throw std::runtime_error("Loaded index is not IVFPQ");
    } else {
        // Create new IVF-PQ index (not trained yet)
        faiss::IndexFlatL2 quantizer(dim);
        index.reset(new faiss::IndexIVFPQ(&quantizer, dim, nlist, m, nbits));
        // Wait until train() is called with real sample vectors
    }
}

// Train index if not already trained
void FaissStore::trainIfNeeded(const std::vector<std::vector<float>>& sampleVectors) {
    std::lock_guard<std::mutex> lock(writeMtx);
    if (!index->is_trained) {
        size_t numTrain = sampleVectors.size();
        std::vector<float> trainData(numTrain * dim);
        for (size_t i = 0; i < numTrain; i++) {
            std::copy(sampleVectors[i].begin(), sampleVectors[i].end(),
                      trainData.begin() + i * dim);
        }
        index->train(numTrain, trainData.data());
    }
}

void FaissStore::add(const std::vector<float>& embedding) {
    if (embedding.size() != dim) throw std::runtime_error("Embedding dimension mismatch");

    std::lock_guard<std::mutex> lock(writeMtx);

    // If index is not trained, throw error (require user to train with sample first)
    if (!index->is_trained) throw std::runtime_error("Index not trained. Call trainIfNeeded() first.");

    index->add(1, embedding.data());
}

void FaissStore::addBatch(const std::vector<std::vector<float>>& embeddings) {
    if (embeddings.empty()) return;

    std::lock_guard<std::mutex> lock(writeMtx);


    std::cout<< "[FaissStore] Adding batch of " << embeddings.size() << " embeddings with dimension " << dim << std::endl;
    // If index not trained yet, train using first batch
    if (!index->is_trained) trainIfNeeded(embeddings);

    std::cout<< "[FaissStore] Index is trained, proceeding to add batch" << std::endl;
    // Flatten data
    size_t num = embeddings.size();
    std::vector<float> flatData(num * dim);
    for (size_t i = 0; i < num; i++) {
        if (embeddings[i].size() != dim)
            throw std::runtime_error("Embedding dimension mismatch in batch");
        std::copy(embeddings[i].begin(), embeddings[i].end(), flatData.begin() + i * dim);
    }

    index->add(num, flatData.data());
}

std::vector<std::pair<faiss::idx_t, float>> FaissStore::search(const std::vector<float>& query, int k) {
    if (query.size() != dim) throw std::runtime_error("Query dimension mismatch");

    std::vector<faiss::idx_t> indices(k);
    std::vector<float> distances(k);

    std::lock_guard<std::mutex> lock(readMtx);
    index->search(1, query.data(), k, distances.data(), indices.data());

    std::vector<std::pair<faiss::idx_t, float>> results;
    for (int i = 0; i < k; i++) results.emplace_back(indices[i], distances[i]);
    return results;
}

void FaissStore::save() {
    std::lock_guard<std::mutex> lock(writeMtx);
    faiss::write_index(index.get(), filePath.c_str());
}
