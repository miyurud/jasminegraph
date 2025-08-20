#include "FaissStore.h"
#include <faiss/index_io.h>
#include <faiss/IndexFlat.h>
#include <faiss/IndexIDMap.h>
#include <fstream>
#include <iostream>
#include <stdexcept>

// Static members
std::unique_ptr<FaissStore> FaissStore::instance = nullptr;
std::once_flag FaissStore::initFlag;

FaissStore* FaissStore::getInstance(int embeddingDim, const std::string& filepath) {
    std::call_once(initFlag, [&]() {
        instance.reset(new FaissStore(embeddingDim, filepath));
    });
    return instance.get();
}

FaissStore::FaissStore(int embeddingDim, const std::string& filepath)
    : dim(embeddingDim), filePath(filepath)
{
    std::ifstream f(filepath);
    std::cout << "Loading FAISS index from: " << filepath << std::endl;

    if (f.good()) {
        std::cout << "File exists, loading index..." << std::endl;
        faiss::Index* loaded = faiss::read_index(filepath.c_str());
        index = dynamic_cast<faiss::IndexIDMap*>(loaded);
        if (!index) {
            throw std::runtime_error("Loaded FAISS index is not IndexIDMap.");
        }
    } else {
        // Create a new IndexFlatL2 wrapped in IndexIDMap
        faiss::IndexFlatL2* base_index = new faiss::IndexFlatL2(dim);
        index = new faiss::IndexIDMap(base_index);
    }
}

FaissStore::~FaissStore() {
    try {
        save(filePath);
    } catch (const std::exception& e) {
        fprintf(stderr, "[FaissStore] Failed to auto-save index: %s\n", e.what());
    }
    delete index;
}

void FaissStore::add(const std::vector<float>& embedding, faiss::idx_t custom_id) {
    if (embedding.size() != dim) {
        throw std::runtime_error("Embedding dimension mismatch!");
    }
    std::lock_guard<std::mutex> lock(mtx);
    index->add_with_ids(1, embedding.data(), &custom_id);
}

std::vector<std::pair<faiss::idx_t, float>> FaissStore::search(const std::vector<float>& query, int k) {
    if (query.size() != dim) {
        throw std::runtime_error("Query dimension mismatch!");
    }

    std::vector<faiss::idx_t> indices(k);
    std::vector<float> distances(k);

    std::lock_guard<std::mutex> lock(mtx);
    index->search(1, query.data(), k, distances.data(), indices.data());

    std::vector<std::pair<faiss::idx_t, float>> results;
    for (int i = 0; i < k; i++) {
        results.emplace_back(indices[i], distances[i]);
    }
    return results;
}

void FaissStore::save(const std::string& filepath) {
    std::lock_guard<std::mutex> lock(mtx);
    faiss::write_index(index, filepath.c_str());
}

void FaissStore::load(const std::string& filepath) {
    std::lock_guard<std::mutex> lock(mtx);
    faiss::Index* loaded = faiss::read_index(filepath.c_str());
    if (!loaded) throw std::runtime_error("Failed to load FAISS index.");

    delete index;
    index = dynamic_cast<faiss::IndexIDMap*>(loaded);
    if (!index) throw std::runtime_error("Loaded index is not IndexIDMap.");
}
