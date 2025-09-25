#pragma once
#include <vector>
#include <string>
#include <memory>
#include <mutex>
#include <faiss/IndexIVFPQ.h>

class FaissStore {
public:
    // Delete copy/move to enforce singleton
    FaissStore(const FaissStore&) = delete;
    FaissStore& operator=(const FaissStore&) = delete;
    FaissStore(FaissStore&&) = delete;
    FaissStore& operator=(FaissStore&&) = delete;

    // Access the singleton instance
    static FaissStore* getInstance(int embeddingDim = 128,
                                   const std::string& filepath = "faiss.index",
                                   size_t nlist_ = 4096,
                                   size_t m_ = 64,
                                   size_t nbits_ = 8);

    void trainIfNeeded(const std::vector<std::vector<float>>& sampleVectors);
    void add(const std::vector<float>& embedding);
    void addBatch(const std::vector<std::vector<float>>& embeddings);
    std::vector<std::pair<faiss::idx_t, float>> search(const std::vector<float>& query, int k);
    void save();

private:
    FaissStore(int embeddingDim, const std::string& filepath,
               size_t nlist_, size_t m_, size_t nbits_);
    ~FaissStore();
    void loadOrCreate();

    int dim;
    std::string filePath;
    size_t nlist;
    size_t m;
    size_t nbits;

    std::unique_ptr<faiss::IndexIVFPQ> index;
    std::mutex writeMtx;
    std::mutex readMtx;
};
