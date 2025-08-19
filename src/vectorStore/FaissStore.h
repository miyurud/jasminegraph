#pragma once
#include <faiss/IndexFlat.h>
#include <mutex>
#include <string>
#include <vector>
#include <memory>

class FaissStore {
public:
    // Get singleton instance
    static FaissStore* getInstance(int embeddingDim, const std::string& filepath);

    ~FaissStore();

    void add(const std::vector<float>& embedding);
    std::vector<std::pair<faiss::idx_t, float>> search(const std::vector<float>& query, int k);

    void save(const std::string& filepath);
    void load(const std::string& filepath);

private:
    // Private constructor (Singleton)
    FaissStore(int embeddingDim, const std::string& filepath);

    int dim;
    faiss::IndexFlatL2* index;
    std::mutex mtx;
    std::string filePath;   // keep track of save location

    static std::unique_ptr<FaissStore> instance;  // singleton instance
    static std::once_flag initFlag;               // for thread-safe initialization
};
