#pragma once
#include <faiss/IndexIDMap.h>
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

    // Add with a custom ID
    void add(const std::vector<float>& embedding, faiss::idx_t custom_id);

    // (Optional) Add without ID → will use sequential IDs
    void add(const std::vector<float>& embedding);

    // Search returns (id, distance)
    std::vector<std::pair<faiss::idx_t, float>> search(const std::vector<float>& query, int k);

    void save(const std::string& filepath);
    void load(const std::string& filepath);

private:
    // Private constructor (Singleton)
    FaissStore(int embeddingDim, const std::string& filepath);

    int dim;
    faiss::IndexIDMap* index;     // <-- now supports custom IDs
    std::mutex mtx;
    std::string filePath;

    static std::unique_ptr<FaissStore> instance;
    static std::once_flag initFlag;
};
