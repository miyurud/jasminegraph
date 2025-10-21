#pragma once
#include <faiss/IndexIDMap.h>
#include <faiss/IndexFlat.h>
#include <mutex>
#include <string>
#include <vector>
#include <memory>
#include <boost/bimap.hpp>
#include <boost/bimap/set_of.hpp>
#include <boost/bimap/multiset_of.hpp>
class FaissIndex {
public:
    // Get singleton instance
    static FaissIndex* getInstance(int embeddingDim, const std::string& filepath);

    ~FaissIndex();
    faiss::idx_t add(const std::vector<float>& embedding, std::string nodeId);

    faiss::idx_t add(const std::vector<float>& embedding);

    // Search returns (id, distance)
    std::vector<std::pair<faiss::idx_t, float>> search(const std::vector<float>& query, int k);

    void save(const std::string& filepath);
    void save();
    void load(const std::string& filepath);
    std::vector<float> getEmbeddingById(std::string id);
    std::string getNodeIdFromEmbeddingId(faiss::idx_t embeddingId);
    std::string getNodeIdFromEmbeddingId(std::string embeddingId) const;

private:
    // Private constructor (Singleton)
    FaissIndex(int embeddingDim, const std::string& filepath);

    int dim;
    faiss::IndexFlatL2* index;
    std::mutex mtx;
    std::string filePath;
    // typedef boost::bimap<boost::bimaps::multiset_of<std::string>, boost::bimaps::set_of<faiss::idx_t> > bimap_t;
    // typedef bimap_t::value_type value_type;

    // boost::bimap<std::string, faiss::idx_t > nodeEmbeddingMap; // Maps node IDs to FAISS IDs
    std::unordered_map<std::string,faiss::idx_t> nodeIdToEmbeddingIdMap; // Maps node IDs to FAISS IDs
    std::unordered_map<faiss::idx_t,std::string> embeddingIdToNodeIdMap;


    static std::unique_ptr<FaissIndex> instance;
    static std::once_flag initFlag;
};