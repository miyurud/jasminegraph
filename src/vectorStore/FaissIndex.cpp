#include "FaissIndex.h"
#include <faiss/index_io.h>
#include <faiss/IndexFlat.h>
#include <faiss/IndexIDMap.h>
#include <fstream>
#include <iostream>
#include <stdexcept>

// Static members
std::unique_ptr<FaissIndex> FaissIndex::instance = nullptr;
std::once_flag FaissIndex::initFlag;

FaissIndex* FaissIndex::getInstance(int embeddingDim, const std::string& filepath) {
    std::call_once(initFlag, [&]() {
        instance.reset(new FaissIndex(embeddingDim, filepath));

    });
    return instance.get();
}

FaissIndex::FaissIndex(int embeddingDim, const std::string& filepath)
    : dim(embeddingDim), filePath(filepath)
{
    load(filepath);
    // std::ifstream f(filepath);
    // std::cout<< "Loading FAISS index from: " << filepath << std::endl;
    // if (f.good()) {
    //     std::cout << "File exists, loading index..." << std::endl;
    //     faiss::Index* loaded = faiss::read_index(filepath.c_str());
    //     index = dynamic_cast<faiss::IndexFlatL2*>(loaded);FA
    //     if (!index) {
    //         throw std::runtime_error("Loaded FAISS index is not L2 Flat index.");
    //     }
    // } else {
    //     // Otherwise create new index
    //     index = new faiss::IndexFlatL2(dim);
    // }
}

FaissIndex::~FaissIndex() {
    try {
        save(filePath);
    } catch (const std::exception& e) {
        fprintf(stderr, "[FaissIndex] Failed to auto-save index: %s\n", e.what());
    }
    delete index;
}

faiss::idx_t FaissIndex::add(const std::vector<float>& embedding, std::string nodeId) {
    if (embedding.size() != dim) {
        throw std::runtime_error("Embedding dimension mismatch!");
    }
    std::lock_guard<std::mutex> lock(mtx);

   faiss::idx_t new_id = index->ntotal;
   std::cout << "[FaissIndex] Adding new embedding with nodeId: " << nodeId << ", assigned id: " << new_id << std::endl;

   index->add(1, embedding.data());

    // // iterate over the nodeIdToEmbeddingIdMap to check if the nodeId already exists
    // for (auto & entry : nodeIdToEmbeddingIdMap) {
    //     // if (entry.first == nodeId) {
    //         std::cout << "[FaissIndex] Node ID already exists in the map. Returning existing ID: " << entry.second << std::endl;
    //         // return entry.second; // Return existing ID if nodeId already exists
    //     // }
    // }
    // for (auto & entry : embeddingIdToNodeIdMap) {
    //     // if (entry.first == nodeId) {
    //     std::cout << "[FaissIndex] Node ID already exists in the map. Returning existing ID: " << entry.second << std::endl;
    //     // return entry.second; // Return existing ID if nodeId already exists
    //     // }
    // }
   std::cout << "[FaissIndex] Embedding added to index. Updating nodeEmbeddingMap." << std::endl;
    nodeIdToEmbeddingIdMap.insert({nodeId, new_id});
    embeddingIdToNodeIdMap.insert({new_id, nodeId});
   // nodeEmbeddingMap.insert( value_type(nodeId, new_id));
   std::cout << "[FaissIndex] nodeEmbeddingMap updated for nodeId: " << nodeId << std::endl;
    return new_id;
}

std::vector<std::pair<faiss::idx_t, float>> FaissIndex::search(const std::vector<float>& query, int k) {

    if (query.size() != dim) {
        throw std::runtime_error("Query dimension mismatch!");
    }

    std::cout << "61";

    std::vector<faiss::idx_t> indices(k);
    std::vector<float> distances(k);
    std::cout << "65";

    std::lock_guard<std::mutex> lock(mtx);
    std::cout << "68";

    index->search(1, query.data(), k, distances.data(), indices.data());
    std::cout << "71";

    std::vector<std::pair<faiss::idx_t, float>> results;
    for (int i = 0; i < k; i++) {
        results.emplace_back(indices[i], distances[i]);
    }
    return results;
}

void FaissIndex::save(const std::string& filepath) {
    std::lock_guard<std::mutex> lock(mtx);

    // Save FAISS index
    faiss::write_index(index, filepath.c_str());

    // Save mapping alongside index (e.g., filepath + ".map")
    std::ofstream mapFile(filepath + ".map", std::ios::binary);
    if (!mapFile.is_open()) {
        throw std::runtime_error("Failed to open map file for saving.");
    }

    size_t size = nodeIdToEmbeddingIdMap.size();
    mapFile.write(reinterpret_cast<const char*>(&size), sizeof(size));

    for (const auto& entry : nodeIdToEmbeddingIdMap) {
        size_t keyLen = entry.first.size();
        mapFile.write(reinterpret_cast<const char*>(&keyLen), sizeof(keyLen));
        mapFile.write(entry.first.data(), keyLen);
        mapFile.write(reinterpret_cast<const char*>(&entry.second), sizeof(entry.second));
    }

    mapFile.close();
}
void FaissIndex::load(const std::string& filepath) {
    std::lock_guard<std::mutex> lock(mtx);

    // Load FAISS index
    std::ifstream f(filepath, std::ios::binary);
    std::cout << "Loading FAISS index from: " << filepath << std::endl;

    if (f.good()) {
        std::cout << "File exists, loading index..." << std::endl;
        faiss::Index* loaded = faiss::read_index(filepath.c_str());
        index = dynamic_cast<faiss::IndexFlatL2*>(loaded);
        if (!index) {
            throw std::runtime_error("Loaded FAISS index is not L2 Flat index.");
        }
    } else {
        // Create a new index if file not found
        index = new faiss::IndexFlatL2(dim);
    }

    std::cout << "[FaissIndex::load] FAISS index loaded successfully." << std::endl;

    // Load mapping file
    std::ifstream mapFile(filepath + ".map", std::ios::binary);
    if (!mapFile.is_open()) {
        std::cerr << "[FaissIndex::load] [Warning] Mapping file not found, nodeEmbeddingMap will be empty." << std::endl;
        return;
    }

    size_t size = 0;
    if (!mapFile.read(reinterpret_cast<char*>(&size), sizeof(size))) {
        std::cerr << "[FaissIndex::load] Failed to read mapping size." << std::endl;
        return;
    }

    std::cout << "[FaissIndex::load] Mapping file opened. Entries to read: " << size << std::endl;

    for (size_t i = 0; i < size; i++) {
        size_t keyLen = 0;

        if (!mapFile.read(reinterpret_cast<char*>(&keyLen), sizeof(keyLen))) {
            std::cerr << "[FaissIndex::load] Unexpected EOF while reading key length." << std::endl;
            break;
        }

        // Sanity check key length
        if (keyLen == 0 || keyLen > 1024) {
            std::cerr << "[FaissIndex::load] Invalid key length " << keyLen << ", skipping entry." << std::endl;
            // Skip the value if key length is invalid
            faiss::idx_t dummy;
            if (!mapFile.read(reinterpret_cast<char*>(&dummy), sizeof(dummy))) {
                std::cerr << "[FaissIndex::load] Unexpected EOF while skipping value." << std::endl;
            }
            continue;
        }

        std::string key(keyLen, '\0');
        if (!mapFile.read(&key[0], keyLen)) {
            std::cerr << "[FaissIndex::load] Unexpected EOF while reading key." << std::endl;
            break;
        }

        // Remove trailing null bytes if present
        size_t realLen = strnlen(key.c_str(), keyLen);
        key.resize(realLen);

        faiss::idx_t value = 0;
        if (!mapFile.read(reinterpret_cast<char*>(&value), sizeof(value))) {
            std::cerr << "[FaissIndex::load] Unexpected EOF while reading value." << std::endl;
            break;
        }

        nodeIdToEmbeddingIdMap[key] = value;
        embeddingIdToNodeIdMap[value] = key;

        // std::cout << "[FaissIndex::load] Mapping loaded: " << key << " -> " << value << std::endl;
    }

    mapFile.close();
    std::cout << "[FaissIndex::load] Mapping file closed." << std::endl;
}


std::vector<float> FaissIndex::getEmbeddingById(std::string  nodeId) {
    std::lock_guard<std::mutex> lock(mtx);

    if (!index) {
        throw std::runtime_error("FAISS index not initialized.");
    }

    // Allocate vector for reconstructed embedding
    std::vector<float> embedding(dim);

    try {
        // FAISS reconstruct expects the **internal index position**, not necessarily the ID
        // If using IndexIDMap, reconstruct the vector for a given ID
        if (nodeIdToEmbeddingIdMap.find(nodeId) == nodeIdToEmbeddingIdMap.end()) {
           return embedding; // Return empty vector if nodeId not found
        }
        faiss::idx_t id = nodeIdToEmbeddingIdMap.at(nodeId.c_str());
        if (id < 0 || id >= index->ntotal) {
            throw std::out_of_range("ID out of range in FAISS index.");
        }
        index->reconstruct(id, embedding.data());
    } catch (const std::exception& e) {
        throw std::runtime_error(std::string("Failed to reconstruct embedding for ID ") +
                                nodeId + ": " + e.what());
    }

    return embedding;
}

std::string FaissIndex::getNodeIdFromEmbeddingId(faiss::idx_t embeddingId)
{
    // std::lock_guard<std::mutex> lock(mtx);

    // Debug logging (optional)
    // for (const auto& entry : nodeIdToEmbeddingIdMap) {
    //     std::cout << "Node ID: " << entry.first
    //               << ", Embedding ID: " << entry.second << std::endl;
    // }

    auto it = embeddingIdToNodeIdMap.find(embeddingId);
    if (it == embeddingIdToNodeIdMap.end()) {
        throw std::runtime_error("Node ID not found for embedding ID: " + std::to_string(embeddingId));
    }

    return it->second; // access the nodeId from the right map
}