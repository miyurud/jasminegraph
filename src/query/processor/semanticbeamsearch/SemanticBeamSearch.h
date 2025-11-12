//
// Created by sajeenthiran on 2025-08-18.
//

#ifndef JASMINEGRAPH_SEMANTICBEAMSEARCH_H
#define JASMINEGRAPH_SEMANTICBEAMSEARCH_H
#include <iosfwd>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>
#include <string>
#include <vector>

#include "../../../nativestore/NodeManager.h"
#include "../../../server/JasmineGraphServer.h"
#include "../../../vectorstore/FaissIndex.h"
#include "../../../vectorstore/TextEmbedder.h"
#include "../cypher/util/SharedBuffer.h"

namespace faiss {
struct Index;
}

struct ScoredPath {
    nlohmann::json pathObj;
    float score;
};

class SemanticBeamSearch {
 private:
    FaissIndex *faissStore;
    TextEmbedder *textEmbedder;
    const std::vector<float> emb;
    int k;  // Number of top results to return
    GraphConfig gc;
    NodeManager *nodeManager;
    std::vector<JasmineGraphServer::worker> workerList;  // List of workers for remote expansion
    std::unordered_map<std::string, std::vector<float>> typeEmbeddingCache;

 public:
    SemanticBeamSearch(FaissIndex *faissStore, std::vector<float> emb, int k, GraphConfig gc);
    SemanticBeamSearch(FaissIndex *faissStore, TextEmbedder *textEmbedder, std::vector<float> emb, int k,
                       GraphConfig gc, vector<JasmineGraphServer::worker> workerList);
    SemanticBeamSearch(FaissIndex *faissStore, std::vector<float> emb, int k, GraphConfig gc,
                       vector<JasmineGraphServer::worker> workerList);
    std::vector<ScoredPath> getSeedNodes();
    void semanticMultiHopBeamSearch(SharedBuffer &buffer, int numHops, int beamWidth);
    nlohmann::json callRemoteExpansion(int partitionId, const std::vector<ScoredPath> &currentPaths,
                                       std::vector<ScoredPath> &expandedPaths,
                                       vector<std::string> &embeddingRequestsForNewlyExploredEdges, int hop,
                                       SharedBuffer &buffer);
    void getKPaths();
};

#endif  // JASMINEGRAPH_SEMANTICBEAMSEARCH_H
