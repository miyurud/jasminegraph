/**
Copyright 2025 JasmineGraph Team
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */
#ifndef JASMINEGRAPH_SEMANTICBEAMSEARCH_H
#define JASMINEGRAPH_SEMANTICBEAMSEARCH_H
#include <iosfwd>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>
#include <string>
#include <vector>

#include "../../../../nativestore/NodeManager.h"
#include "../../../../server/JasmineGraphServer.h"
#include "../../../../vectorstore/FaissIndex.h"
#include "../../../../vectorstore/TextEmbedder.h"
#include "../../cypher/util/SharedBuffer.h"

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
