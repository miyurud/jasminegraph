//
// Created by sajeenthiran on 2025-08-18.
//

#ifndef JASMINEGRAPH_SEMANTICBEAMSEARCH_H
#define JASMINEGRAPH_SEMANTICBEAMSEARCH_H
#include <iosfwd>
#include <iosfwd>
#include <string>
#include <vector>
#include <vector>
#include <vector>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>

#include "../../../nativestore/NodeManager.h"
#include "../../../server/JasmineGraphServer.h"
#include "../../../vectorStore/FaissStore.h"
#include "../cypher/util/SharedBuffer.h"




namespace faiss
{
    struct Index;
}


struct ScoredPath {
    nlohmann::json pathObj;
    float score;
};
class  SemanticBeamSearch
{

private:
    FaissStore* faissStore;
    const std::vector<float> emb;
    int k;  // Number of top results to return
    GraphConfig gc;
    NodeManager* nodeManager;
    std::vector<JasmineGraphServer::worker> workerList; // List of workers for remote expansion


public:
    SemanticBeamSearch(FaissStore* faissStore, std::vector<float> emb, int k, GraphConfig gc);
    SemanticBeamSearch(FaissStore* faissStore, std::vector<float> emb, int k, GraphConfig gc,
                       vector<JasmineGraphServer::worker> workerList);
    std::vector<ScoredPath> getSeedNodes();
    void semanticMultiHopBeamSearch(SharedBuffer &buffer,
                                                     int numHops,
                                                     int beamWidth);
    nlohmann::json callRemoteExpansion(int partitionId, const std::vector<json>& currentPaths ,  std::vector<ScoredPath>& expandedPaths  );
    void getKPaths();
};


#endif //JASMINEGRAPH_SEMANTICBEAMSEARCH_H