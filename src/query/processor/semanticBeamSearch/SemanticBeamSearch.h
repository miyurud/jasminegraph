//
// Created by sajeenthiran on 2025-08-18.
//

#ifndef JASMINEGRAPH_SEMANTICBEAMSEARCH_H
#define JASMINEGRAPH_SEMANTICBEAMSEARCH_H
#include <string>
#include <vector>

#include "../../../nativestore/NodeManager.h"
#include "../../../vectorStore/FaissStore.h"


namespace faiss
{
    struct Index;
}

class  SemanticBeamSearch
{

private:
    FaissStore* faissStore;
    const std::vector<float> emb;
    int k;  // Number of top results to return
    GraphConfig gc;
public:

    SemanticBeamSearch(FaissStore* faissStore, std::vector<float> emb, int k, GraphConfig gc);
    std::vector<int> getSeedNodes();

};


#endif //JASMINEGRAPH_SEMANTICBEAMSEARCH_H