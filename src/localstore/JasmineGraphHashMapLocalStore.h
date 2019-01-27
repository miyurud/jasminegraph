//
// Created by chinthaka on 9/19/18.
//

#ifndef JASMINEGRAPH_JASMINEGRAPHHASHMAPLOCALSTORE_H
#define JASMINEGRAPH_JASMINEGRAPHHASHMAPLOCALSTORE_H

#include "JasmineGraphLocalStore.h"
#include "../util/dbutil/edgestore_generated.h"
#include <flatbuffers/util.h>

using namespace JasmineGraph::Edgestore;


class JasmineGraphHashMapLocalStore : public JasmineGraphLocalStore {
private:
    std::string VERTEX_STORE_NAME = "acacia.nodestore.db";
    std::string EDGE_STORE_NAME = "acacia.edgestore.db";
    std::string ATTRIBUTE_STORE_NAME = "acacia.attributestore.db";

    int graphId;
    int partitionId;
    std::string instanceDataFolderLocation;
    std::map<long, std::unordered_set<long>> localSubGraphMap;

    long vertexCount;
    long edgeCount;
    int *distributionArray;


    std::string getFileSeparator();

    void toLocalSubGraphMap(const EdgeStore *edgeStoreData);

public:
    JasmineGraphHashMapLocalStore(int graphid, int partitionid);

    JasmineGraphHashMapLocalStore(std::string folderLocation);

    inline bool loadGraph();

    bool storeGraph();

    long getEdgeCount();

    long getVertexCount();

    void addEdge(long startVid, long endVid);

    unordered_set<long> getVertexSet();

    int *getOutDegreeDistribution();

    map<long, long> getOutDegreeDistributionHashMap();

    map<long, unordered_set<long>> getUnderlyingHashMap();

    void initialize();

    void addVertex(string *attributes);
};


#endif //JASMINEGRAPH_JASMINEGRAPHHASHMAPLOCALSTORE_H
