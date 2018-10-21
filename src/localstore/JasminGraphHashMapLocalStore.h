//
// Created by chinthaka on 9/19/18.
//

#ifndef JASMINGRAPH_JASMINGRAPHHASHMAPLOCALSTORE_H
#define JASMINGRAPH_JASMINGRAPHHASHMAPLOCALSTORE_H

#include "JasminGraphLocalStore.h"
#include "../util/dbutil/edgestore_generated.h"
#include <flatbuffers/util.h>

using namespace JasminGraph::Edgestore;


class JasminGraphHashMapLocalStore:public JasminGraphLocalStore {
private:
    std::string VERTEX_STORE_NAME = "acacia.nodestore.db";
    std::string EDGE_STORE_NAME = "acacia.edgestore.db";
    std::string ATTRIBUTE_STORE_NAME = "acacia.attributestore.db";

    int graphId;
    int partitionId;
    std::string instanceDataFolderLocation;
    std::map<long,std::unordered_set<long>> localSubGraphMap;

    long vertexCount;
    long edgeCount;


    std::string getFileSeparator();
    void toLocalSubGraphMap(const EdgeStore *edgeStoreData);
public:
    JasminGraphHashMapLocalStore(int graphid, int partitionid);
    JasminGraphHashMapLocalStore(std::string folderLocation);
    inline bool loadGraph() override;
    bool storeGraph() override;
    long getEdgeCount() override;
    long getVertexCount() override;
    void addEdge(long startVid, long endVid) override;
    unordered_set<long> getVertexSet();
    int* getOutDegreeDistribution();
    map<long, long> getOutDegreeDistributionHashMap() override;
    map<long, unordered_set<long>> getUnderlyingHashMap() override;
    void initialize() override;
    void addVertex(string* attributes) override;
};


#endif //JASMINGRAPH_JASMINGRAPHHASHMAPLOCALSTORE_H
