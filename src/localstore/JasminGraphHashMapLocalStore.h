//
// Created by chinthaka on 9/19/18.
//

#ifndef JASMINGRAPH_JASMINGRAPHHASHMAPLOCALSTORE_H
#define JASMINGRAPH_JASMINGRAPHHASHMAPLOCALSTORE_H

#include "JasminGraphLocalStore.h"
#include "../util/dbutil/edgestore_generated.h"
#include "flatbuffers/util.h"

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
    bool loadGraph();
    bool storeGraph();
    long getEdgeCount();
    long getVertexCount();
    void addEdge(long startVid, long endVid);
    unordered_set<long> getVertexSet();
    int* getOutDegreeDistribution();
    map<long, long> getOutDegreeDistributionHashMap();
};


#endif //JASMINGRAPH_JASMINGRAPHHASHMAPLOCALSTORE_H
