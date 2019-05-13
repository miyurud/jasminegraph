//
// Created by chinthaka on 5/11/19.
//

#ifndef JASMINEGRAPH_JASMINEGRAPHHASHMAPCENTRALSTORE_H
#define JASMINEGRAPH_JASMINEGRAPHHASHMAPCENTRALSTORE_H

#include "../localstore/JasmineGraphLocalStore.h"
#include <map>
#include <set>
#include "../util/Utils.h"
#include "../util/dbutil/edgestore_generated.h"
#include "../util/dbutil/attributestore_generated.h"
#include <flatbuffers/util.h>

using std::string;
using namespace JasmineGraph::Edgestore;
using namespace JasmineGraph::AttributeStore;


class JasmineGraphHashMapCentralStore: public JasmineGraphLocalStore {
private:
    string VERTEX_STORE_NAME = "jasminegraph.nodestore.db";
    string CENTRAL_STORE_NAME = "jasminegraph.centralstore.db";
    string ATTRIBUTE_STORE_NAME = "jasminegraph.attributestore.db";

    int graphId = 0;
    int  partitionId = 0;

    string instanceDataFolderLocation;
    std::map<long,unordered_set<long>> localSubgraphMap;

    long vertexCount = 0;
    long edgeCount =0;

    std::string getFileSeparator();

    void toLocalSubGraphMap(const EdgeStore *edgeStoreData);

    void toLocalAttributeMap(const AttributeStore *attributeStoreData);

public:
    JasmineGraphHashMapCentralStore();

    JasmineGraphHashMapCentralStore(int graphId, int partitionId);

    JasmineGraphHashMapCentralStore(std::string folderLocation);

    bool loadGraph();

    bool storeGraph();

    map<long, unordered_set<long>> getUnderlyingHashMap();

    map<long, long> getOutDegreeDistributionHashMap();

    void initialize();

    void addVertex(string *attributes);

    void addEdge(long startVid, long endVid);

    long getVertexCount();

    long getEdgeCount();
};


#endif //JASMINEGRAPH_JASMINEGRAPHHASHMAPCENTRALSTORE_H