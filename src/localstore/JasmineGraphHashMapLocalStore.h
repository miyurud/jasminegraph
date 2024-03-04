/**
Copyright 2019 JasminGraph Team
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

#ifndef JASMINEGRAPH_JASMINEGRAPHHASHMAPLOCALSTORE_H
#define JASMINEGRAPH_JASMINEGRAPHHASHMAPLOCALSTORE_H

#include <flatbuffers/util.h>

#include <fstream>

#include "../util/dbutil/attributestore_generated.h"
#include "../util/dbutil/edgestore_generated.h"
#include "../util/dbutil/partedgemapstore_generated.h"
#include "JasmineGraphLocalStore.h"

using namespace JasmineGraph::Edgestore;
using namespace JasmineGraph::AttributeStore;
using namespace JasmineGraph::PartEdgeMapStore;

class JasmineGraphHashMapLocalStore : public JasmineGraphLocalStore {
 private:
    std::string VERTEX_STORE_NAME = "jasminegraph.nodestore.db";
    std::string EDGE_STORE_NAME = "jasminegraph.edgestore.db";
    std::string ATTRIBUTE_STORE_NAME = "jasminegraph.attributestore";

    int graphId;
    int partitionId;
    std::string instanceDataFolderLocation;
    std::map<long, std::unordered_set<long>> localSubGraphMap;
    std::map<long, std::vector<string>> localAttributeMap;
    std::map<int, std::vector<int>> edgeMap;

    long vertexCount;
    long edgeCount;
    int *distributionArray;

    std::string getFileSeparator();

    void toLocalSubGraphMap(const PartEdgeMapStore *edgeMapStoreData);

    void toLocalAttributeMap(const AttributeStore *attributeStoreData);

 public:
    JasmineGraphHashMapLocalStore(int graphid, int partitionid, std::string folderLocation);

    JasmineGraphHashMapLocalStore(std::string folderLocation);

    JasmineGraphHashMapLocalStore();

    inline bool loadGraph() {
        bool result = false;
        std::string edgeStorePath = instanceDataFolderLocation + getFileSeparator() + std::to_string(graphId) + "_" +
                                    std::to_string(partitionId);

        std::ifstream dbFile;
        dbFile.open(edgeStorePath, std::ios::binary | std::ios::in);

        if (!dbFile.is_open()) {
            return result;
        }

        dbFile.seekg(0, std::ios::end);
        int length = dbFile.tellg();
        dbFile.seekg(0, std::ios::beg);
        char *data = new char[length];
        dbFile.read(data, length);
        dbFile.close();

        auto edgeStoreData = GetPartEdgeMapStore(data);

        toLocalSubGraphMap(edgeStoreData);

        result = true;

        vertexCount = localSubGraphMap.size();
        edgeCount = getEdgeCount();

        return result;
    }

    bool loadAttributes();

    bool storeGraph();

    static bool storeAttributes(const std::map<long, std::vector<string>> &attributeMap, const std::string &storePath);

    long getEdgeCount();

    long getVertexCount();

    void addEdge(long startVid, long endVid);

    unordered_set<long> getVertexSet();

    int *getOutDegreeDistribution();

    map<long, long> getOutDegreeDistributionHashMap();

    map<long, long> getInDegreeDistributionHashMap();

    map<long, unordered_set<long>> getUnderlyingHashMap();

    void initialize();

    void addVertex(string *attributes);

    map<long, std::vector<std::string>> getAttributeHashMap();

    // The following 4 functions are used to serialize partition edge maps before uploading through workers.
    // If this function can be done by existing methods we can remove these.
    void toLocalEdgeMap(const PartEdgeMapStore *edgeMapStoreData);

    bool loadPartEdgeMap(const std::string filePath);

    static bool storePartEdgeMap(const std::map<int, std::vector<int>> &edgeMap, const std::string &savePath);

    map<int, std::vector<int>> getEdgeHashMap(const std::string filePath);
};

#endif  // JASMINEGRAPH_JASMINEGRAPHHASHMAPLOCALSTORE_H
