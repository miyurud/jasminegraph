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

#ifndef JASMINEGRAPH_JASMINEGRAPHHASHMAPCENTRALSTORE_H
#define JASMINEGRAPH_JASMINEGRAPHHASHMAPCENTRALSTORE_H

#include <flatbuffers/util.h>

#include <map>
#include <set>

#include "../localstore/JasmineGraphLocalStore.h"
#include "../util/Utils.h"
#include "../util/dbutil/attributestore_generated.h"
#include "../util/dbutil/edgestore_generated.h"
#include "../util/dbutil/partedgemapstore_generated.h"

using std::string;
using namespace JasmineGraph::Edgestore;
using namespace JasmineGraph::AttributeStore;
using namespace JasmineGraph::PartEdgeMapStore;

class JasmineGraphHashMapCentralStore : public JasmineGraphLocalStore {
 private:
    string VERTEX_STORE_NAME = "jasminegraph.nodestore.db";
    string CENTRAL_STORE_NAME = "jasminegraph.centralstore.db";
    string ATTRIBUTE_STORE_NAME = "jasminegraph.attributestore.db";

    int graphId = 0;
    int partitionId = 0;

    string instanceDataFolderLocation;
    std::map<long, unordered_set<long>> centralSubgraphMap;

    long vertexCount = 0;
    long edgeCount = 0;

    static std::string getFileSeparator();

    void toLocalSubGraphMap(const PartEdgeMapStore *edgeMapStoreData);

    void toLocalAttributeMap(const AttributeStore *attributeStoreData);

 public:
    JasmineGraphHashMapCentralStore();

    JasmineGraphHashMapCentralStore(int graphId, int partitionId);

    JasmineGraphHashMapCentralStore(std::string folderLocation);

    bool loadGraph();

    bool loadGraph(const std::string &fileName);

    bool storeGraph();

    map<long, unordered_set<long>> getUnderlyingHashMap();

    map<long, long> getOutDegreeDistributionHashMap();

    map<long, long> getInDegreeDistributionHashMap();

    void initialize();

    void addVertex(string *attributes);

    void addEdge(long startVid, long endVid);

    long getVertexCount();

    long getEdgeCount();

    bool storePartEdgeMap(std::map<int, std::vector<int>> edgeMap, const std::string savePath);
};

#endif  // JASMINEGRAPH_JASMINEGRAPHHASHMAPCENTRALSTORE_H
