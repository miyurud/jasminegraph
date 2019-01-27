/**
Copyright 2018 JasmineGraph Team
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

#ifndef JASMINEGRAPH_JASMINEGRAPHLOCALSTORE_H
#define JASMINEGRAPH_JASMINEGRAPHLOCALSTORE_H

#include <map>
#include <unordered_set>

using std::map;
using std::unordered_set;
using std::string;

class JasmineGraphLocalStore {
private:
public:
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


#endif //JASMINEGRAPH_JASMINEGRAPHLOCALSTORE_H