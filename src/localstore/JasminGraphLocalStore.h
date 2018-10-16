/**
Copyright 2018 JasminGraph Team
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

#ifndef JASMINGRAPH_JASMINGRAOHLOCALSTORE_H
#define JASMINGRAPH_JASMINGRAOHLOCALSTORE_H

#include <map>
#include <unordered_set>

using std::map;
using std::unordered_set;
using std::string;

class JasminGraphLocalStore {
private:
public:
    virtual bool loadGraph() = 0;
    virtual bool storeGraph() = 0;
    virtual map<long, unordered_set<long>> getUnderlyingHashMap() = 0;
    virtual map<long, long> getOutDegreeDistributionHashMap() = 0;
    virtual void initialize() = 0;
    virtual void addVertex(string* attributes) = 0;
    virtual void addEdge(long startVid, long endVid) = 0;
    virtual long getVertexCount() = 0;
    virtual long getEdgeCount() = 0;
};


#endif //JASMINGRAPH_JASMINGRAOHLOCALSTORE_H