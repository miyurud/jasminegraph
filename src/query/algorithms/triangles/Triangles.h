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

#ifndef JASMINEGRAPH_TRIANGLES_H
#define JASMINEGRAPH_TRIANGLES_H

#include <algorithm>
#include <chrono>
#include <map>
#include <set>
#include <string>
#include <thread>

#include "../../../centralstore/JasmineGraphHashMapCentralStore.h"
#include "../../../centralstore/JasmineGraphHashMapDuplicateCentralStore.h"
#include "../../../localstore/JasmineGraphHashMapLocalStore.h"
#include "../../../util/Conts.h"

// Helper structure to hold the result
struct TriangleResult {
    std::string triangles;
    long count;
};

class JasmineGraphHashMapCentralStore;
class JasmineGraphHashMapDuplicateCentralStore;

class Triangles {
 public:
    static long run(JasmineGraphHashMapLocalStore graphDB, JasmineGraphHashMapCentralStore centralStore,
                    JasmineGraphHashMapDuplicateCentralStore duplicateCentralStore, std::string hostName);

    static long run(JasmineGraphHashMapLocalStore graphDB, JasmineGraphHashMapCentralStore centralStore,
                    JasmineGraphHashMapDuplicateCentralStore duplicateCentralStore, std::string graphId,
                    std::string partitionId, int threadPriority);

    static TriangleResult countTriangles(map<long, unordered_set<long>> &centralStore,
                                             map<long, long> &distributionMap, bool returnTriangles);
};

#endif  // JASMINEGRAPH_TRIANGLES_H
