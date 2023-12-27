//
// Created by ashokkumar on 23/12/23.
//

#ifndef JASMINEGRAPH_STRAMINGTRIANGLES_H
#define JASMINEGRAPH_STRAMINGTRIANGLES_H

#include <algorithm>
#include <chrono>
#include <map>
#include <set>
#include <string>
#include <thread>

#include "../../../util/Conts.h"
#include "../../../localstore/incremental/JasmineGraphIncrementalLocalStore.h"

class JasmineGraphHashMapCentralStore;
class JasmineGraphHashMapDuplicateCentralStore;

class StreamingTriangles {
public:

    static long run(JasmineGraphIncrementalLocalStore *incrementalLocalStoreInstance);

//    static string countCentralStoreTriangles(map<long, unordered_set<long>> centralStore,
//                                             map<long, long> distributionMap, int threadPriority);
};

#endif //JASMINEGRAPH_STRAMINGTRIANGLES_H
