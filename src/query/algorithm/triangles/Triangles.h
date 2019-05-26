//
// Created by chinthaka on 5/4/19.
//

#ifndef JASMINEGRAPH_TRIANGLES_H
#define JASMINEGRAPH_TRIANGLES_H


#include <string>
#include "../../../localstore/JasmineGraphHashMapLocalStore.h"
#include "../../../centralstore/JasmineGraphHashMapCentralStore.h"
#include <map>
#include <set>
#include <algorithm>

class Triangles {
public:
    static long run (JasmineGraphHashMapLocalStore graphDB, JasmineGraphHashMapCentralStore centralStore, std::string hostName);

    static long run (JasmineGraphHashMapLocalStore graphDB, JasmineGraphHashMapCentralStore centralStore, std::string graphId, std::string partitionId);



};


#endif //JASMINEGRAPH_TRIANGLES_H
