//
// Created by chinthaka on 5/4/19.
//

#ifndef JASMINEGRAPH_TRIANGLES_H
#define JASMINEGRAPH_TRIANGLES_H


#include <string>
#include "../../../localstore/JasmineGraphHashMapLocalStore.h"
#include <map>
#include <set>
#include <algorithm>

class Triangles {
public:
    static std::string run (JasmineGraphHashMapLocalStore graphDB, std::string hostName);

    static std::string run (JasmineGraphHashMapLocalStore graphDB, std::string graphId, std::string partitionId, std::string serverHostName);



};


#endif //JASMINEGRAPH_TRIANGLES_H
