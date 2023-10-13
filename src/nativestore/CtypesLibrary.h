//
// Created by sandaruwan on 4/9/23.
//

#ifndef JASMINEGRAPH_CTYPESLIBRARY_H
#define JASMINEGRAPH_CTYPESLIBRARY_H

#endif //JASMINEGRAPH_CTYPESLIBRARY_H

#include <string>
#include <unordered_map>
#include "NodeManager.h"


class CtypesLibrary{
public:
    std::unordered_map<std::string,  NodeManager*> nodeManagerIndex;
    void get_node_data();

};