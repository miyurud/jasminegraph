//
// Created by chinthaka on 5/4/19.
//

#ifndef JASMINEGRAPH_JASMINEGRAPHLOCALSTOREFACTORY_H
#define JASMINEGRAPH_JASMINEGRAPHLOCALSTOREFACTORY_H

#include "JasmineGraphHashMapLocalStore.h"


class JasmineGraphLocalStoreFactory {
public:
    static JasmineGraphHashMapLocalStore load (std::string graphId, std::string partitionId, std::string baseDir);

};


#endif //JASMINEGRAPH_JASMINEGRAPHLOCALSTOREFACTORY_H
