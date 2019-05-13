//
// Created by chinthaka on 5/4/19.
//

#include "JasmineGraphLocalStoreFactory.h"

JasmineGraphHashMapLocalStore JasmineGraphLocalStoreFactory::load(std::string graphId, std::string partitionId,
                                                           std::string baseDir) {
    int graphIdentifier = atoi(graphId.c_str());
    int partitionIdentifier = atoi(partitionId.c_str());
    JasmineGraphHashMapLocalStore hashMapLocalStore = *new JasmineGraphHashMapLocalStore(graphIdentifier,partitionIdentifier);

    return hashMapLocalStore;
}