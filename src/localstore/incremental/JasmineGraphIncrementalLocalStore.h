#include <string>
#include "../../centralstore/incremental/NodeManager.h"
#ifndef Incremental_LocalStore
#define Incremental_LocalStore


class JasmineGraphIncrementalLocalStore
{
   public:
    GraphConfig gc;
    NodeManager *nm;
    std::string addEdgeFromString(std::string edgeString);
    JasmineGraphIncrementalLocalStore(unsigned int graphID = 0, unsigned int partitionID = 0 );
};

#endif
