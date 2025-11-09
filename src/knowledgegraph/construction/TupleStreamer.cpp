#ifndef TUPLE_STREAMER_H
#define TUPLE_STREAMER_H

#include <string>

#include "../../query/processor/cypher/util/SharedBuffer.h"

class TupleStreamer {
 public:
    virtual ~TupleStreamer() = default;

    virtual void streamChunk(const std::string& chunkKey, const std::string& chunkText, SharedBuffer& tupleBuffer) = 0;
};
#endif
