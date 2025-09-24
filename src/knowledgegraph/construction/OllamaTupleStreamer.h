#ifndef OLLAMA_TUPLE_STREAMER_H
#define OLLAMA_TUPLE_STREAMER_H

#include <string>

#include "../../query/processor/cypher/util/SharedBuffer.h"


class OllamaTupleStreamer {
public:
    OllamaTupleStreamer(const std::string& modelName);

    void streamChunk(const std::string& chunkKey,
                     const std::string& chunkText,
                     SharedBuffer& tupleBuffer);

private:
    std::string model;

    struct StreamContext {
        std::string chunkKey;
        SharedBuffer* buffer;
        std::string current_tuple; // persist partial tuple here
        bool isSuccess;

    };

    static size_t StreamCallback(char* ptr, size_t size, size_t nmemb, void* userdata);
};

#endif
