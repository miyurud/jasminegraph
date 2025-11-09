#ifndef OLLAMA_TUPLE_STREAMER_H
#define OLLAMA_TUPLE_STREAMER_H

#include <string>

#include "../../query/processor/cypher/util/SharedBuffer.h"
#include "TupleStreamer.cpp"

class OllamaTupleStreamer : public TupleStreamer {
 public:
  OllamaTupleStreamer(const std::string& modelName, const std::string& host);

  void streamChunk(const std::string& chunkKey, const std::string& chunkText,
                   SharedBuffer& tupleBuffer);

 private:
  std::string model;
  std::string host;

  struct StreamContext {
    std::string chunkKey;
    SharedBuffer* buffer;
    std::string current_tuple;  // persist partial tuple here
    bool isSuccess;
    int braceDepth = 0;
    int bracketDepth = 0;
  };

  static size_t StreamCallback(char* ptr, size_t size, size_t nmemb,
                               void* userdata);
};

#endif
