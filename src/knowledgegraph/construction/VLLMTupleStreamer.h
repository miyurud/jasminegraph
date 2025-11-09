#pragma once

#include <mutex>
#include <stack>
#include <string>

#include "../../query/processor/cypher/util/SharedBuffer.h"
#include "TupleStreamer.cpp"

// ---------------- Stream Context ----------------
struct StreamContext {
  std::string chunkKey;       // key/ID for this chunk
  SharedBuffer* buffer;       // shared buffer to push tuples
  std::string current_tuple;  // accumulate partial tuple text
  bool isSuccess;             // track success/failure
  std::stack<char> braceStack;
  int braceDepth = 0;
};

// ---------------- VLLMTupleStreamer ----------------
class VLLMTupleStreamer : public TupleStreamer {
 public:
  // Constructor
  VLLMTupleStreamer(const std::string& modelName, const std::string& host);

  // Public method to stream a chunk of text into tuples
  void streamChunk(const std::string& chunkKey, const std::string& chunkText,
                   SharedBuffer& tupleBuffer);
  void processChunk(const std::string& chunkKey, const std::string& chunkText,
                    SharedBuffer& tupleBuffer);

 private:
  std::string model;  // model name (e.g., "meta-llama/Llama-3.2-3B-Instruct")
  std::string host;   // VLLM server host (e.g., "http://127.0.0.1:6578")

  // Callback for CURL streaming
  static size_t StreamCallback(char* ptr, size_t size, size_t nmemb,
                               void* userdata);
};
