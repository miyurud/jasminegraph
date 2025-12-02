/**
Copyright 2025 JasmineGraph Team
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */
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
