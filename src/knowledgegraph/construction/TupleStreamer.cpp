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
