/**
Copyright 2021 JasmineGraph Team
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

#ifndef JASMINEGRAPH_BLOOMFILTER_H
#define JASMINEGRAPH_BLOOMFILTER_H

#include <bitset>
#include <array>
#include <cstring>
#include "MurmurHash3.h"

class BloomFilter {
public:
    BloomFilter(uint64_t size, uint8_t numHashes);

    void insert(std::string &str);

    void add(const char *data, std::size_t len);

    std::array<uint64_t, 2> hash(const char *data, std::size_t len);

    inline uint64_t nthHash(uint8_t n, uint64_t hashA, uint64_t hashB, uint64_t filterSize);

    inline void reset();

    uint8_t m_numHashes;
    std::bitset<256> m_bits;

};

#endif //JASMINEGRAPH_BLOOMFILTER_H
