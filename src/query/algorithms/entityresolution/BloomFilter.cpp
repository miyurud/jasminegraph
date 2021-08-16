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

#include "BloomFilter.h"

BloomFilter::BloomFilter(uint64_t size, uint8_t numHashes): m_bits(size), m_numHashes(numHashes) {}

void BloomFilter::insert(std::string &str) {
    int len = str.length();

    if (len < 2) {
        char key = str[0];
        add(&key, 1);
    } else {
        for (int i = 0; i < len - 2; i++) {
            const char *key = str.substr(i, i+2).c_str();
            add(key, 2);
        }
    }
}

void BloomFilter::add(const char *data, std::size_t len) {
    auto hashValues = hash(data, len);

    for (int n = 0; n < m_numHashes; n++) {
        uint64_t pos = nthHash(n, hashValues[0], hashValues[1], m_bits.size());
        m_bits[pos] = true;
    }
}

std::array<uint64_t, 2> BloomFilter::hash(const char *data, std::size_t len) {
    std::array<uint64_t, 2> hashValue;
    MurmurHash3_x64_128(data, len, 0, hashValue.data());

    return hashValue;
}

inline uint64_t BloomFilter::nthHash(uint8_t n, uint64_t hashA, uint64_t hashB, uint64_t filterSize) {
    return (hashA + n * hashB) % filterSize;
}

inline void BloomFilter::reset() {
    m_bits.reset();
}

