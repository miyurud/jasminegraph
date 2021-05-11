//
// Created by root on 1/29/21.
//

#ifndef JASMINEGRAPH_BLOOMFILTER_HPP
#define JASMINEGRAPH_BLOOMFILTER_HPP

#include <bitset>
#include <array>
#include <cstring>
#include "MurmurHash3.h"

class BloomFilter {
public:
    BloomFilter(uint64_t size, uint8_t numHashes)
            : m_bits(size),
              m_numHashes(numHashes) {}

    void insert(std::string &str) {
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
//
//        for (uint8_t s: str) {
//            add(&s, 1);
//        }
    }

    void add(const char *data, std::size_t len) {
        auto hashValues = hash(data, len);

        for (int n = 0; n < m_numHashes; n++) {
            uint64_t pos = nthHash(n, hashValues[0], hashValues[1], m_bits.size());
//            std::cout << pos << std::endl;
            m_bits[pos] = true;
        }
    }

    std::array<uint64_t, 2> hash(const char *data, std::size_t len) {
        std::array<uint64_t, 2> hashValue;
        MurmurHash3_x64_128(data, len, 0, hashValue.data());

        return hashValue;
    }

    inline uint64_t nthHash(uint8_t n, uint64_t hashA, uint64_t hashB, uint64_t filterSize) {
        return (hashA + n * hashB) % filterSize;
    }

    uint8_t m_numHashes;
    std::bitset<256> m_bits;

    inline void reset() {
        m_bits.reset();
    }
};

#endif //JASMINEGRAPH_BLOOMFILTER_HPP
