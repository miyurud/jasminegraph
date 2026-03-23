/**
Copyright 2026 JasminGraph Team
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

#ifndef BITMAP_COMPRESSION_H
#define BITMAP_COMPRESSION_H

#include <vector>
#include <cstdint>
#include <algorithm>

/**
 * BitmapCompression - Run-Length Encoding (RLE) for bitmaps
 *
 * Compresses consecutive runs of 0s and 1s to save space.
 *
 * Format: Each uint32_t stores:
 *   - Bits 0-30: Run length (up to 2^31-1)
 *   - Bit 31: Value (0 or 1)
 *
 * Example:
 *   Uncompressed: 1110000111111
 *   Compressed:   [(3,1), (4,0), (6,1)]
 *   Storage: 13 bits → 12 bytes (3 × uint32_t)
 *
 *   But for sparse bitmaps with long runs:
 *   Uncompressed: 11111111111111111111 (20 bits)
 *   Compressed:   [(20,1)]
 *   Storage: 20 bits → 4 bytes (huge saving!)
 */
class BitmapCompression {
 private:
    static const uint32_t RUN_LENGTH_MASK = 0x7FFFFFFF;  // 31 bits for length
    static const uint32_t VALUE_BIT = 0x80000000;  // 1 bit for value

    /**
     * Pack run length and value into uint32_t
     */
    static inline uint32_t packRun(uint32_t length, bool value) {
        return (length & RUN_LENGTH_MASK) | (value ? VALUE_BIT : 0);
    }

    /**
     * Unpack uint32_t into run length and value
     */
    static inline void unpackRun(uint32_t packed, uint32_t& length, bool& value) {
        length = packed & RUN_LENGTH_MASK;
        value = (packed & VALUE_BIT) != 0;
    }

 public:
    /**
     * Compress bitmap using Run-Length Encoding
     * @param bitmap Vector of uint64_t words
     * @param totalBits Total number of significant bits
     * @return Compressed data as vector of uint32_t
     */
    static std::vector<uint32_t> compress(const std::vector<uint64_t>& bitmap,
                                          uint32_t totalBits) {
        std::vector<uint32_t> compressed;

        if (bitmap.empty() || totalBits == 0) {
            return compressed;
        }

        // Track current run
        bool currentValue = false;
        uint32_t runLength = 0;

        // Get first bit to start
        size_t wordIdx = 0;
        uint8_t bitIdx = 0;
        currentValue = (bitmap[0] & 1) != 0;
        runLength = 1;

        // Iterate through all bits
        for (uint32_t i = 1; i < totalBits; ++i) {
            wordIdx = i / 64;
            bitIdx = i % 64;

            bool bit = (bitmap[wordIdx] & (1ULL << bitIdx)) != 0;

            if (bit == currentValue) {
                // Continue current run
                runLength++;

                // Check for overflow (max run length is 2^31-1)
                if (runLength == RUN_LENGTH_MASK) {
                    compressed.push_back(packRun(runLength, currentValue));
                    runLength = 0;
                    currentValue = bit;
                }
            } else {
                // End current run, start new one
                compressed.push_back(packRun(runLength, currentValue));
                currentValue = bit;
                runLength = 1;
            }
        }

        // Add final run
        if (runLength > 0) {
            compressed.push_back(packRun(runLength, currentValue));
        }

        return compressed;
    }

    /**
     * Decompress RLE-encoded bitmap
     * @param compressed Compressed data
     * @param totalBits Total number of bits to reconstruct
     * @return Decompressed bitmap as vector of uint64_t
     */
    static std::vector<uint64_t> decompress(const std::vector<uint32_t>& compressed,
                                           uint32_t totalBits) {
        size_t numWords = (totalBits + 63) / 64;
        std::vector<uint64_t> bitmap(numWords, 0);

        if (compressed.empty() || totalBits == 0) {
            return bitmap;
        }

        uint32_t currentBit = 0;

        for (uint32_t packed : compressed) {
            uint32_t runLength;
            bool value;
            unpackRun(packed, runLength, value);

            // Set bits for this run
            for (uint32_t i = 0; i < runLength && currentBit < totalBits; ++i, ++currentBit) {
                if (value) {
                    size_t wordIdx = currentBit / 64;
                    uint8_t bitIdx = currentBit % 64;
                    bitmap[wordIdx] |= (1ULL << bitIdx);
                }
            }
        }

        return bitmap;
    }

    /**
     * Calculate compression ratio
     * @param originalSize Size in bytes before compression
     * @param compressedSize Size in bytes after compression
     * @return Compression ratio (e.g., 10.5 means 10.5:1)
     */
    static double compressionRatio(size_t originalSize, size_t compressedSize) {
        if (compressedSize == 0) return 0.0;
        return static_cast<double>(originalSize) / static_cast<double>(compressedSize);
    }

    /**
     * Estimate compressed size without actually compressing
     * Useful for deciding whether to compress
     */
    static size_t estimateCompressedSize(const std::vector<uint64_t>& bitmap,
                                         uint32_t totalBits) {
        if (bitmap.empty() || totalBits == 0) {
            return 0;
        }

        uint32_t runCount = 0;
        bool currentValue = (bitmap[0] & 1) != 0;

        for (uint32_t i = 1; i < totalBits; ++i) {
            size_t wordIdx = i / 64;
            uint8_t bitIdx = i % 64;
            bool bit = (bitmap[wordIdx] & (1ULL << bitIdx)) != 0;

            if (bit != currentValue) {
                runCount++;
                currentValue = bit;
            }
        }
        runCount++;  // Final run

        return runCount * sizeof(uint32_t);
    }

    /**
     * Check if compression would be beneficial
     */
    static bool shouldCompress(const std::vector<uint64_t>& bitmap,
                              uint32_t totalBits) {
        size_t originalSize = bitmap.size() * sizeof(uint64_t);
        size_t estimatedSize = estimateCompressedSize(bitmap, totalBits);

        // Only compress if we save at least 25%
        return estimatedSize < (originalSize * 3 / 4);
    }
};

#endif  // BITMAP_COMPRESSION_H
