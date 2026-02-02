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

#ifndef EDGE_LIFESPAN_BITMAP_H
#define EDGE_LIFESPAN_BITMAP_H

#include <roaring/roaring.h>
#include <vector>
#include <cstdint>
#include <string>
#include <cstring>
#include <memory>

/**
 * EdgeLifespanBitmap - Tracks edge existence across snapshots using Roaring Bitmaps
 * 
 * Roaring Bitmaps provide:
 * - 10-100× space savings for sparse data
 * - Hardware-accelerated bitwise operations (SIMD)
 * - Fast iteration over set bits
 * - Automatic optimization based on data density
 */
class EdgeLifespanBitmap {
private:
    roaring_bitmap_t* bitmap_;  // CRoaring bitmap for efficient storage
    
public:
    EdgeLifespanBitmap() {
        bitmap_ = roaring_bitmap_create();
    }
    
    /**
     * Create bitmap to track N snapshots
     * Roaring bitmaps auto-optimize storage based on density
     */
    explicit EdgeLifespanBitmap(uint32_t totalSnapshots) {
        bitmap_ = roaring_bitmap_create();
    }
    
    ~EdgeLifespanBitmap() {
        if (bitmap_) {
            roaring_bitmap_free(bitmap_);
        }
    }
    
    // Copy constructor
    EdgeLifespanBitmap(const EdgeLifespanBitmap& other) {
        bitmap_ = roaring_bitmap_copy(other.bitmap_);
    }
    
    // Move constructor
    EdgeLifespanBitmap(EdgeLifespanBitmap&& other) noexcept {
        bitmap_ = other.bitmap_;
        other.bitmap_ = nullptr;
    }
    
    // Copy assignment
    EdgeLifespanBitmap& operator=(const EdgeLifespanBitmap& other) {
        if (this != &other) {
            if (bitmap_) roaring_bitmap_free(bitmap_);
            bitmap_ = roaring_bitmap_copy(other.bitmap_);
        }
        return *this;
    }
    
    // Move assignment
    EdgeLifespanBitmap& operator=(EdgeLifespanBitmap&& other) noexcept {
        if (this != &other) {
            if (bitmap_) roaring_bitmap_free(bitmap_);
            bitmap_ = other.bitmap_;
            other.bitmap_ = nullptr;
        }
        return *this;
    }
    
    /**
     * Set bit: Edge exists in snapshot
     * Time complexity: O(log n) but with very fast constant
     */
    void set(uint32_t snapshotId) {
        roaring_bitmap_add(bitmap_, snapshotId);
    }
    
    /**
     * Check if edge exists in a snapshot
     * Time complexity: O(log n) but very fast in practice
     * 
     * Example:
     *   if (bitmap.contains(42)) {
     *       // Edge existed in snapshot 42
     *   }
     */
    bool contains(uint32_t snapshotId) const {
        return roaring_bitmap_contains(bitmap_, snapshotId);
    }
    
    /**
     * Remove bit: Edge doesn't exist in snapshot
     */
    void remove(uint32_t snapshotId) {
        roaring_bitmap_remove(bitmap_, snapshotId);
    }
    
    /**
     * Get all snapshots where this edge exists
     * VERY FAST iteration - only over set bits!
     * 
     * Example:
     *   auto active = bitmap.getActiveSnapshots();
     *   // Returns: [1, 2, 3, 6, 7, 8, 9, 10]
     */
    std::vector<uint32_t> getActiveSnapshots() const {
        uint64_t card = roaring_bitmap_get_cardinality(bitmap_);
        std::vector<uint32_t> result(card);
        roaring_bitmap_to_uint32_array(bitmap_, result.data());
        return result;
    }
    
    /**
     * Count how many snapshots this edge appears in
     * O(1) - cached internally by Roaring!
     * 
     * Example: Edge in 7 snapshots → returns 7 instantly
     */
    uint32_t countActiveSnapshots() const {
        return static_cast<uint32_t>(roaring_bitmap_get_cardinality(bitmap_));
    }
    
    /**
     * Get cardinality (count of active snapshots)
     * Alias for countActiveSnapshots() for standard API compatibility
     */
    uint64_t cardinality() const {
        return roaring_bitmap_get_cardinality(bitmap_);
    }
    
    /**
     * Bitwise AND - HARDWARE ACCELERATED with SIMD!
     * Returns new bitmap with intersection
     * 
     * Example: Find snapshots where BOTH edges exist
     *   auto common = bitmap1.intersect(bitmap2);
     */
    EdgeLifespanBitmap intersect(const EdgeLifespanBitmap& other) const {
        EdgeLifespanBitmap result;
        roaring_bitmap_free(result.bitmap_);
        result.bitmap_ = roaring_bitmap_and(bitmap_, other.bitmap_);
        return result;
    }
    
    /**
     * Bitwise OR - Union of snapshots
     */
    EdgeLifespanBitmap unionWith(const EdgeLifespanBitmap& other) const {
        EdgeLifespanBitmap result;
        roaring_bitmap_free(result.bitmap_);
        result.bitmap_ = roaring_bitmap_or(bitmap_, other.bitmap_);
        return result;
    }
    
    /**
     * Bitwise XOR - Snapshots in either but not both
     */
    EdgeLifespanBitmap difference(const EdgeLifespanBitmap& other) const {
        EdgeLifespanBitmap result;
        roaring_bitmap_free(result.bitmap_);
        result.bitmap_ = roaring_bitmap_xor(bitmap_, other.bitmap_);
        return result;
    }
    
    /**
     * Check if any bit is set in range [start, end]
     * Much faster than iterating!
     */
    bool intersectsRange(uint32_t start, uint32_t end) const {
        return roaring_bitmap_range_cardinality(bitmap_, start, end + 1) > 0;
    }
    
    /**
     * Count bits in range [start, end]
     */
    uint64_t countInRange(uint32_t start, uint32_t end) const {
        return roaring_bitmap_range_cardinality(bitmap_, start, end + 1);
    }
    
    /**
     * Serialize bitmap to binary string for storage
     * Roaring uses portable format (works across architectures!)
     */
    std::string serialize() const {
        size_t size = roaring_bitmap_portable_size_in_bytes(bitmap_);
        std::string buffer(size, '\0');
        roaring_bitmap_portable_serialize(bitmap_, buffer.data());
        return buffer;
    }
    
    /**
     * Deserialize bitmap from storage
     */
    static EdgeLifespanBitmap deserialize(const std::string& data) {
        EdgeLifespanBitmap bitmap;
        roaring_bitmap_free(bitmap.bitmap_);
        bitmap.bitmap_ = roaring_bitmap_portable_deserialize(data.data());
        return bitmap;
    }
    
    /**
     * Get memory size in bytes
     * Roaring compression can save 10-100× space!
     */
    size_t getSizeBytes() const {
        return roaring_bitmap_portable_size_in_bytes(bitmap_);
    }
    
    /**
     * Optimize storage (call after bulk additions)
     * Converts to most efficient container type automatically
     */
    void optimize() {
        roaring_bitmap_run_optimize(bitmap_);
    }
    
    /**
     * Clear all bits
     */
    void clear() {
        roaring_bitmap_clear(bitmap_);
    }
    
    /**
     * Check if bitmap is empty
     */
    bool isEmpty() const {
        return roaring_bitmap_is_empty(bitmap_);
    }
    
    /**
     * Get internal Roaring bitmap pointer (for advanced operations)
     */
    roaring_bitmap_t* getBitmap() const {
        return bitmap_;
    }
    
    // Backward compatibility methods
    void setBit(uint32_t snapshotId, bool value) {
        if (value) {
            set(snapshotId);
        } else {
            remove(snapshotId);
        }
    }
    
    bool getBit(uint32_t snapshotId) const {
        return contains(snapshotId);
    }
    
    uint32_t getTotalSnapshots() const {
        if (isEmpty()) return 0;
        return roaring_bitmap_maximum(bitmap_) + 1;
    }
};

#endif // EDGE_LIFESPAN_BITMAP_H
