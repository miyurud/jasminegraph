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

#ifndef TEMPORAL_STORE_H
#define TEMPORAL_STORE_H

#include <map>
#include <unordered_map>
#include <functional>
#include <string>
#include <vector>
#include <memory>
#include <mutex>
#include "EdgeLifespanBitmap.h"
#include "PropertyIntervalDictionary.h"
#include "SnapshotManager.h"
#include "TemporalStorePersistence.h"

/**
 * TemporalStore - Main class for temporal graph storage
 * 
 * Stores historical graph snapshots using:
 * - EdgeLifespanBitmap: Track edge existence across snapshots
 * - PropertyIntervalDictionary: Track property changes over time
 * - SnapshotManager: Manage snapshot lifecycle
 * 
 * Example Usage:
 *   TemporalStore store(graphId, partitionId);
 *   
 *   // Add edges
 *   store.addEdge("Alice", "Bob", currentSnapshot);
 *   store.addEdge("Bob", "Charlie", currentSnapshot);
 *   
 *   // Update property
 *   store.updateNodeProperty("Alice", "city", "NYC", currentSnapshot);
 *   
 *   // Query historical state
 *   bool existed = store.edgeExistsAtSnapshot("Alice", "Bob", 42);
 *   string city = store.getPropertyAtSnapshot("Alice", "city", 42);
 */
class TemporalStore {
public:
    struct EdgeKey {
        std::string sourceId;
        std::string destId;
        
        EdgeKey(const std::string& src, const std::string& dst)
            : sourceId(src), destId(dst) {}
        
        bool operator<(const EdgeKey& other) const {
            if (sourceId != other.sourceId) return sourceId < other.sourceId;
            return destId < other.destId;
        }
        
        bool operator==(const EdgeKey& other) const {
            return sourceId == other.sourceId && destId == other.destId;
        }
        
        struct Hash {
            size_t operator()(const EdgeKey& k) const noexcept {
                size_t h1 = std::hash<std::string>{}(k.sourceId);
                size_t h2 = std::hash<std::string>{}(k.destId);
                return h1 ^ (h2 * 2654435761ULL);  // FNV-inspired mixing
            }
        };
        
        std::string toString() const {
            return sourceId + "->" + destId;
        }
    };

private:
    uint32_t graphId_;
    uint32_t partitionId_;
    
    // Core data structures
    std::unordered_map<EdgeKey, EdgeLifespanBitmap, EdgeKey::Hash> edgeBitmaps_;
    std::map<std::string, PropertyIntervalDictionary> nodeProperties_;
    std::unordered_map<EdgeKey, PropertyIntervalDictionary, EdgeKey::Hash> edgeProperties_;
    
    // Snapshot management
    std::unique_ptr<SnapshotManager> snapshotManager_;
    
    // Thread safety
    std::mutex mutex_;
    
    // Statistics
    uint64_t totalEdgesTracked_;
    uint64_t totalNodesWithProperties_;

public:
    /**
     * Constructor
     */
    TemporalStore(uint32_t graphId, 
                 uint32_t partitionId,
                 uint64_t timeThreshold = 60,
                 uint64_t edgeThreshold = 10000,
                 SnapshotManager::SnapshotMode mode = SnapshotManager::SnapshotMode::HYBRID)
        : graphId_(graphId),
          partitionId_(partitionId),
          totalEdgesTracked_(0),
          totalNodesWithProperties_(0),
          autoSaveEnabled_(false),
          autoSaveCompress_(true) {
        
        snapshotManager_ = std::make_unique<SnapshotManager>(
            graphId, partitionId, timeThreshold, edgeThreshold, mode);
    }
    
    /**
     * Add or update an edge in the temporal store
     * @param sourceId Source node ID
     * @param destId Destination node ID
     * @param snapshotId Current snapshot ID
     * @return true if edge was newly added, false if it already existed
     * @throws std::bad_alloc if memory allocation fails
     */
    bool addEdge(const std::string& sourceId, 
                const std::string& destId,
                uint32_t snapshotId) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        EdgeKey key(sourceId, destId);
        
        // Check if edge already exists
        auto it = edgeBitmaps_.find(key);
        if (it == edgeBitmaps_.end()) {
            try {
                // New edge - create bitmap
                EdgeLifespanBitmap bitmap(snapshotId + 100);  // Allocate extra space
                bitmap.setBit(snapshotId, true);
                edgeBitmaps_[key] = bitmap;
                totalEdgesTracked_++;
                
                // Record in snapshot manager
                snapshotManager_->recordEdge();
                return true;
            } catch (const std::bad_alloc& e) {
                // Memory allocation failed - re-throw with context
                throw std::bad_alloc();
            }
        } else {
            // Existing edge - update bitmap
            it->second.setBit(snapshotId, true);
            snapshotManager_->recordEdge();
            return false;
        }
    }
    
    /**
     * Mark an edge as deleted (set bit to 0)
     */
    void removeEdge(const std::string& sourceId,
                   const std::string& destId,
                   uint32_t snapshotId) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        EdgeKey key(sourceId, destId);
        auto it = edgeBitmaps_.find(key);
        if (it != edgeBitmaps_.end()) {
            it->second.setBit(snapshotId, false);
        }
    }
    
    /**
     * Check if edge exists at a specific snapshot
     */
    bool edgeExistsAtSnapshot(const std::string& sourceId,
                             const std::string& destId,
                             uint32_t snapshotId) const {
        std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(mutex_));
        
        EdgeKey key(sourceId, destId);
        auto it = edgeBitmaps_.find(key);
        if (it != edgeBitmaps_.end()) {
            return it->second.intersectsRange(0, snapshotId);  // lazy inheritance: edge present in [firstSnap, query]
        }
        return false;
    }
    
    /**
     * Update node property
     */
    void updateNodeProperty(const std::string& nodeId,
                           const std::string& propertyKey,
                           const std::string& propertyValue,
                           uint32_t snapshotId) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        auto& propDict = nodeProperties_[nodeId];
        propDict.addOrUpdateProperty(propertyKey, propertyValue, snapshotId);
        
        if (nodeProperties_.size() > totalNodesWithProperties_) {
            totalNodesWithProperties_ = nodeProperties_.size();
        }
    }
    
    /**
     * Update edge property
     */
    void updateEdgeProperty(const std::string& sourceId,
                           const std::string& destId,
                           const std::string& propertyKey,
                           const std::string& propertyValue,
                           uint32_t snapshotId) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        EdgeKey key(sourceId, destId);
        auto& propDict = edgeProperties_[key];
        propDict.addOrUpdateProperty(propertyKey, propertyValue, snapshotId);
    }
    
    /**
     * Get node property value at specific snapshot
     */
    std::string getNodePropertyAtSnapshot(const std::string& nodeId,
                                         const std::string& propertyKey,
                                         uint32_t snapshotId) const {
        std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(mutex_));
        
        auto it = nodeProperties_.find(nodeId);
        if (it != nodeProperties_.end()) {
            return it->second.getValueAtSnapshot(propertyKey, snapshotId);
        }
        return "";
    }
    
    /**
     * Get edge property value at specific snapshot
     */
    std::string getEdgePropertyAtSnapshot(const std::string& sourceId,
                                         const std::string& destId,
                                         const std::string& propertyKey,
                                         uint32_t snapshotId) const {
        std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(mutex_));
        
        EdgeKey key(sourceId, destId);
        auto it = edgeProperties_.find(key);
        if (it != edgeProperties_.end()) {
            return it->second.getValueAtSnapshot(propertyKey, snapshotId);
        }
        return "";
    }
    
    /**
     * Get all edges that exist at a specific snapshot
     */
    std::vector<EdgeKey> getEdgesAtSnapshot(uint32_t snapshotId) const {
        std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(mutex_));
        
        std::vector<EdgeKey> edges;
        for (const auto& [key, bitmap] : edgeBitmaps_) {
            if (bitmap.intersectsRange(0, snapshotId)) {  // lazy inheritance
                edges.push_back(key);
            }
        }
        return edges;
    }
    
    /**
     * Count triangles at specific snapshot using optimized Roaring bitmap operations
     * 
     * Algorithm: For each edge (u,v), compute neighbors(u) ∩ neighbors(v)
     * The cardinality gives the number of common neighbors = triangles with edge (u,v)
     * Uses AVX2 SIMD instructions (vpand) for parallel bitmap intersection
     * 
     * @param snapshotId Snapshot to count triangles in
     * @return Number of triangles
     */
    uint64_t countTrianglesAtSnapshot(uint32_t snapshotId) const {
        std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(mutex_));
        
        // Step 1: Create bidirectional mapping: string nodeId ↔ uint32_t index
        std::map<std::string, uint32_t> nodeToIndex;
        std::vector<std::string> indexToNode;
        uint32_t nextIndex = 0;
        
        // Collect all edges at this snapshot and build node index
        std::vector<EdgeKey> edges;
        for (const auto& [key, bitmap] : edgeBitmaps_) {
            if (bitmap.intersectsRange(0, snapshotId)) {  // lazy inheritance
                edges.push_back(key);
                
                if (nodeToIndex.find(key.sourceId) == nodeToIndex.end()) {
                    nodeToIndex[key.sourceId] = nextIndex++;
                    indexToNode.push_back(key.sourceId);
                }
                if (nodeToIndex.find(key.destId) == nodeToIndex.end()) {
                    nodeToIndex[key.destId] = nextIndex++;
                    indexToNode.push_back(key.destId);
                }
            }
        }
        
        if (edges.empty()) return 0;
        
        // Step 2: Build adjacency using Roaring bitmaps with integer node indices
        std::map<uint32_t, roaring_bitmap_t*> neighbors;
        for (uint32_t i = 0; i < nextIndex; i++) {
            neighbors[i] = roaring_bitmap_create();
        }
        
        for (const auto& edge : edges) {
            uint32_t u = nodeToIndex[edge.sourceId];
            uint32_t v = nodeToIndex[edge.destId];
            
            // Undirected graph: add both directions
            roaring_bitmap_add(neighbors[u], v);
            roaring_bitmap_add(neighbors[v], u);
        }
        
        // Step 3: Count triangles using SIMD-optimized bitmap intersection
        uint64_t triangleCount = 0;
        
        for (const auto& edge : edges) {
            uint32_t u = nodeToIndex[edge.sourceId];
            uint32_t v = nodeToIndex[edge.destId];
            
            if (u > v) continue;  // Process each edge only once
            
            // Compute intersection: neighbors(u) ∩ neighbors(v)
            // roaring_bitmap_and() uses AVX2 SIMD instructions for parallel processing:
            //   vmovdqa ymm0, [u_bitmap]  ; Load 256 bits
            //   vpand ymm0, ymm0, [v_bitmap]  ; Parallel AND
            roaring_bitmap_t* intersection = roaring_bitmap_and(neighbors[u], neighbors[v]);
            
            // Cardinality = number of common neighbors = triangles containing edge (u,v)
            triangleCount += roaring_bitmap_get_cardinality(intersection);
            
            roaring_bitmap_free(intersection);
        }
        
        // Cleanup
        for (auto& [idx, bitmap] : neighbors) {
            roaring_bitmap_free(bitmap);
        }
        
        // Each triangle counted 3 times (once per edge), divide by 3
        return triangleCount / 3;
    }
    
    /**
     * Count triangles across a range of snapshots
     * Uses Roaring bitmap operations for maximum efficiency
     * 
     * @param startSnapshot First snapshot in range
     * @param endSnapshot Last snapshot in range
     * @return Map of snapshot ID to triangle count
     */
    std::map<uint32_t, uint64_t> countTrianglesInRange(
        uint32_t startSnapshot, 
        uint32_t endSnapshot) const {
        
        std::map<uint32_t, uint64_t> results;
        
        // For each snapshot in range, count triangles
        for (uint32_t snapId = startSnapshot; snapId <= endSnapshot; ++snapId) {
            results[snapId] = countTrianglesAtSnapshot(snapId);
        }
        
        return results;
    }
    
    /**
     * Check if snapshot should be created
     */
    bool shouldCreateSnapshot() const {
        return snapshotManager_->shouldCreateSnapshot();
    }
    
    /**
     * Close current snapshot
     */
    uint32_t closeCurrentSnapshot() {
        return snapshotManager_->closeCurrentSnapshot();
    }
    
    /**
     * Open new snapshot
     * Inherits all edges from previous snapshot (cumulative semantics)
     */
    uint32_t openNewSnapshot() {
        std::lock_guard<std::mutex> lock(mutex_);
        
        uint32_t newSnapshotId = snapshotManager_->openNewSnapshot();
        
        // LAZY INHERITANCE: No need to iterate all edges to copy bits.
        // edgeExistsAtSnapshot / getEdgesAtSnapshot now use intersectsRange(0, snapshotId)
        // so any edge added in snapshot N is implicitly visible in all snapshots >= N.
        // This turns a blocking O(N_edges) iteration into O(1).
        
        return newSnapshotId;
    }
    
    /**
     * Get current snapshot ID
     */
    uint32_t getCurrentSnapshotId() const {
        return snapshotManager_->getCurrentSnapshotId();
    }
    
    /**
     * Get statistics
     */
    struct Stats {
        uint64_t totalEdgesTracked;
        uint64_t totalNodesWithProperties;
        uint32_t currentSnapshotId;
        uint64_t currentSnapshotEdgeCount;
        size_t memoryUsageBytes;
    };
    
    Stats getStats() const {
        std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(mutex_));
        
        Stats stats;
        stats.totalEdgesTracked = totalEdgesTracked_;
        stats.totalNodesWithProperties = totalNodesWithProperties_;
        stats.currentSnapshotId = snapshotManager_->getCurrentSnapshotId();
        stats.currentSnapshotEdgeCount = snapshotManager_->getCurrentEdgeCount();
        
        // Estimate memory usage
        stats.memoryUsageBytes = 0;
        for (const auto& [key, bitmap] : edgeBitmaps_) {
            stats.memoryUsageBytes += bitmap.getSizeBytes();
            stats.memoryUsageBytes += key.sourceId.size() + key.destId.size();
        }
        
        return stats;
    }
    
    /**
     * Get graph ID
     */
    uint32_t getGraphId() const {
        return graphId_;
    }
    
    /**
     * Get partition ID
     */
    uint32_t getPartitionId() const {
        return partitionId_;
    }
    
    /**
     * Get snapshot manager (for external configuration)
     */
    SnapshotManager* getSnapshotManager() {
        return snapshotManager_.get();
    }
    
    /**
     * Save current state to disk
     * Returns true if successful
     */
    bool saveSnapshotToDisk(const std::string& baseDir, bool compress = true) {
        // Phase 1: copy shared state under lock (fast — in-memory copy)
        // Phase 2: serialize to disk WITHOUT holding the lock (expensive I/O)
        // This reduces the lock-hold from ~2 s (I/O) to ~tens of ms (copy).
        uint32_t snapshotId;
        std::string filePath;
        decltype(edgeBitmaps_)      bitmapsCopy;
        decltype(nodeProperties_)   nodesCopy;
        decltype(edgeProperties_)   edgePropsCopy;

        {
            std::lock_guard<std::mutex> lock(mutex_);
            snapshotId = snapshotManager_->getCurrentSnapshotId();
            filePath   = TemporalStorePersistence::generateFilePath(
                baseDir, graphId_, partitionId_, snapshotId);
            bitmapsCopy   = edgeBitmaps_;
            nodesCopy     = nodeProperties_;
            edgePropsCopy = edgeProperties_;
        }  // lock released here — producers can continue immediately

        return TemporalStorePersistence::saveSnapshot(
            filePath, graphId_, partitionId_, snapshotId,
            bitmapsCopy, nodesCopy, edgePropsCopy, compress);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Bitmap-index persistence  (replaces per-snapshot full dumps)
    //
    //  graph{G}_part{P}_bitmaps.ebm  — single file, rewritten on each snapshot
    //                                   close; contains ALL edges + their full
    //                                   Roaring bitmaps, Roaring-compressed.
    //  graph{G}_part{P}_snapmeta.bin — append-only; one 32-byte record per
    //                                   closed snapshot (ID, counts, timestamp).
    //
    //  At billion scale this reduces disk from O(N²) (current) to O(N):
    //    snap0..snap20 part2 today:  2.9 GB  → ~150 MB with this design
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Rewrite graph{G}_part{P}_bitmaps.ebm with the current edgeBitmaps_ map.
     * After a successful write, evict from RAM all edges that were NOT active
     * in closedSnapshotId (keeping only edges still live for the next snapshot).
     */
    bool saveBitmapIndexToDisk(const std::string& baseDir, uint32_t closedSnapshotId) {
        std::string filePath = TemporalStorePersistence::generateBitmapFilePath(
            baseDir, graphId_, partitionId_);

        // Phase 1: copy under lock (fast — in-memory copy)
        decltype(edgeBitmaps_) bitmapsCopy;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            bitmapsCopy = edgeBitmaps_;
        }

        // Write the full cumulative map to disk.
        // NOTE: Do NOT evict from edgeBitmaps_ here.
        // Lazy-inheritance semantics mean edge "A→B" added at snapshot 0 only has
        // bit 0 set — it will not have bit N set just because it persists to snapshot N.
        // Evicting on !getBit(closedSnapshotId) would therefore delete all edges from
        // previous snapshots, leaving each .ebm file with only the current batch
        // (~100K edges) instead of the full cumulative history.
        // The in-memory map IS the cumulative store; disk is just a checkpoint of it.
        return TemporalStorePersistence::saveBitmapIndex(
            filePath, graphId_, partitionId_, closedSnapshotId, bitmapsCopy);
    }

    /**
     * Append one record to graph{G}_part{P}_snapmeta.bin.
     * newEdgesInSnapshot is the count of edges first seen in closedSnapshotId.
     */
    bool appendSnapshotMetaToDisk(const std::string& baseDir,
                                  uint32_t closedSnapshotId,
                                  uint64_t newEdgesInSnapshot = 0) {
        uint64_t totalEdges;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            totalEdges = edgeBitmaps_.size();
        }
        std::string metaPath = TemporalStorePersistence::generateMetaFilePath(
            baseDir, graphId_, partitionId_);
        return TemporalStorePersistence::appendSnapshotMeta(
            metaPath, graphId_, partitionId_,
            closedSnapshotId, totalEdges, newEdgesInSnapshot);
    }

    /**
     * Load graph{G}_part{P}_bitmaps.ebm and restore edgeBitmaps_ + snapshotId.
     */
    bool loadBitmapIndexFromDisk(const std::string& filePath) {
        uint32_t graphId, partitionId, latestSnapshotId;
        std::unordered_map<EdgeKey, EdgeLifespanBitmap, EdgeKey::Hash> edgeBitmaps;

        bool success = TemporalStorePersistence::loadBitmapIndex(
            filePath, graphId, partitionId, latestSnapshotId, edgeBitmaps);
        if (!success) return false;
        if (graphId != graphId_ || partitionId != partitionId_) return false;

        std::lock_guard<std::mutex> lock(mutex_);
        edgeBitmaps_        = std::move(edgeBitmaps);
        totalEdgesTracked_  = edgeBitmaps_.size();
        snapshotManager_->setCurrentSnapshotId(latestSnapshotId);
        return true;
    }

    /**
     * Load snapshot from disk (legacy .tgs format — kept for backward compat)
     * Returns true if successful
     */
    bool loadSnapshotFromDisk(const std::string& filePath) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        uint32_t graphId, partitionId, snapshotId;
        std::unordered_map<EdgeKey, EdgeLifespanBitmap, EdgeKey::Hash> edgeBitmaps;
        std::map<std::string, PropertyIntervalDictionary> nodeProps;
        std::unordered_map<EdgeKey, PropertyIntervalDictionary, EdgeKey::Hash> edgeProps;
        
        bool success = TemporalStorePersistence::loadSnapshot(
            filePath, graphId, partitionId, snapshotId,
            edgeBitmaps, nodeProps, edgeProps);
        
        if (!success) {
            return false;
        }
        
        // Verify graph/partition match
        if (graphId != graphId_ || partitionId != partitionId_) {
            return false;
        }
        
        // Merge loaded data
        edgeBitmaps_ = edgeBitmaps;
        nodeProperties_ = nodeProps;
        edgeProperties_ = edgeProps;
        totalEdgesTracked_ = edgeBitmaps.size();
        totalNodesWithProperties_ = nodeProps.size();
        
        return true;
    }
    
    /**
     * Auto-save on snapshot close
     */
    void enableAutoSave(const std::string& baseDir, bool compress = true) {
        std::lock_guard<std::mutex> lock(mutex_);
        autoSaveEnabled_ = true;
        autoSaveBaseDir_ = baseDir;
        autoSaveCompress_ = compress;
    }
    
    void disableAutoSave() {
        std::lock_guard<std::mutex> lock(mutex_);
        autoSaveEnabled_ = false;
    }

private:
    // Auto-save settings
    bool autoSaveEnabled_;
    std::string autoSaveBaseDir_;
    bool autoSaveCompress_;
    
    /**
     * Internal method to handle snapshot closure
     */
    void onSnapshotClose(uint32_t closedSnapshotId) {
        if (autoSaveEnabled_) {
            saveSnapshotToDisk(autoSaveBaseDir_, autoSaveCompress_);
        }
    }
};

#endif // TEMPORAL_STORE_H

