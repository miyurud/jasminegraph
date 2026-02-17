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

#ifndef SNAPSHOT_MANAGER_H
#define SNAPSHOT_MANAGER_H

#include <chrono>
#include <cstdint>
#include <string>
#include <mutex>

/**
 * SnapshotManager - Manages snapshot lifecycle and triggers
 * 
 * Responsibilities:
 * - Track current snapshot ID
 * - Determine when to close current snapshot and open new one
 * - Support multiple snapshot strategies (time-based, count-based, hybrid, adaptive)
 * 
 * Example:
 *   SnapshotManager mgr(60, 10000, "hybrid");  // 60 sec OR 10K edges
 *   
 *   while (streaming) {
 *       mgr.recordEdge();
 *       if (mgr.shouldCreateSnapshot()) {
 *           mgr.closeCurrentSnapshot();
 *           mgr.openNewSnapshot();
 *       }
 *   }
 */
class SnapshotManager {
public:
    enum class SnapshotMode {
        TIME_BASED,    // Create snapshot every T seconds
        COUNT_BASED,   // Create snapshot every K edges
        HYBRID,        // Create when (time >= T) OR (edges >= K)
        ADAPTIVE       // Calculate K based on system metrics
    };

private:
    uint32_t currentSnapshotId_;
    uint32_t graphId_;
    uint32_t partitionId_;
    
    // Snapshot triggers
    uint64_t currentEdgeCount_;
    std::chrono::time_point<std::chrono::system_clock> snapshotStartTime_;
    
    // Configuration
    SnapshotMode mode_;
    uint64_t timeThresholdSeconds_;
    uint64_t edgeCountThreshold_;
    
    // Thread safety
    std::mutex mutex_;
    
    // Helper to check if time threshold reached
    bool isTimeThresholdReached() const {
        auto now = std::chrono::system_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
            now - snapshotStartTime_).count();
        return elapsed >= timeThresholdSeconds_;
    }
    
    // Helper to check if edge count threshold reached
    bool isEdgeCountThresholdReached() const {
        return currentEdgeCount_ >= edgeCountThreshold_;
    }

public:
    /**
     * Constructor
     * @param graphId Graph identifier
     * @param partitionId Partition identifier
     * @param timeThreshold Seconds between snapshots (for time-based/hybrid)
     * @param edgeThreshold Edges per snapshot (for count-based/hybrid)
     * @param mode Snapshot creation strategy
     */
    SnapshotManager(uint32_t graphId, 
                   uint32_t partitionId,
                   uint64_t timeThreshold = 60,
                   uint64_t edgeThreshold = 10000,
                   SnapshotMode mode = SnapshotMode::HYBRID)
        : currentSnapshotId_(0),
          graphId_(graphId),
          partitionId_(partitionId),
          currentEdgeCount_(0),
          snapshotStartTime_(std::chrono::system_clock::now()),
          mode_(mode),
          timeThresholdSeconds_(timeThreshold),
          edgeCountThreshold_(edgeThreshold) {
    }
    
    /**
     * Record that an edge was added (increments counter)
     */
    void recordEdge() {
        std::lock_guard<std::mutex> lock(mutex_);
        currentEdgeCount_++;
    }
    
    /**
     * Check if we should create a new snapshot based on current strategy
     */
    bool shouldCreateSnapshot() const {
        std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(mutex_));
        
        switch (mode_) {
            case SnapshotMode::TIME_BASED:
                return isTimeThresholdReached();
                
            case SnapshotMode::COUNT_BASED:
                return isEdgeCountThresholdReached();
                
            case SnapshotMode::HYBRID:
                return isTimeThresholdReached() || isEdgeCountThresholdReached();
                
            case SnapshotMode::ADAPTIVE:
                // For now, use hybrid logic
                // TODO: Implement adaptive sizing based on system metrics
                return isTimeThresholdReached() || isEdgeCountThresholdReached();
                
            default:
                return false;
        }
    }
    
    /**
     * Close current snapshot and prepare for next
     * @return The snapshot ID that was just closed
     */
    uint32_t closeCurrentSnapshot() {
        std::lock_guard<std::mutex> lock(mutex_);
        uint32_t closedSnapshotId = currentSnapshotId_;
        
        // Reset counters
        currentEdgeCount_ = 0;
        snapshotStartTime_ = std::chrono::system_clock::now();
        
        return closedSnapshotId;
    }
    
    /**
     * Open a new snapshot
     * @return The new snapshot ID
     */
    uint32_t openNewSnapshot() {
        std::lock_guard<std::mutex> lock(mutex_);
        currentSnapshotId_++;
        currentEdgeCount_ = 0;
        snapshotStartTime_ = std::chrono::system_clock::now();
        return currentSnapshotId_;
    }
    
    /**
     * Get current snapshot ID
     */
    uint32_t getCurrentSnapshotId() const {
        std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(mutex_));
        return currentSnapshotId_;
    }
    
    /**
     * Get current edge count in this snapshot
     */
    uint64_t getCurrentEdgeCount() const {
        std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(mutex_));
        return currentEdgeCount_;
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
     * Update thresholds dynamically
     */
    void updateThresholds(uint64_t timeThreshold, uint64_t edgeThreshold) {
        std::lock_guard<std::mutex> lock(mutex_);
        timeThresholdSeconds_ = timeThreshold;
        edgeCountThreshold_ = edgeThreshold;
    }
    
    /**
     * Change snapshot mode
     */
    void setMode(SnapshotMode mode) {
        std::lock_guard<std::mutex> lock(mutex_);
        mode_ = mode;
    }
    
    /**
     * Set current snapshot ID (used when restoring from disk)
     * This is typically called when restarting and continuing from existing snapshots
     */
    void setCurrentSnapshotId(uint32_t snapshotId) {
        std::lock_guard<std::mutex> lock(mutex_);
        currentSnapshotId_ = snapshotId;
        currentEdgeCount_ = 0;
        snapshotStartTime_ = std::chrono::system_clock::now();
    }
    
    /**
     * Convert string to SnapshotMode enum
     */
    static SnapshotMode stringToMode(const std::string& modeStr) {
        if (modeStr == "time") return SnapshotMode::TIME_BASED;
        if (modeStr == "count") return SnapshotMode::COUNT_BASED;
        if (modeStr == "hybrid") return SnapshotMode::HYBRID;
        if (modeStr == "adaptive") return SnapshotMode::ADAPTIVE;
        return SnapshotMode::HYBRID;  // Default
    }
};

#endif // SNAPSHOT_MANAGER_H
