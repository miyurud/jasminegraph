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

#ifndef DATA_AGGREGATOR_H
#define DATA_AGGREGATOR_H

#include <vector>
#include <map>
#include <string>
#include <mutex>
#include <chrono>
#include "TemporalStore.h"

/**
 * DataAggregator - Buffers and batches updates before writing to TemporalStore
 *
 * Purpose:
 * - Reduce I/O operations by batching updates
 * - Handle partition-wise buffering
 * - Flush when buffer is full or timeout reached
 *
 * Example:
 *   DataAggregator aggregator;
 *
 *  // Buffer updates
 *   aggregator.bufferEdgeUpdate(partitionId, "Alice", "Bob", snapshot);
 *   aggregator.bufferEdgeUpdate(partitionId, "Bob", "Charlie", snapshot);
 *
 *  // Check if should flush
 *   if (aggregator.shouldFlush(partitionId)) {
 *       aggregator.flushPartition(partitionId, temporalStore);
 *   }
 */
class DataAggregator {
 public:
    struct EdgeUpdate {
        std::string sourceId;
        std::string destId;
        uint32_t snapshotId;
        bool isAddition;  // true = add, false = remove
        std::chrono::time_point<std::chrono::system_clock> timestamp;

        EdgeUpdate(const std::string& src, const std::string& dst,
                  uint32_t snapId, bool isAdd = true)
            : sourceId(src), destId(dst), snapshotId(snapId),
              isAddition(isAdd), timestamp(std::chrono::system_clock::now()) {}
    };

    struct PropertyUpdate {
        std::string nodeOrEdgeId;
        std::string propertyKey;
        std::string propertyValue;
        uint32_t snapshotId;
        bool isNodeProperty;  // true = node, false = edge
        std::chrono::time_point<std::chrono::system_clock> timestamp;

        PropertyUpdate(const std::string& id, const std::string& key,
                      const std::string& value, uint32_t snapId, bool isNode)
            : nodeOrEdgeId(id), propertyKey(key), propertyValue(value),
              snapshotId(snapId), isNodeProperty(isNode),
              timestamp(std::chrono::system_clock::now()) {}
    };

    struct PartitionBuffer {
        std::vector<EdgeUpdate> edgeUpdates;
        std::vector<PropertyUpdate> propertyUpdates;
        std::chrono::time_point<std::chrono::system_clock> bufferStartTime;
        uint64_t edgeCount;

        PartitionBuffer()
            : bufferStartTime(std::chrono::system_clock::now()),
              edgeCount(0) {}
    };

 private:
    // Partition ID -> Buffer
    std::map<uint32_t, PartitionBuffer> partitionBuffers_;

    // Configuration
    uint64_t maxBufferSize_;  // Max edges before flush
    uint64_t maxBufferTimeSeconds_;  // Max seconds before flush

    // Thread safety
    mutable std::mutex mutex_;

 public:
    /**
     * Constructor
     * @param maxBufferSize Maximum edges to buffer before flushing
     * @param maxBufferTimeSeconds Maximum seconds to buffer before flushing
     */
    DataAggregator(uint64_t maxBufferSize = 1000,
                  uint64_t maxBufferTimeSeconds = 5)
        : maxBufferSize_(maxBufferSize),
          maxBufferTimeSeconds_(maxBufferTimeSeconds) {
    }

    /**
     * Buffer an edge update
     */
    void bufferEdgeUpdate(uint32_t partitionId,
                         const std::string& sourceId,
                         const std::string& destId,
                         uint32_t snapshotId,
                         bool isAddition = true) {
        std::lock_guard<std::mutex> lock(mutex_);

        auto& buffer = partitionBuffers_[partitionId];
        buffer.edgeUpdates.emplace_back(sourceId, destId, snapshotId, isAddition);
        buffer.edgeCount++;
    }

    /**
     * Buffer a property update
     */
    void bufferPropertyUpdate(uint32_t partitionId,
                             const std::string& nodeOrEdgeId,
                             const std::string& propertyKey,
                             const std::string& propertyValue,
                             uint32_t snapshotId,
                             bool isNodeProperty) {
        std::lock_guard<std::mutex> lock(mutex_);

        auto& buffer = partitionBuffers_[partitionId];
        buffer.propertyUpdates.emplace_back(nodeOrEdgeId, propertyKey,
                                           propertyValue, snapshotId,
                                           isNodeProperty);
    }

    /**
     * Check if partition buffer should be flushed
     */
    bool shouldFlush(uint32_t partitionId) const {
        std::lock_guard<std::mutex> lock(mutex_);

        auto it = partitionBuffers_.find(partitionId);
        if (it == partitionBuffers_.end()) {
            return false;
        }

        const auto& buffer = it->second;

        // Check edge count threshold
        if (buffer.edgeCount >= maxBufferSize_) {
            return true;
        }

        // Check time threshold
        auto now = std::chrono::system_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
            now - buffer.bufferStartTime).count();
        if (elapsed >= maxBufferTimeSeconds_) {
            return true;
        }

        return false;
    }

    /**
     * Flush partition buffer to TemporalStore
     * @return Number of updates flushed
     */
    uint64_t flushPartition(uint32_t partitionId, TemporalStore* store) {
        if (!store) return 0;

        std::lock_guard<std::mutex> lock(mutex_);

        auto it = partitionBuffers_.find(partitionId);
        if (it == partitionBuffers_.end()) {
            return 0;
        }

        auto& buffer = it->second;
        uint64_t updateCount = 0;

        // Flush edge updates
        for (const auto& update : buffer.edgeUpdates) {
            if (update.isAddition) {
                store->addEdge(update.sourceId, update.destId, update.snapshotId);
            } else {
                store->removeEdge(update.sourceId, update.destId, update.snapshotId);
            }
            updateCount++;
        }

        // Flush property updates
        for (const auto& update : buffer.propertyUpdates) {
            if (update.isNodeProperty) {
                store->updateNodeProperty(update.nodeOrEdgeId,
                                        update.propertyKey,
                                        update.propertyValue,
                                        update.snapshotId);
            } else {
                // Parse edge ID (format: "src->dst")
                size_t arrowPos = update.nodeOrEdgeId.find("->");
                if (arrowPos != std::string::npos) {
                    std::string src = update.nodeOrEdgeId.substr(0, arrowPos);
                    std::string dst = update.nodeOrEdgeId.substr(arrowPos + 2);
                    store->updateEdgeProperty(src, dst,
                                            update.propertyKey,
                                            update.propertyValue,
                                            update.snapshotId);
                }
            }
            updateCount++;
        }

        // Clear buffer
        buffer.edgeUpdates.clear();
        buffer.propertyUpdates.clear();
        buffer.edgeCount = 0;
        buffer.bufferStartTime = std::chrono::system_clock::now();

        return updateCount;
    }

    /**
     * Flush all partition buffers
     * @return Total number of updates flushed
     */
    uint64_t flushAll(TemporalStore* store) {
        if (!store) return 0;

        std::lock_guard<std::mutex> lock(mutex_);

        uint64_t totalUpdates = 0;
        for (auto& [partitionId, buffer] : partitionBuffers_) {
            // Unlock temporarily to avoid deadlock
            mutex_.unlock();
            totalUpdates += flushPartition(partitionId, store);
            mutex_.lock();
        }

        return totalUpdates;
    }

    /**
     * Get buffer statistics
     */
    struct BufferStats {
        uint32_t partitionId;
        uint64_t edgeUpdateCount;
        uint64_t propertyUpdateCount;
        uint64_t secondsSinceStart;
    };

    std::vector<BufferStats> getBufferStats() const {
        std::lock_guard<std::mutex> lock(mutex_);

        std::vector<BufferStats> stats;
        auto now = std::chrono::system_clock::now();

        for (const auto& [partitionId, buffer] : partitionBuffers_) {
            BufferStats s;
            s.partitionId = partitionId;
            s.edgeUpdateCount = buffer.edgeUpdates.size();
            s.propertyUpdateCount = buffer.propertyUpdates.size();
            s.secondsSinceStart = std::chrono::duration_cast<std::chrono::seconds>(
                now - buffer.bufferStartTime).count();
            stats.push_back(s);
        }

        return stats;
    }

    /**
     * Clear all buffers
     */
    void clearAll() {
        std::lock_guard<std::mutex> lock(mutex_);
        partitionBuffers_.clear();
    }

    /**
     * Update configuration
     */
    void updateConfig(uint64_t maxBufferSize, uint64_t maxBufferTimeSeconds) {
        std::lock_guard<std::mutex> lock(mutex_);
        maxBufferSize_ = maxBufferSize;
        maxBufferTimeSeconds_ = maxBufferTimeSeconds;
    }
};

#endif  // DATA_AGGREGATOR_H
