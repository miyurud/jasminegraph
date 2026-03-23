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

#ifndef TEMPORAL_QUERY_EXECUTOR_H
#define TEMPORAL_QUERY_EXECUTOR_H

#include <string>
#include <vector>
#include <map>
#include <set>
#include <memory>
#include <functional>
#include "TemporalStore.h"
#include "TemporalStorePersistence.h"

/**
 * TemporalQueryExecutor - Execute temporal graph queries
 *
 * Supports temporal Cypher extensions:
 * - AT SNAPSHOT <id>: Query graph state at specific snapshot
 * - FROM <start> TO <end>: Query graph evolution over time range
 * - TEMPORAL PATTERN: Track pattern changes over time
 *
 * Examples:
 *  // Get edges at snapshot 42
 *   MATCH (a)-[r]->(b) AT SNAPSHOT 42
 *
 *  // Track edge evolution
 *   MATCH (a)-[r]->(b) FROM 0 TO 100
 *
 *  // Find when property changed
 *   MATCH (n {name: "Alice"})
 *   RETURN n.city FROM 0 TO 50
 */
class TemporalQueryExecutor {
 public:
    /**
     * Query result types
     */
    enum class ResultType {
        EDGE_LIST,  // List of edges
        NODE_LIST,  // List of nodes
        PROPERTY_VALUE,  // Single property value
        TIMELINE,  // Temporal evolution data
        AGGREGATE  // Aggregated statistics
    };

    /**
     * Query result structure
     */
    struct QueryResult {
        ResultType type;

        // Edge results
        std::vector<std::pair<std::string, std::string>> edges;

        // Node results
        std::vector<std::string> nodes;

        // Property results
        std::map<std::string, std::string> properties;

        // Timeline results (snapshot -> value)
        std::map<uint32_t, std::vector<std::pair<std::string, std::string>>> timeline;

        // Aggregate results
        std::map<std::string, double> aggregates;

        // Metadata
        uint32_t snapshotId;
        size_t resultCount;
        double executionTimeMs;
    };

    /**
     * Query types
     */
    enum class QueryType {
        POINT_IN_TIME,  // Query at specific snapshot
        TIME_RANGE,  // Query over time range
        PATTERN_MATCH,  // Pattern matching
        AGGREGATION  // Temporal aggregation
    };

 private:
    std::shared_ptr<TemporalStore> store_;
    std::string persistenceDir_;

    // Cache for loaded snapshots
    std::map<uint32_t, std::shared_ptr<TemporalStore>> snapshotCache_;
    size_t maxCacheSize_;

 public:
    /**
     * Constructor
     */
    explicit TemporalQueryExecutor(std::shared_ptr<TemporalStore> store,
                                  const std::string& persistenceDir = "",
                                  size_t maxCacheSize = 10)
        : store_(store), persistenceDir_(persistenceDir), maxCacheSize_(maxCacheSize) {}

    /**
     * Query edges at specific snapshot
     * @param snapshotId Snapshot identifier
     * @return List of edges existing at that snapshot
     */
    QueryResult getEdgesAtSnapshot(uint32_t snapshotId) {
        auto start = std::chrono::high_resolution_clock::now();

        QueryResult result;
        result.type = ResultType::EDGE_LIST;
        result.snapshotId = snapshotId;

        // Get edges from current store
        auto edges = store_->getEdgesAtSnapshot(snapshotId);
        for (const auto& edge : edges) {
            result.edges.push_back({edge.sourceId, edge.destId});
        }

        result.resultCount = result.edges.size();

        auto end = std::chrono::high_resolution_clock::now();
        result.executionTimeMs = std::chrono::duration<double, std::milli>(end - start).count();

        return result;
    }

    /**
     * Get specific edge existence at snapshot
     */
    bool edgeExistsAtSnapshot(const std::string& sourceId,
                             const std::string& destId,
                             uint32_t snapshotId) {
        return store_->edgeExistsAtSnapshot(sourceId, destId, snapshotId);
    }

    /**
     * Get property value at specific snapshot
     */
    std::string getPropertyAtSnapshot(const std::string& nodeId,
                                     const std::string& propertyName,
                                     uint32_t snapshotId) {
        return store_->getNodePropertyAtSnapshot(nodeId, propertyName, snapshotId);
    }

    /**
     * Track edge evolution over time range
     * @param sourceId Source node ID
     * @param destId Destination node ID
     * @param startSnapshot Start snapshot (inclusive)
     * @param endSnapshot End snapshot (inclusive)
     * @return Timeline showing when edge existed
     */
    QueryResult trackEdgeEvolution(const std::string& sourceId,
                                   const std::string& destId,
                                   uint32_t startSnapshot,
                                   uint32_t endSnapshot) {
        auto start = std::chrono::high_resolution_clock::now();

        QueryResult result;
        result.type = ResultType::TIMELINE;

        for (uint32_t snap = startSnapshot; snap <= endSnapshot; ++snap) {
            if (store_->edgeExistsAtSnapshot(sourceId, destId, snap)) {
                result.timeline[snap].push_back({sourceId, destId});
            }
        }

        result.resultCount = result.timeline.size();

        auto end = std::chrono::high_resolution_clock::now();
        result.executionTimeMs = std::chrono::duration<double, std::milli>(end - start).count();

        return result;
    }

    /**
     * Track property evolution over time range
     * @param nodeId Node identifier
     * @param propertyName Property to track
     * @param startSnapshot Start snapshot (inclusive)
     * @param endSnapshot End snapshot (inclusive)
     * @return Map of snapshot -> property value
     */
    std::map<uint32_t, std::string> trackPropertyEvolution(const std::string& nodeId,
                                                           const std::string& propertyName,
                                                           uint32_t startSnapshot,
                                                           uint32_t endSnapshot) {
        std::map<uint32_t, std::string> evolution;

        for (uint32_t snap = startSnapshot; snap <= endSnapshot; ++snap) {
            std::string value = store_->getNodePropertyAtSnapshot(nodeId, propertyName, snap);
            if (!value.empty()) {
                evolution[snap] = value;
            }
        }

        return evolution;
    }

    /**
     * Get all edges that existed at any point in time range
     * @param startSnapshot Start snapshot
     * @param endSnapshot End snapshot
     * @return Aggregated list of unique edges
     */
    QueryResult getEdgesInTimeRange(uint32_t startSnapshot, uint32_t endSnapshot) {
        auto start = std::chrono::high_resolution_clock::now();

        QueryResult result;
        result.type = ResultType::EDGE_LIST;

        // Use set to avoid duplicates
        std::set<std::pair<std::string, std::string>> uniqueEdges;

        for (uint32_t snap = startSnapshot; snap <= endSnapshot; ++snap) {
            auto edges = store_->getEdgesAtSnapshot(snap);
            for (const auto& edge : edges) {
                uniqueEdges.insert({edge.sourceId, edge.destId});
            }
        }

        // Convert to result
        result.edges.assign(uniqueEdges.begin(), uniqueEdges.end());
        result.resultCount = result.edges.size();

        auto end = std::chrono::high_resolution_clock::now();
        result.executionTimeMs = std::chrono::duration<double, std::milli>(end - start).count();

        return result;
    }

    /**
     * Count edges at each snapshot in range
     * @param startSnapshot Start snapshot
     * @param endSnapshot End snapshot
     * @return Map of snapshot -> edge count
     */
    std::map<uint32_t, size_t> countEdgesOverTime(uint32_t startSnapshot, uint32_t endSnapshot) {
        std::map<uint32_t, size_t> counts;

        for (uint32_t snap = startSnapshot; snap <= endSnapshot; ++snap) {
            auto edges = store_->getEdgesAtSnapshot(snap);
            counts[snap] = edges.size();
        }

        return counts;
    }

    /**
     * Find snapshots where edge appeared/disappeared
     * @param sourceId Source node ID
     * @param destId Destination node ID
     * @param maxSnapshots Maximum snapshots to check
     * @return Pairs of (snapshot, appeared=true/disappeared=false)
     */
    std::vector<std::pair<uint32_t, bool>> findEdgeTransitions(const std::string& sourceId,
                                                               const std::string& destId,
                                                               uint32_t maxSnapshots) {
        std::vector<std::pair<uint32_t, bool>> transitions;
        bool previousState = false;

        for (uint32_t snap = 0; snap < maxSnapshots; ++snap) {
            bool currentState = store_->edgeExistsAtSnapshot(sourceId, destId, snap);

            if (currentState != previousState) {
                transitions.push_back({snap, currentState});
                previousState = currentState;
            }
        }

        return transitions;
    }

    /**
     * Find nodes connected at specific snapshot
     * @param nodeId Center node
     * @param snapshotId Snapshot to query
     * @param direction "out" (outgoing), "in" (incoming), "both"
     * @return List of connected node IDs
     */
    std::vector<std::string> getNeighborsAtSnapshot(const std::string& nodeId,
                                                    uint32_t snapshotId,
                                                    const std::string& direction = "out") {
        std::set<std::string> neighbors;
        auto edges = store_->getEdgesAtSnapshot(snapshotId);

        for (const auto& edge : edges) {
            if (direction == "out" || direction == "both") {
                if (edge.sourceId == nodeId) {
                    neighbors.insert(edge.destId);
                }
            }
            if (direction == "in" || direction == "both") {
                if (edge.destId == nodeId) {
                    neighbors.insert(edge.sourceId);
                }
            }
        }

        return std::vector<std::string>(neighbors.begin(), neighbors.end());
    }

    /**
     * Compute temporal degree (number of neighbors over time)
     * @param nodeId Node identifier
     * @param startSnapshot Start snapshot
     * @param endSnapshot End snapshot
     * @return Map of snapshot -> degree
     */
    std::map<uint32_t, size_t> computeTemporalDegree(const std::string& nodeId,
                                                     uint32_t startSnapshot,
                                                     uint32_t endSnapshot) {
        std::map<uint32_t, size_t> degrees;

        for (uint32_t snap = startSnapshot; snap <= endSnapshot; ++snap) {
            auto neighbors = getNeighborsAtSnapshot(nodeId, snap, "both");
            degrees[snap] = neighbors.size();
        }

        return degrees;
    }

    /**
     * Find snapshots where property value matches
     * @param nodeId Node identifier
     * @param propertyName Property to check
     * @param targetValue Value to match
     * @param maxSnapshots Maximum snapshots to check
     * @return List of snapshots where property = targetValue
     */
    std::vector<uint32_t> findSnapshotsWithPropertyValue(const std::string& nodeId,
                                                         const std::string& propertyName,
                                                         const std::string& targetValue,
                                                         uint32_t maxSnapshots) {
        std::vector<uint32_t> matchingSnapshots;

        for (uint32_t snap = 0; snap < maxSnapshots; ++snap) {
            std::string value = store_->getNodePropertyAtSnapshot(nodeId, propertyName, snap);
            if (value == targetValue) {
                matchingSnapshots.push_back(snap);
            }
        }

        return matchingSnapshots;
    }

    /**
     * Reconstruct graph snapshot from disk
     * @param snapshotId Snapshot to load
     * @return Loaded TemporalStore or nullptr if failed
     */
    std::shared_ptr<TemporalStore> loadSnapshot(uint32_t snapshotId) {
        // Check cache first
        auto it = snapshotCache_.find(snapshotId);
        if (it != snapshotCache_.end()) {
            return it->second;
        }

        // Load from disk
        if (persistenceDir_.empty()) {
            return nullptr;
        }

        uint32_t graphId = store_->getGraphId();
        uint32_t partitionId = store_->getPartitionId();

        std::string filePath = TemporalStorePersistence::generateFilePath(
            persistenceDir_, graphId, partitionId, snapshotId);

        auto loadedStore = std::make_shared<TemporalStore>(graphId, partitionId);
        if (!loadedStore->loadSnapshotFromDisk(filePath)) {
            return nullptr;
        }

        // Add to cache (evict oldest if needed)
        if (snapshotCache_.size() >= maxCacheSize_) {
            snapshotCache_.erase(snapshotCache_.begin());
        }
        snapshotCache_[snapshotId] = loadedStore;

        return loadedStore;
    }

    /**
     * Clear snapshot cache
     */
    void clearCache() {
        snapshotCache_.clear();
    }

    /**
     * Get cache statistics
     */
    struct CacheStats {
        size_t size;
        size_t maxSize;
        size_t hits;
        size_t misses;
    };

    CacheStats getCacheStats() const {
        CacheStats stats;
        stats.size = snapshotCache_.size();
        stats.maxSize = maxCacheSize_;
        stats.hits = 0;  // Would need to track this
        stats.misses = 0;
        return stats;
    }

    /**
     * Aggregate query: Get edge count statistics
     */
    QueryResult aggregateEdgeStats(uint32_t startSnapshot, uint32_t endSnapshot) {
        auto start = std::chrono::high_resolution_clock::now();

        QueryResult result;
        result.type = ResultType::AGGREGATE;

        size_t totalCount = 0;
        size_t minCount = SIZE_MAX;
        size_t maxCount = 0;
        size_t snapshotCount = 0;

        for (uint32_t snap = startSnapshot; snap <= endSnapshot; ++snap) {
            auto edges = store_->getEdgesAtSnapshot(snap);
            size_t count = edges.size();

            totalCount += count;
            minCount = std::min(minCount, count);
            maxCount = std::max(maxCount, count);
            snapshotCount++;
        }

        double avgCount = snapshotCount > 0 ?
            static_cast<double>(totalCount) / snapshotCount : 0.0;

        result.aggregates["total"] = totalCount;
        result.aggregates["average"] = avgCount;
        result.aggregates["min"] = minCount;
        result.aggregates["max"] = maxCount;
        result.aggregates["snapshots"] = snapshotCount;

        auto end = std::chrono::high_resolution_clock::now();
        result.executionTimeMs = std::chrono::duration<double, std::milli>(end - start).count();

        return result;
    }
};

#endif  // TEMPORAL_QUERY_EXECUTOR_H
