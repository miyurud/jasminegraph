/**
Copyright 2026 JasmineGraph Team
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

#ifndef PROPERTY_INTERVAL_DICTIONARY_H
#define PROPERTY_INTERVAL_DICTIONARY_H

#include <map>
#include <vector>
#include <string>
#include <algorithm>
#include <cstdint>
#include <limits>

/**
 * Represents a time interval during which a property had a specific value
 *
 * Example:
 *   PropertyInterval("blue", 5, 10) means:
 *   Property value was "blue" from snapshot 5 to snapshot 10 (inclusive)
 */
struct PropertyInterval {
    std::string value;
    uint32_t startSnapshot;
    uint32_t endSnapshot;  // UINT32_MAX means "forever" (still active)

    PropertyInterval(const std::string& val, uint32_t start, uint32_t end = UINT32_MAX)
        : value(val), startSnapshot(start), endSnapshot(end) {}

    /**
     * Check if this interval contains a snapshot
     * Example: interval(5,10).contains(7) → true
     */
    bool contains(uint32_t snapshotId) const {
        return snapshotId >= startSnapshot && snapshotId <= endSnapshot;
    }
};

/**
 * Dictionary storing property name → list of intervals
 *
 * Example usage:
 *   PropertyIntervalDictionary dict;
 *   dict.addOrUpdateProperty("color", "red", 1);  // color = red starting at snapshot 1
 *   dict.addOrUpdateProperty("color", "blue", 5);  // color = blue starting at snapshot 5
 *   dict.getValueAtSnapshot("color", 3);  // Returns "red"
 *   dict.getValueAtSnapshot("color", 7);  // Returns "blue"
 */
class PropertyIntervalDictionary {
 private:
    // Property key → list of intervals (sorted by startSnapshot)
    // Example: "age" → [(25,1,10), (26,11,20), (27,21,∞)]
    std::map<std::string, std::vector<PropertyInterval>> properties_;

 public:
    PropertyIntervalDictionary() = default;

    /**
     * Add or update a property value starting at a snapshot
     *
     * Example:
     *   dict.addOrUpdateProperty("age", "25", 1);  // age = 25 starting at snapshot 1
     *   dict.addOrUpdateProperty("age", "26", 11);  // age = 26 starting at snapshot 11
     *
     * This automatically closes the previous interval:
     *   Result: [("25", 1, 10), ("26", 11, ∞)]
     */
    void addOrUpdateProperty(const std::string& key,
                            const std::string& value,
                            uint32_t snapshotId) {
        auto& intervals = properties_[key];

        // Case 1: First value for this property
        if (intervals.empty()) {
            intervals.emplace_back(value, snapshotId);
            return;
        }

        // Case 2: Close the most recent interval and add new one
        PropertyInterval& lastInterval = intervals.back();
        if (lastInterval.endSnapshot == UINT32_MAX) {
            // Close the previous interval at snapshot before current
            lastInterval.endSnapshot = snapshotId - 1;
        }

        // Add new interval (open-ended)
        intervals.emplace_back(value, snapshotId);
    }

    /**
     * Get property value at a specific snapshot (O(log n) binary search)
     *
     * Example:
     *   Intervals: [("NYC", 1, 10), ("LA", 11, 20), ("SF", 21, ∞)]
     *   Query snapshot 15 → Returns "LA"
     *   Query snapshot 5 → Returns "NYC"
     *   Query snapshot 100 → Returns "SF"
     *
     * Time complexity: O(log n) where n = number of intervals for this property
     */
    std::string getValueAtSnapshot(const std::string& key,
                                   uint32_t snapshotId) const {
        auto it = properties_.find(key);
        if (it == properties_.end()) {
            return "";  // Property doesn't exist
        }

        const auto& intervals = it->second;

        // Binary search for the interval containing snapshotId
        auto intervalIt = std::lower_bound(
            intervals.begin(),
            intervals.end(),
            snapshotId,
            [](const PropertyInterval& interval, uint32_t snapId) {
                return interval.endSnapshot < snapId;
            });

        if (intervalIt != intervals.end() && intervalIt->contains(snapshotId)) {
            return intervalIt->value;
        }

        return "";  // Property didn't exist at this snapshot
    }

    /**
     * Get all intervals for a property (for debugging/analysis)
     *
     * Example:
     *   auto intervals = dict.getIntervals("color");
     *   for (auto& interval : intervals) {
     *       cout << interval.value << " from " << interval.startSnapshot
     *            << " to " << interval.endSnapshot << endl;
     *   }
     */
    std::vector<PropertyInterval> getIntervals(const std::string& key) const {
        auto it = properties_.find(key);
        if (it != properties_.end()) {
            return it->second;
        }
        return {};
    }

    /**
     * Get all property keys
     * Example: ["age", "city", "name"]
     */
    std::vector<std::string> getPropertyKeys() const {
        std::vector<std::string> keys;
        for (const auto& pair : properties_) {
            keys.push_back(pair.first);
        }
        return keys;
    }

    /**
     * Check if property exists
     */
    bool hasProperty(const std::string& key) const {
        return properties_.find(key) != properties_.end();
    }

    /**
     * Get total number of intervals across all properties
     * Useful for memory estimation
     */
    size_t getTotalIntervals() const {
        size_t count = 0;
        for (const auto& pair : properties_) {
            count += pair.second.size();
        }
        return count;
    }

    /**
     * Serialize to JSON-like format for database storage
     *
     * Format:
     * {"color":[{"v":"red","s":1,"e":10},{"v":"blue","s":11,"e":4294967295}]}
     */
    std::string serialize() const {
        std::string result = "{";
        bool first = true;

        for (const auto& pair : properties_) {
            if (!first) result += ",";
            first = false;

            const std::string& key = pair.first;
            const std::vector<PropertyInterval>& intervals = pair.second;

            result += "\"" + key + "\":[";
            for (size_t i = 0; i < intervals.size(); ++i) {
                if (i > 0) result += ",";
                const auto& interval = intervals[i];
                result += "{\"v\":\"" + interval.value +
                         "\",\"s\":" + std::to_string(interval.startSnapshot) +
                         ",\"e\":" + std::to_string(interval.endSnapshot) + "}";
            }
            result += "]";
        }

        result += "}";
        return result;
    }

    /**
     * Get estimated memory usage
     * Rough estimate: key size + interval data
     */
    size_t estimateMemoryBytes() const {
        size_t total = 0;
        for (const auto& pair : properties_) {
            total += pair.first.size();
            for (const auto& interval : pair.second) {
                total += interval.value.size() + sizeof(uint32_t) * 2;
            }
        }
        return total;
    }
};

#endif  // PROPERTY_INTERVAL_DICTIONARY_H
