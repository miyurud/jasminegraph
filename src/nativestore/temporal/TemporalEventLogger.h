/**
Copyright 2024 JasmineGraph Team
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

#ifndef TEMPORAL_EVENT_LOGGER_H
#define TEMPORAL_EVENT_LOGGER_H

#include <mutex>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "../../util/logger/Logger.h"

struct TemporalEdgeEvent {
    std::string operation;
    std::string timestamp;
    nlohmann::json payload;

    nlohmann::json toJson() const;
};

class TemporalEventLogger {
 public:
    explicit TemporalEventLogger(const std::string& dbPrefix);
    void log(const TemporalEdgeEvent& event) const;
    std::string getLogFilePath() const;
    std::string getIndexFilePath() const;
    
    // Event log management
    void buildIndex();
    void rotateLog(size_t maxSizeBytes = 100 * 1024 * 1024);  // Default 100MB
    void compactLog(long long retentionMillis = 0);  // 0 means keep all
    
    // Query support
    std::vector<TemporalEdgeEvent> queryEvents(const std::string& entityId, 
                                                long long fromTimestamp = 0,
                                                long long toTimestamp = 0) const;

 private:
    std::string logFilePath;
    std::string indexFilePath;
    mutable std::mutex logMutex;
    
    void updateIndex(const std::string& entityId, long long timestamp, long long offset) const;
};

#endif  // TEMPORAL_EVENT_LOGGER_H
