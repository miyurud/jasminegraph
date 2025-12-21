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

#include "TemporalEventLogger.h"

#include <fstream>
#include <algorithm>
#include <sys/stat.h>

#include "TemporalConstants.h"

static Logger temporal_event_logger;

nlohmann::json TemporalEdgeEvent::toJson() const {
    nlohmann::json serialized = payload.is_null() ? nlohmann::json::object() : payload;
    if (!operation.empty()) {
        serialized["operationType"] = operation;
    }
    if (!timestamp.empty()) {
        serialized["operationTimestamp"] = timestamp;
    }
    return serialized;
}

TemporalEventLogger::TemporalEventLogger(const std::string& dbPrefix)
    : logFilePath(dbPrefix + "_temporal_history.jsonl"),
      indexFilePath(dbPrefix + "_temporal_index.db") {}

void TemporalEventLogger::log(const TemporalEdgeEvent& event) const {
    std::ofstream outfile;
    auto metadata = event.toJson();
    std::string serialized = metadata.dump();

    std::lock_guard<std::mutex> guard(logMutex);
    outfile.open(logFilePath, std::ios::out | std::ios::app);

    if (!outfile.is_open()) {
        temporal_event_logger.error("Failed to open temporal log file: " + logFilePath);
        return;
    }

    outfile << serialized << std::endl;
}

std::string TemporalEventLogger::getLogFilePath() const {
    return logFilePath;
}

std::string TemporalEventLogger::getIndexFilePath() const {
    return indexFilePath;
}

void TemporalEventLogger::updateIndex(const std::string& entityId, long long timestamp, long long offset) const {
    // Simple index: entityId -> list of (timestamp, offset) pairs
    // Format: entityId|timestamp|offset\n
    std::ofstream indexFile(indexFilePath, std::ios::out | std::ios::app);
    if (indexFile.is_open()) {
        indexFile << entityId << "|" << timestamp << "|" << offset << std::endl;
    }
}

void TemporalEventLogger::buildIndex() {
    std::lock_guard<std::mutex> guard(logMutex);
    std::ifstream logFile(logFilePath);
    std::ofstream indexFile(indexFilePath, std::ios::out | std::ios::trunc);
    
    if (!logFile.is_open() || !indexFile.is_open()) {
        temporal_event_logger.error("Failed to build index for " + logFilePath);
        return;
    }
    
    std::string line;
    long long offset = 0;
    while (std::getline(logFile, line)) {
        try {
            auto event = nlohmann::json::parse(line);
            std::string entityId;
            long long timestamp = 0;
            
            // Extract entity ID (from source or destination)
            if (event.contains("source") && event["source"].contains("id")) {
                entityId = event["source"]["id"].get<std::string>();
            } else if (event.contains("id")) {
                entityId = event["id"].get<std::string>();
            }
            
            // Extract timestamp
            if (event.contains("operationTimestamp")) {
                if (event["operationTimestamp"].is_number()) {
                    timestamp = event["operationTimestamp"].get<long long>();
                } else if (event["operationTimestamp"].is_string()) {
                    timestamp = TemporalConstants::stringToEpochMillis(
                        event["operationTimestamp"].get<std::string>());
                }
            }
            
            if (!entityId.empty()) {
                indexFile << entityId << "|" << timestamp << "|" << offset << std::endl;
            }
        } catch (...) {
            // Skip malformed lines
        }
        offset = logFile.tellg();
    }
    temporal_event_logger.info("Index built for " + logFilePath);
}

void TemporalEventLogger::rotateLog(size_t maxSizeBytes) {
    std::lock_guard<std::mutex> guard(logMutex);
    struct stat st;
    if (stat(logFilePath.c_str(), &st) == 0 && st.st_size > static_cast<long>(maxSizeBytes)) {
        std::string archivePath = logFilePath + "." + 
            TemporalConstants::epochMillisToString(TemporalConstants::getCurrentEpochMillis());
        std::rename(logFilePath.c_str(), archivePath.c_str());
        temporal_event_logger.info("Rotated log to " + archivePath);
    }
}

void TemporalEventLogger::compactLog(long long retentionMillis) {
    std::lock_guard<std::mutex> guard(logMutex);
    if (retentionMillis <= 0) {
        return;  // Keep all events
    }
    
    long long cutoffTime = TemporalConstants::getCurrentEpochMillis() - retentionMillis;
    std::string tempPath = logFilePath + ".tmp";
    
    std::ifstream inFile(logFilePath);
    std::ofstream outFile(tempPath);
    
    if (!inFile.is_open() || !outFile.is_open()) {
        temporal_event_logger.error("Failed to compact log " + logFilePath);
        return;
    }
    
    std::string line;
    int kept = 0, removed = 0;
    while (std::getline(inFile, line)) {
        try {
            auto event = nlohmann::json::parse(line);
            long long timestamp = 0;
            
            if (event.contains("operationTimestamp")) {
                if (event["operationTimestamp"].is_number()) {
                    timestamp = event["operationTimestamp"].get<long long>();
                } else if (event["operationTimestamp"].is_string()) {
                    timestamp = TemporalConstants::stringToEpochMillis(
                        event["operationTimestamp"].get<std::string>());
                }
            }
            
            if (timestamp >= cutoffTime) {
                outFile << line << std::endl;
                kept++;
            } else {
                removed++;
            }
        } catch (...) {
            // Keep malformed lines
            outFile << line << std::endl;
        }
    }
    
    inFile.close();
    outFile.close();
    
    std::rename(tempPath.c_str(), logFilePath.c_str());
    temporal_event_logger.info("Compacted log: kept " + std::to_string(kept) + 
                              ", removed " + std::to_string(removed) + " events");
}

std::vector<TemporalEdgeEvent> TemporalEventLogger::queryEvents(
    const std::string& entityId, long long fromTimestamp, long long toTimestamp) const {
    
    std::vector<TemporalEdgeEvent> results;
    std::lock_guard<std::mutex> guard(logMutex);
    
    std::ifstream logFile(logFilePath);
    if (!logFile.is_open()) {
        return results;
    }
    
    std::string line;
    while (std::getline(logFile, line)) {
        try {
            auto event = nlohmann::json::parse(line);
            
            // Check entity ID match
            std::string currentEntityId;
            if (event.contains("source") && event["source"].contains("id")) {
                currentEntityId = event["source"]["id"].get<std::string>();
            } else if (event.contains("id")) {
                currentEntityId = event["id"].get<std::string>();
            }
            
            if (!entityId.empty() && currentEntityId != entityId) {
                continue;
            }
            
            // Check timestamp range
            long long timestamp = 0;
            if (event.contains("operationTimestamp")) {
                if (event["operationTimestamp"].is_number()) {
                    timestamp = event["operationTimestamp"].get<long long>();
                } else if (event["operationTimestamp"].is_string()) {
                    timestamp = TemporalConstants::stringToEpochMillis(
                        event["operationTimestamp"].get<std::string>());
                }
            }
            
            if ((fromTimestamp == 0 || timestamp >= fromTimestamp) &&
                (toTimestamp == 0 || timestamp <= toTimestamp)) {
                
                TemporalEdgeEvent edgeEvent;
                if (event.contains("operationType")) {
                    edgeEvent.operation = event["operationType"].get<std::string>();
                }
                if (event.contains("operationTimestamp")) {
                    if (event["operationTimestamp"].is_string()) {
                        edgeEvent.timestamp = event["operationTimestamp"].get<std::string>();
                    } else {
                        edgeEvent.timestamp = std::to_string(timestamp);
                    }
                }
                edgeEvent.payload = event;
                results.push_back(edgeEvent);
            }
        } catch (...) {
            // Skip malformed lines
        }
    }
    
    return results;
}
