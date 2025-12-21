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
    : logFilePath(dbPrefix + "_temporal_history.jsonl") {}

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
