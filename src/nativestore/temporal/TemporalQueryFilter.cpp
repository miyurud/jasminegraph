/**
Copyright 2025 JasmineGraph Team
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

#include "TemporalQueryFilter.h"

#include <algorithm>
#include <cctype>
#include <limits>

#include "../../util/Utils.h"

namespace {
const long long kNegativeInfinity = std::numeric_limits<long long>::min() / 2;
const long long kPositiveInfinity = std::numeric_limits<long long>::max() / 2;

std::string toUpper(const std::string& input) {
    std::string value = input;
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) { return std::toupper(c); });
    return value;
}

std::string cleanTimestamp(const std::string& timestamp) {
    std::string cleaned;
    cleaned.reserve(timestamp.size());
    for (char c : timestamp) {
        if (std::isdigit(static_cast<unsigned char>(c))) {
            cleaned.push_back(c);
        }
    }
    return cleaned;
}
}

TemporalConstraints TemporalQueryFilter::fromJson(const nlohmann::json& plan) {
    TemporalConstraints constraints;
    if (!plan.contains("temporal")) {
        return constraints;
    }

    const auto& config = plan["temporal"];
    std::string mode = "AS_OF";
    if (config.contains("mode") && config["mode"].is_string()) {
        mode = toUpper(config["mode"].get<std::string>());
    }

    constraints.includeDeleted = config.contains("includeDeleted")
                                     ? parseBoolean(config["includeDeleted"], false)
                                     : false;

    if (mode == "RANGE" || mode == "BETWEEN" || mode == "WINDOW") {
        constraints.mode = TemporalFilterMode::RANGE;
        if (config.contains("from") && config["from"].is_string()) {
            constraints.rangeFrom = config["from"].get<std::string>();
        } else if (config.contains("start") && config["start"].is_string()) {
            constraints.rangeFrom = config["start"].get<std::string>();
        }
        if (config.contains("to") && config["to"].is_string()) {
            constraints.rangeTo = config["to"].get<std::string>();
        } else if (config.contains("end") && config["end"].is_string()) {
            constraints.rangeTo = config["end"].get<std::string>();
        }
    } else {
        constraints.mode = TemporalFilterMode::AS_OF;
        if (config.contains("timestamp") && config["timestamp"].is_string()) {
            constraints.asOf = config["timestamp"].get<std::string>();
        } else if (config.contains("asOf") && config["asOf"].is_string()) {
            constraints.asOf = config["asOf"].get<std::string>();
        }
        if (constraints.asOf.empty()) {
            constraints.asOf = Utils::getCurrentTimestamp();
        }
    }
    return constraints;
}

bool TemporalQueryFilter::isNodeVisible(const TemporalConstraints& constraints,
                                        const std::map<std::string, std::string>& meta) {
    return isVisible(constraints, meta);
}

bool TemporalQueryFilter::isRelationVisible(const TemporalConstraints& constraints,
                                            const std::map<std::string, std::string>& meta) {
    return isVisible(constraints, meta);
}

bool TemporalQueryFilter::isVisible(const TemporalConstraints& constraints,
                                    const std::map<std::string, std::string>& meta) {
    if (constraints.mode == TemporalFilterMode::NONE) {
        return true;
    }

    auto createdIt = meta.find(TemporalConstants::CREATED_AT);
    auto deletedIt = meta.find(TemporalConstants::DELETED_AT);
    auto statusIt = meta.find(TemporalConstants::STATUS);

    std::string created = createdIt != meta.end() ? createdIt->second : std::string();
    std::string deleted = deletedIt != meta.end() ? deletedIt->second : std::string();
    std::string status = statusIt != meta.end() ? statusIt->second : TemporalConstants::STATUS_ACTIVE;

    if (constraints.mode == TemporalFilterMode::AS_OF) {
        return evaluateAsOf(constraints, created, deleted, status);
    }
    if (constraints.mode == TemporalFilterMode::RANGE) {
        return evaluateRange(constraints, created, deleted);
    }
    return true;
}

bool TemporalQueryFilter::evaluateAsOf(const TemporalConstraints& constraints, const std::string& created,
                                       const std::string& deleted, const std::string& status) {
    long long createdValue = created.empty() ? kNegativeInfinity : normalizeTimestamp(created);
    long long deletedValue = deleted.empty() ? kPositiveInfinity : normalizeTimestamp(deleted);
    long long asOfValue = constraints.asOf.empty() ? kPositiveInfinity : normalizeTimestamp(constraints.asOf);

    bool exists = createdValue <= asOfValue && asOfValue < deletedValue;
    if (!exists) {
        if (constraints.includeDeleted && createdValue <= asOfValue) {
            return true;
        }
        return false;
    }

    if (!constraints.includeDeleted && toUpper(status) == TemporalConstants::STATUS_DELETED) {
        return false;
    }

    return true;
}

bool TemporalQueryFilter::evaluateRange(const TemporalConstraints& constraints, const std::string& created,
                                        const std::string& deleted) {
    long long createdValue = created.empty() ? kNegativeInfinity : normalizeTimestamp(created);
    long long deletedValue = deleted.empty() ? kPositiveInfinity : normalizeTimestamp(deleted);
    long long rangeFromValue = constraints.rangeFrom.empty() ? kNegativeInfinity
                                                             : normalizeTimestamp(constraints.rangeFrom);
    long long rangeToValue = constraints.rangeTo.empty() ? kPositiveInfinity
                                                         : normalizeTimestamp(constraints.rangeTo);

    bool overlaps = createdValue < rangeToValue && deletedValue > rangeFromValue;
    if (overlaps) {
        return true;
    }
    if (constraints.includeDeleted && createdValue <= rangeToValue) {
        return true;
    }
    return false;
}

long long TemporalQueryFilter::normalizeTimestamp(const std::string& timestamp) {
    if (timestamp.empty()) {
        return kNegativeInfinity;
    }
    std::string cleaned = cleanTimestamp(timestamp);
    if (cleaned.empty()) {
        return kNegativeInfinity;
    }
    try {
        return std::stoll(cleaned);
    } catch (const std::exception&) {
        return kNegativeInfinity;
    }
}

bool TemporalQueryFilter::parseBoolean(const nlohmann::json& value, bool defaultValue) {
    if (value.is_boolean()) {
        return value.get<bool>();
    }
    if (value.is_string()) {
        std::string data = value.get<std::string>();
        std::transform(data.begin(), data.end(), data.begin(), [](unsigned char c) { return std::toupper(c); });
        if (data == "TRUE" || data == "1" || data == "YES") {
            return true;
        }
        if (data == "FALSE" || data == "0" || data == "NO") {
            return false;
        }
    }
    if (value.is_number_integer()) {
        return value.get<int>() != 0;
    }
    return defaultValue;
}
