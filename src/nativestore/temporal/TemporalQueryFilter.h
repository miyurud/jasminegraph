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

#ifndef TEMPORAL_QUERY_FILTER_H
#define TEMPORAL_QUERY_FILTER_H

#include <map>
#include <string>

#include <nlohmann/json.hpp>

#include "TemporalConstants.h"

enum class TemporalFilterMode {
    NONE,
    AS_OF,
    RANGE
};

struct TemporalConstraints {
    TemporalFilterMode mode = TemporalFilterMode::NONE;
    bool includeDeleted = false;
    std::string asOf;
    std::string rangeFrom;
    std::string rangeTo;
};

class TemporalQueryFilter {
 public:
    static TemporalConstraints fromJson(const nlohmann::json& plan);
    static bool isNodeVisible(const TemporalConstraints& constraints,
                              const std::map<std::string, std::string>& meta);
    static bool isRelationVisible(const TemporalConstraints& constraints,
                                  const std::map<std::string, std::string>& meta);

 private:
    static bool isVisible(const TemporalConstraints& constraints,
                          const std::map<std::string, std::string>& meta);
    static bool evaluateAsOf(const TemporalConstraints& constraints, const std::string& created,
                             const std::string& deleted, const std::string& status);
    static bool evaluateRange(const TemporalConstraints& constraints, const std::string& created,
                              const std::string& deleted);
    static long long normalizeTimestamp(const std::string& timestamp);
    static bool parseBoolean(const nlohmann::json& value, bool defaultValue);
};

#endif  // TEMPORAL_QUERY_FILTER_H
