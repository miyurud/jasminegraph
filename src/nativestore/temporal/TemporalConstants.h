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

#ifndef TEMPORAL_CONSTANTS_H
#define TEMPORAL_CONSTANTS_H

#include <string>
#include <chrono>

namespace TemporalConstants {
// Core temporal metadata keys
inline const std::string LAST_OPERATION = "__temporal_last_operation";
inline const std::string LAST_OPERATION_TS = "__temporal_last_operation_timestamp";
inline const std::string STATUS = "__temporal_status";
inline const std::string CREATED_AT = "__temporal_created_at";
inline const std::string DELETED_AT = "__temporal_deleted_at";
inline const std::string UPDATED_AT = "__temporal_updated_at";

// Property versioning
inline const std::string PROPERTY_VERSION = "__temporal_property_version";
inline const std::string PROPERTY_HISTORY = "__temporal_property_history";

// Status values
inline const std::string STATUS_ACTIVE = "ACTIVE";
inline const std::string STATUS_DELETED = "DELETED";

// Operation types
inline const std::string OP_ADD = "ADD";
inline const std::string OP_DELETE = "DELETE";
inline const std::string OP_UPDATE = "UPDATE";

// Timestamp utilities
inline long long getCurrentEpochMillis() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

inline std::string epochMillisToString(long long epochMillis) {
    return std::to_string(epochMillis);
}

inline long long stringToEpochMillis(const std::string& str) {
    try {
        return std::stoll(str);
    } catch (...) {
        return 0;
    }
}
}

#endif  // TEMPORAL_CONSTANTS_H
