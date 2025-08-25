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

#ifndef JASMINEGRAPH_METRICMETADATA_H
#define JASMINEGRAPH_METRICMETADATA_H

#include <string>
#include <map>
#include <vector>

namespace JasmineGraph {
namespace Telemetry {

enum class MetricType {
    COUNTER,    // Always increasing values (function calls, triangle counts)
    GAUGE,      // Current values (memory usage, current connections)
    HISTOGRAM   // Time durations, sizes
};

struct MetricMetadata {
    std::string name;                                    // Base metric name
    std::string value;                                   // Metric value as string
    std::map<std::string, std::string> labels;          // Key-value labels
    std::string unit;                                    // Unit of measurement
    std::string description;                             // Human readable description
    MetricType type;                                     // Prometheus metric type
    
    // Constructor for easy creation
    MetricMetadata(const std::string& name, const std::string& value, 
                   MetricType type = MetricType::COUNTER,
                   const std::string& unit = "", 
                   const std::string& description = "")
        : name(name), value(value), type(type), unit(unit), description(description) {}
    
    // Add label helper
    MetricMetadata& addLabel(const std::string& key, const std::string& value) {
        labels[key] = value;
        return *this;
    }
    
    // Generate Prometheus format with labels
    std::string toPrometheusFormat() const {
        std::string result = name;
        
        // Add labels if any
        if (!labels.empty()) {
            result += "{";
            bool first = true;
            for (const auto& label : labels) {
                if (!first) result += ",";
                result += label.first + "=\"" + label.second + "\"";
                first = false;
            }
            result += "}";
        }
        
        result += " " + value;
        return result;
    }
};

} // namespace Telemetry
} // namespace JasmineGraph

#endif //JASMINEGRAPH_METRICMETADATA_H
