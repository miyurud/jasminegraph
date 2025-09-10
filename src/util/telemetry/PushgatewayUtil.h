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

#ifndef JASMINEGRAPH_PUSHGATEWAYUTIL_H
#define JASMINEGRAPH_PUSHGATEWAYUTIL_H

#include <string>
#include <map>
#include <curl/curl.h>

/**
 * Utility class for sending metrics to Prometheus Pushgateway
 * Integrates with OpenTelemetry to export metrics to Pushgateway for Prometheus scraping
 */
class PushgatewayUtil {
private:
    static std::string pushgateway_url_;
    static std::string job_name_;
    static std::string instance_name_;
    static bool initialized_;
    
    // Helper function for HTTP POST
    static size_t WriteCallback(void* contents, size_t size, size_t nmemb, void* userp);
    
public:
    /**
     * Initialize Pushgateway configuration
     * @param pushgateway_url URL of the Pushgateway (e.g., "http://localhost:9091")
     * @param job_name Job name for metrics (e.g., "jasminegraph")
     * @param instance_name Instance identifier (e.g., "frontend-node-1")
     */
    static void initialize(const std::string& pushgateway_url, 
                         const std::string& job_name, 
                         const std::string& instance_name);
    
    /**
     * Push triangle count metrics to Pushgateway
     */
    static bool pushTriangleCountMetrics(
        const std::string& graph_id,
        long triangle_count,
        double execution_time_ms,
        int partition_count,
        const std::map<std::string, std::string>& additional_labels = {}
    );
    
    /**
     * Push worker performance metrics to Pushgateway
     */
    static bool pushWorkerMetrics(
        const std::string& worker_id,
        const std::string& operation_name,
        double value,
        const std::string& metric_type = "gauge",
        const std::map<std::string, std::string>& labels = {}
    );
    
    /**
     * Push execution timing metrics to Pushgateway
     */
    static bool pushExecutionMetrics(
        const std::string& operation_name,
        double duration_ms,
        const std::string& status = "success",
        const std::map<std::string, std::string>& labels = {}
    );
    
    /**
     * Push component-level metrics to Pushgateway
     */
    static bool pushComponentMetrics(
        const std::string& component_name,
        const std::string& metric_name,
        double value,
        const std::string& unit = "",
        const std::map<std::string, std::string>& labels = {}
    );
    
    /**
     * Push custom metrics with Prometheus format
     */
    static bool pushCustomMetric(
        const std::string& metric_name,
        const std::string& metric_help,
        const std::string& metric_type,
        double value,
        const std::map<std::string, std::string>& labels = {}
    );
    
    /**
     * Delete metrics from Pushgateway
     */
    static bool deleteMetrics();
    
    /**
     * Get metrics endpoint URL for this instance
     */
    static std::string getMetricsUrl();
};

#endif //JASMINEGRAPH_PUSHGATEWAYUTIL_H
