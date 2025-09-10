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

#include "PushgatewayUtil.h"
#include <sstream>
#include <iostream>
#include <curl/curl.h>

// Static member definitions
std::string PushgatewayUtil::pushgateway_url_;
std::string PushgatewayUtil::job_name_;
std::string PushgatewayUtil::instance_name_;
bool PushgatewayUtil::initialized_ = false;

size_t PushgatewayUtil::WriteCallback(void* contents, size_t size, size_t nmemb, void* userp) {
    ((std::string*)userp)->append((char*)contents, size * nmemb);
    return size * nmemb;
}

void PushgatewayUtil::initialize(const std::string& pushgateway_url, 
                                const std::string& job_name, 
                                const std::string& instance_name) {
    pushgateway_url_ = pushgateway_url;
    job_name_ = job_name;
    instance_name_ = instance_name;
    initialized_ = true;
    
    // Initialize curl globally
    curl_global_init(CURL_GLOBAL_DEFAULT);
    
    std::cout << "PushgatewayUtil initialized:" << std::endl;
    std::cout << "  URL: " << pushgateway_url_ << std::endl;
    std::cout << "  Job: " << job_name_ << std::endl;
    std::cout << "  Instance: " << instance_name_ << std::endl;
}

bool PushgatewayUtil::pushTriangleCountMetrics(
    const std::string& graph_id,
    long triangle_count,
    double execution_time_ms,
    int partition_count,
    const std::map<std::string, std::string>& additional_labels) {
    
    if (!initialized_) {
        std::cerr << "PushgatewayUtil not initialized" << std::endl;
        return false;
    }
    
    // Prepare metrics data in Prometheus format
    std::stringstream metrics_data;
    
    // Triangle count metric
    metrics_data << "# HELP jasminegraph_triangle_count Total number of triangles found in graph\n";
    metrics_data << "# TYPE jasminegraph_triangle_count gauge\n";
    metrics_data << "jasminegraph_triangle_count{graph_id=\"" << graph_id 
                 << "\",partition_count=\"" << partition_count << "\"";
    for (const auto& label : additional_labels) {
        metrics_data << ",\"" << label.first << "\"=\"" << label.second << "\"";
    }
    metrics_data << "} " << triangle_count << "\n";
    
    // Execution time metric
    metrics_data << "# HELP jasminegraph_triangle_count_duration_ms Triangle counting execution time in milliseconds\n";
    metrics_data << "# TYPE jasminegraph_triangle_count_duration_ms gauge\n";
    metrics_data << "jasminegraph_triangle_count_duration_ms{graph_id=\"" << graph_id 
                 << "\",partition_count=\"" << partition_count << "\"";
    for (const auto& label : additional_labels) {
        metrics_data << ",\"" << label.first << "\"=\"" << label.second << "\"";
    }
    metrics_data << "} " << execution_time_ms << "\n";
    
    // Partition count metric
    metrics_data << "# HELP jasminegraph_partition_count Number of partitions processed\n";
    metrics_data << "# TYPE jasminegraph_partition_count gauge\n";
    metrics_data << "jasminegraph_partition_count{graph_id=\"" << graph_id << "\"";
    for (const auto& label : additional_labels) {
        metrics_data << ",\"" << label.first << "\"=\"" << label.second << "\"";
    }
    metrics_data << "} " << partition_count << "\n";
    
    return pushCustomMetric("jasminegraph_triangle_metrics", 
                           "JasmineGraph triangle counting metrics", 
                           "gauge", 
                           0, // dummy value - actual metrics are in the body
                           {{"graph_id", graph_id}});
}

bool PushgatewayUtil::pushWorkerMetrics(
    const std::string& worker_id,
    const std::string& operation_name,
    double value,
    const std::string& metric_type,
    const std::map<std::string, std::string>& labels) {
    
    if (!initialized_) {
        std::cerr << "PushgatewayUtil not initialized" << std::endl;
        return false;
    }
    
    std::map<std::string, std::string> worker_labels = labels;
    worker_labels["worker_id"] = worker_id;
    worker_labels["operation"] = operation_name;
    
    std::string metric_name = "jasminegraph_worker_" + operation_name;
    return pushCustomMetric(metric_name,
                           "JasmineGraph worker metric for " + operation_name,
                           metric_type,
                           value,
                           worker_labels);
}

bool PushgatewayUtil::pushExecutionMetrics(
    const std::string& operation_name,
    double duration_ms,
    const std::string& status,
    const std::map<std::string, std::string>& labels) {
    
    if (!initialized_) {
        std::cerr << "PushgatewayUtil not initialized" << std::endl;
        return false;
    }
    
    std::map<std::string, std::string> exec_labels = labels;
    exec_labels["operation"] = operation_name;
    exec_labels["status"] = status;
    
    return pushCustomMetric("jasminegraph_execution_duration_ms",
                           "JasmineGraph operation execution time in milliseconds",
                           "gauge",
                           duration_ms,
                           exec_labels);
}

bool PushgatewayUtil::pushComponentMetrics(
    const std::string& component_name,
    const std::string& metric_name,
    double value,
    const std::string& unit,
    const std::map<std::string, std::string>& labels) {
    
    if (!initialized_) {
        std::cerr << "PushgatewayUtil not initialized" << std::endl;
        return false;
    }
    
    std::map<std::string, std::string> comp_labels = labels;
    comp_labels["component"] = component_name;
    if (!unit.empty()) {
        comp_labels["unit"] = unit;
    }
    
    std::string full_metric_name = "jasminegraph_component_" + metric_name;
    return pushCustomMetric(full_metric_name,
                           "JasmineGraph component metric: " + metric_name,
                           "gauge",
                           value,
                           comp_labels);
}

bool PushgatewayUtil::pushCustomMetric(
    const std::string& metric_name,
    const std::string& metric_help,
    const std::string& metric_type,
    double value,
    const std::map<std::string, std::string>& labels) {
    
    if (!initialized_) {
        std::cerr << "PushgatewayUtil not initialized" << std::endl;
        return false;
    }
    
    CURL* curl = curl_easy_init();
    if (!curl) {
        std::cerr << "Failed to initialize CURL" << std::endl;
        return false;
    }
    
    // Build URL
    std::string url = pushgateway_url_ + "/metrics/job/" + job_name_ + "/instance/" + instance_name_;
    
    // Build metrics data in Prometheus format
    std::stringstream metrics_data;
    metrics_data << "# HELP " << metric_name << " " << metric_help << "\n";
    metrics_data << "# TYPE " << metric_name << " " << metric_type << "\n";
    metrics_data << metric_name << "{";
    
    bool first = true;
    for (const auto& label : labels) {
        if (!first) metrics_data << ",";
        metrics_data << label.first << "=\"" << label.second << "\"";
        first = false;
    }
    
    metrics_data << "} " << value << "\n";
    
    std::string metrics_str = metrics_data.str();
    std::string response;
    
    // Set CURL options
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, metrics_str.c_str());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, metrics_str.length());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 10L);
    
    // Set headers
    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: text/plain");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    
    // Perform the request
    CURLcode res = curl_easy_perform(curl);
    
    bool success = (res == CURLE_OK);
    
    if (!success) {
        std::cerr << "Failed to push metrics to Pushgateway: " << curl_easy_strerror(res) << std::endl;
    } else {
        std::cout << "Successfully pushed metric " << metric_name << " = " << value << std::endl;
    }
    
    // Cleanup
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    
    return success;
}

bool PushgatewayUtil::deleteMetrics() {
    if (!initialized_) {
        std::cerr << "PushgatewayUtil not initialized" << std::endl;
        return false;
    }
    
    CURL* curl = curl_easy_init();
    if (!curl) {
        std::cerr << "Failed to initialize CURL" << std::endl;
        return false;
    }
    
    std::string url = pushgateway_url_ + "/metrics/job/" + job_name_ + "/instance/" + instance_name_;
    
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 10L);
    
    CURLcode res = curl_easy_perform(curl);
    bool success = (res == CURLE_OK);
    
    if (!success) {
        std::cerr << "Failed to delete metrics from Pushgateway: " << curl_easy_strerror(res) << std::endl;
    }
    
    curl_easy_cleanup(curl);
    return success;
}

std::string PushgatewayUtil::getMetricsUrl() {
    if (!initialized_) {
        return "";
    }
    return pushgateway_url_ + "/metrics/job/" + job_name_ + "/instance/" + instance_name_;
}
