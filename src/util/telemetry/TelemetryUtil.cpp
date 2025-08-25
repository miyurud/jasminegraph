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

#include "TelemetryUtil.h"
#include "../logger/Logger.h"
#include "../Utils.h"
#include <sstream>
#include <mutex>
#include <iomanip>
#include <thread>
#include <atomic>

// Static member definitions
bool TelemetryUtil::initialized_ = false;
std::unordered_map<std::string, uint64_t> TelemetryUtil::counter_values_;
std::unordered_map<std::string, double> TelemetryUtil::histogram_values_;
int TelemetryUtil::push_interval_seconds_ = 0;
std::thread TelemetryUtil::push_thread_;
std::atomic<bool> TelemetryUtil::should_push_{false};

static Logger telemetry_logger;
static std::mutex telemetry_mutex;

void TelemetryUtil::initialize(const std::string& push_gateway_url, 
                               const std::string& job_name,
                               int push_interval_seconds) {
    if (initialized_) {
        return;
    }

    try {
        initialized_ = true;
        push_interval_seconds_ = push_interval_seconds;
        
        // Start push thread if interval is specified
        if (push_interval_seconds > 0) {
            should_push_ = true;
            push_thread_ = std::thread(&TelemetryUtil::pushMetricsLoop);
            telemetry_logger.info("Telemetry initialized with automatic push every " + std::to_string(push_interval_seconds) + " seconds");
        } else {
            telemetry_logger.info("Telemetry initialized in manual push mode");
        }
        
        // Add some basic startup metrics
        std::lock_guard<std::mutex> lock(telemetry_mutex);
        counter_values_["jasminegraph_startup_total"] = 1;
        counter_values_["jasminegraph_telemetry_initialized"] = 1;
        
    } catch (const std::exception& e) {
        telemetry_logger.error("Failed to initialize Telemetry: " + std::string(e.what()));
    }
}

void TelemetryUtil::cleanup() {
    if (!initialized_) {
        return;
    }
    
    // Don't clear metrics data or set initialized to false
    // This prevents worker threads from clearing global metrics
    // Only log that cleanup was requested
    telemetry_logger.info("Telemetry cleanup completed");
}

void TelemetryUtil::shutdown() {
    if (!initialized_) {
        return;
    }
    
    // Stop push thread
    if (push_interval_seconds_ > 0) {
        should_push_ = false;
        if (push_thread_.joinable()) {
            push_thread_.join();
        }
        
        // Final push of metrics
        pushAllMetricsToGateway();
    }
    
    std::lock_guard<std::mutex> lock(telemetry_mutex);
    
    // Clear storage
    counter_values_.clear();
    histogram_values_.clear();
    
    initialized_ = false;
    telemetry_logger.info("Telemetry shutdown completed");
}

void TelemetryUtil::recordTriangleCount(uint64_t count, const std::string& graph_id) {
    if (!initialized_) {
        initialize();
    }
    
    std::lock_guard<std::mutex> lock(telemetry_mutex);
    std::string metric_key = "jasminegraph_triangle_count";
    if (!graph_id.empty()) {
        metric_key += "_graph_" + graph_id;
    }
    
    counter_values_[metric_key] += count;
    telemetry_logger.info("Recorded triangle count: " + std::to_string(count) + 
                         (graph_id.empty() ? "" : " for graph: " + graph_id));
    
    // Optionally push immediately for important metrics
    // pushSingleMetric(metric_key, std::to_string(counter_values_[metric_key]));
}

void TelemetryUtil::recordExecutionTime(const std::string& function_name, double duration_ms, 
                                       const std::string& graph_id) {
    if (!initialized_) {
        initialize();
    }
    
    std::lock_guard<std::mutex> lock(telemetry_mutex);
    std::string metric_key = "jasminegraph_function_duration_" + function_name;
    if (!graph_id.empty()) {
        metric_key += "_graph_" + graph_id;
    }
    
    histogram_values_[metric_key] = duration_ms; // Store latest value
    telemetry_logger.info("Recorded execution time for " + function_name + ": " + 
                         std::to_string(duration_ms) + "ms" +
                         (graph_id.empty() ? "" : " for graph: " + graph_id));
}

void TelemetryUtil::recordFunctionCall(const std::string& function_name, const std::string& graph_id) {
    if (!initialized_) {
        initialize();
    }
    
    std::lock_guard<std::mutex> lock(telemetry_mutex);
    std::string metric_key = "jasminegraph_function_calls_" + function_name;
    if (!graph_id.empty()) {
        metric_key += "_graph_" + graph_id;
    }
    
    counter_values_[metric_key]++;
    telemetry_logger.info("Recorded function call for " + function_name + 
                         (graph_id.empty() ? "" : " for graph: " + graph_id));
}

// Timer implementation
TelemetryUtil::Timer::Timer(const std::string& function_name, const std::string& graph_id)
    : function_name_(function_name), graph_id_(graph_id) {
    start_time_ = std::chrono::high_resolution_clock::now();
    recordFunctionCall(function_name_, graph_id_);
}

TelemetryUtil::Timer::~Timer() {
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time_);
    recordExecutionTime(function_name_, static_cast<double>(duration.count()), graph_id_);
}

// Push Gateway integration using existing Utils::send_job()
void TelemetryUtil::pushMetricsLoop() {
    while (should_push_) {
        std::this_thread::sleep_for(std::chrono::seconds(push_interval_seconds_));
        if (should_push_) {
            pushAllMetricsToGateway();
        }
    }
}

void TelemetryUtil::pushAllMetricsToGateway() {
    std::lock_guard<std::mutex> lock(telemetry_mutex);
    
    // Push all counter metrics using existing Utils::send_job()
    for (const auto& entry : counter_values_) {
        Utils::send_job("telemetry", entry.first, std::to_string(entry.second));
    }
    
    // Push all histogram metrics using existing Utils::send_job()
    for (const auto& entry : histogram_values_) {
        Utils::send_job("telemetry", entry.first, std::to_string(entry.second));
    }
    
    telemetry_logger.info("Pushed " + std::to_string(counter_values_.size() + histogram_values_.size()) + 
                         " telemetry metrics to push gateway");
}

void TelemetryUtil::pushSingleMetric(const std::string& metric_name, const std::string& metric_value) {
    Utils::send_job("telemetry", metric_name, metric_value);
}

// Rich Metadata Implementation
void TelemetryUtil::recordMetricWithMetadata(const JasmineGraph::Telemetry::MetricMetadata& metric) {
    if (!initialized_) {
        initialize();
    }
    
    // Use the Prometheus formatted string as the metric name (includes labels)
    std::string prometheus_format = metric.toPrometheusFormat();
    
    // Extract just the value part
    size_t space_pos = prometheus_format.rfind(' ');
    if (space_pos != std::string::npos) {
        std::string metric_key = prometheus_format.substr(0, space_pos);
        std::string metric_value = prometheus_format.substr(space_pos + 1);
        
        // Send to push gateway with rich metadata
        Utils::send_job("telemetry", metric_key, metric_value);
        
        telemetry_logger.info("Recorded metric with metadata: " + metric.name + 
                             " = " + metric.value + 
                             (metric.unit.empty() ? "" : " " + metric.unit));
    }
}

void TelemetryUtil::recordTriangleCountWithMetadata(uint64_t count, const std::string& graph_id, 
                                                   const std::string& algorithm) {
    using namespace JasmineGraph::Telemetry;
    
    MetricMetadata metric("jasminegraph_triangle_count", std::to_string(count), 
                         MetricType::COUNTER, "triangles", 
                         "Total number of triangles found in graph");
    
    metric.addLabel("graph_id", graph_id)
          .addLabel("algorithm", algorithm)
          .addLabel("component", "core");
    
    recordMetricWithMetadata(metric);
}

void TelemetryUtil::recordExecutionTimeWithMetadata(const std::string& function_name, double duration_ms,
                                                    const std::string& graph_id, 
                                                    const std::string& algorithm) {
    using namespace JasmineGraph::Telemetry;
    
    MetricMetadata metric("jasminegraph_function_duration", std::to_string(duration_ms), 
                         MetricType::HISTOGRAM, "milliseconds", 
                         "Execution time of " + function_name + " function");
    
    metric.addLabel("function_name", function_name)
          .addLabel("component", "core");
    
    if (!graph_id.empty()) {
        metric.addLabel("graph_id", graph_id);
    }
    if (!algorithm.empty()) {
        metric.addLabel("algorithm", algorithm);
    }
    
    recordMetricWithMetadata(metric);
}

// Common helper functions to reduce code duplication
void TelemetryUtil::recordWorkerMetric(const std::string& metric_name, const std::string& value,
                                      const std::string& graph_id, const std::string& worker_id,
                                      const std::string& partition_id, const std::string& host,
                                      const std::string& algorithm) {
    using namespace JasmineGraph::Telemetry;
    
    MetricMetadata metric(metric_name, value, MetricType::COUNTER, 
                         metric_name.find("count") != std::string::npos ? "triangles" : "tasks",
                         "Worker-related metric for " + metric_name);
    
    metric.addLabel("graph_id", graph_id)
          .addLabel("component", "worker")
          .addLabel("algorithm", algorithm);
    
    if (!worker_id.empty()) {
        metric.addLabel("worker_id", worker_id);
    }
    if (!partition_id.empty()) {
        metric.addLabel("partition_id", partition_id);
    }
    if (!host.empty()) {
        metric.addLabel("worker_host", host);
    }
    
    recordMetricWithMetadata(metric);
}

void TelemetryUtil::recordComponentMetric(const std::string& metric_name, const std::string& value,
                                         const std::string& graph_id, const std::string& component,
                                         const std::string& algorithm, const std::string& extra_key,
                                         const std::string& extra_value) {
    using namespace JasmineGraph::Telemetry;
    
    MetricMetadata metric(metric_name, value, MetricType::COUNTER,
                         metric_name.find("count") != std::string::npos ? "triangles" : "tasks",
                         "Component metric for " + component);
    
    metric.addLabel("graph_id", graph_id)
          .addLabel("component", component);
    
    if (!algorithm.empty()) {
        metric.addLabel("algorithm", algorithm);
    }
    if (!extra_key.empty() && !extra_value.empty()) {
        metric.addLabel(extra_key, extra_value);
    }
    
    recordMetricWithMetadata(metric);
}

void TelemetryUtil::recordExecutionMetric(const std::string& operation, const std::string& status,
                                         const std::string& graph_id, const std::string& component,
                                         const std::string& algorithm) {
    using namespace JasmineGraph::Telemetry;
    
    MetricMetadata metric("jasminegraph_execution_status", status == "success" ? "1" : "0",
                         MetricType::GAUGE, "",
                         operation + " execution status");
    
    metric.addLabel("graph_id", graph_id)
          .addLabel("status", status)
          .addLabel("operation", operation)
          .addLabel("component", component)
          .addLabel("algorithm", algorithm);
    
    recordMetricWithMetadata(metric);
}

void TelemetryUtil::recordErrorMetric(const std::string& error_type, const std::string& graph_id,
                                     const std::string& component, const std::string& host) {
    using namespace JasmineGraph::Telemetry;
    
    MetricMetadata metric("jasminegraph_worker_errors", "1",
                         MetricType::COUNTER, "errors",
                         "Error occurrences in system components");
    
    metric.addLabel("error_type", error_type)
          .addLabel("component", component);
    
    if (!graph_id.empty()) {
        metric.addLabel("graph_id", graph_id);
    }
    if (!host.empty()) {
        metric.addLabel("host", host);
    }
    
    recordMetricWithMetadata(metric);
}

// Distributed Tracing Implementation
JasmineGraph::Telemetry::TraceContext TelemetryUtil::startTrace(const std::string& operation_name) {
    return JasmineGraph::Telemetry::TraceContext(operation_name);
}

JasmineGraph::Telemetry::TraceContext TelemetryUtil::startChildTrace(const std::string& operation_name, 
                                                                      const JasmineGraph::Telemetry::TraceContext& parent) {
    JasmineGraph::Telemetry::TraceContext child = parent.createChild(operation_name);
    telemetry_logger.info("Started CHILD trace: " + operation_name + 
                         " (trace_id=" + child.getTraceId() + 
                         ", span_id=" + child.getSpanId() + 
                         ", parent_span_id=" + child.getParentSpanId() + ")");
    return child;
}

void TelemetryUtil::finishTrace(const JasmineGraph::Telemetry::TraceContext& trace, 
                                const std::string& graph_id) {
    // Create a copy to finish (since we need to modify it)
    JasmineGraph::Telemetry::TraceContext mutable_trace = trace;
    double duration = mutable_trace.finish();
    
    // Record the trace as a rich metadata metric
    using namespace JasmineGraph::Telemetry;
    
    // Add timestamp to make each trace unique
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    
    MetricMetadata trace_metric("jasminegraph_trace_duration", std::to_string(duration),
                               MetricType::HISTOGRAM, "milliseconds",
                               "Distributed trace duration for " + trace.getOperationName());
    
    trace_metric.addLabel("operation", trace.getOperationName())
                .addLabel("trace_id", trace.getTraceId())
                .addLabel("span_id", trace.getSpanId())
                .addLabel("timestamp", std::to_string(timestamp))
                .addLabel("component", "trace");
    
    // Always add parent_span_id label - use "null" for root spans
    if (!trace.getParentSpanId().empty()) {
        trace_metric.addLabel("parent_span_id", trace.getParentSpanId());
    } else {
        trace_metric.addLabel("parent_span_id", "null");
    }
    
    if (!graph_id.empty()) {
        trace_metric.addLabel("graph_id", graph_id);
    }
    
    // Push directly with unique job name to prevent overwriting in push gateway
    std::string prometheus_format = trace_metric.toPrometheusFormat();
    size_t space_pos = prometheus_format.rfind(' ');
    if (space_pos != std::string::npos) {
        std::string metric_key = prometheus_format.substr(0, space_pos);
        std::string metric_value = prometheus_format.substr(space_pos + 1);
        
        // Use unique job name for each trace to prevent overwriting in push gateway
        std::string unique_job = "trace_" + trace.getSpanId().substr(0, 8);
        Utils::send_job(unique_job, metric_key, metric_value);
        
        telemetry_logger.info("Pushed trace to unique job: " + unique_job + " - " + metric_key);
    }
    
    telemetry_logger.info("Finished trace: " + trace.getOperationName() + 
                         " (trace_id=" + trace.getTraceId() + 
                         ", span_id=" + trace.getSpanId() +
                         ", parent_span_id=" + (trace.getParentSpanId().empty() ? "null" : trace.getParentSpanId()) +
                         ", duration=" + std::to_string(duration) + "ms)");
}

// TracedTimer Implementation
TelemetryUtil::TracedTimer::TracedTimer(const std::string& operation_name, const std::string& graph_id)
    : trace_(startTrace(operation_name)), graph_id_(graph_id) {
    telemetry_logger.info("Started ROOT trace: " + operation_name + 
                         " (trace_id=" + trace_.getTraceId() + 
                         ", span_id=" + trace_.getSpanId() + ")");
}

TelemetryUtil::TracedTimer::TracedTimer(const std::string& operation_name, 
                                        const JasmineGraph::Telemetry::TraceContext& parent,
                                        const std::string& graph_id)
    : trace_(startChildTrace(operation_name, parent)), graph_id_(graph_id) {
    telemetry_logger.info("Started CHILD trace: " + operation_name + 
                         " (trace_id=" + trace_.getTraceId() + 
                         ", span_id=" + trace_.getSpanId() +
                         ", parent_span_id=" + trace_.getParentSpanId() + ")");
}

TelemetryUtil::TracedTimer::~TracedTimer() {
    finishTrace(trace_, graph_id_);
}
