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

#ifndef JASMINEGRAPH_TELEMETRYUTIL_H
#define JASMINEGRAPH_TELEMETRYUTIL_H

#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>
#include <thread>
#include <atomic>
#include "MetricMetadata.h"
#include "TraceContext.h"

class TelemetryUtil {
private:
    static bool initialized_;
    static std::unordered_map<std::string, uint64_t> counter_values_;
    static std::unordered_map<std::string, double> histogram_values_;
    
    // Push gateway integration
    static int push_interval_seconds_;
    static std::thread push_thread_;
    static std::atomic<bool> should_push_;

public:
    // Initialize telemetry with optional automatic push interval
    static void initialize(const std::string& push_gateway_url = "", 
                          const std::string& job_name = "jasminegraph-telemetry",
                          int push_interval_seconds = 30);
    
    // Cleanup telemetry (lightweight cleanup for worker threads)
    static void cleanup();
    
    // Shutdown telemetry completely (for main process only)
    static void shutdown();

    // Record triangle count metric
    static void recordTriangleCount(uint64_t count, const std::string& graph_id);

    // Record function execution time
    static void recordExecutionTime(const std::string& function_name, double duration_ms, 
                                   const std::string& graph_id = "");

    // Record function call count
    static void recordFunctionCall(const std::string& function_name, const std::string& graph_id = "");

    // Rich metadata methods
    static void recordMetricWithMetadata(const JasmineGraph::Telemetry::MetricMetadata& metric);
    static void recordTriangleCountWithMetadata(uint64_t count, const std::string& graph_id, 
                                               const std::string& algorithm = "default");
    static void recordExecutionTimeWithMetadata(const std::string& function_name, double duration_ms,
                                                const std::string& graph_id = "", 
                                                const std::string& algorithm = "default");

    // Common helper functions to reduce code duplication
    static void recordWorkerMetric(const std::string& metric_name, const std::string& value,
                                  const std::string& graph_id, const std::string& worker_id = "",
                                  const std::string& partition_id = "", const std::string& host = "",
                                  const std::string& algorithm = "partition_local");
    
    static void recordComponentMetric(const std::string& metric_name, const std::string& value,
                                     const std::string& graph_id, const std::string& component,
                                     const std::string& algorithm = "", const std::string& extra_key = "",
                                     const std::string& extra_value = "");
    
    static void recordExecutionMetric(const std::string& operation, const std::string& status,
                                     const std::string& graph_id, const std::string& component = "executor",
                                     const std::string& algorithm = "distributed");
    
    static void recordErrorMetric(const std::string& error_type, const std::string& graph_id,
                                 const std::string& component = "system", const std::string& host = "");

    // Distributed Tracing methods
    static JasmineGraph::Telemetry::TraceContext startTrace(const std::string& operation_name);
    static JasmineGraph::Telemetry::TraceContext startChildTrace(const std::string& operation_name, 
                                                                   const JasmineGraph::Telemetry::TraceContext& parent);
    static void finishTrace(const JasmineGraph::Telemetry::TraceContext& trace, 
                           const std::string& graph_id = "");

    // Push gateway integration (using existing Utils::send_job)
    static void pushAllMetricsToGateway();
    static void pushSingleMetric(const std::string& metric_name, const std::string& metric_value);

private:
    static void pushMetricsLoop();

public:

    // Utility class for automatic timing
    class Timer {
    private:
        std::string function_name_;
        std::string graph_id_;
        std::chrono::high_resolution_clock::time_point start_time_;
        
    public:
        Timer(const std::string& function_name, const std::string& graph_id = "");
        ~Timer();
    };
    
    // Enhanced timer with distributed tracing
    class TracedTimer {
    private:
        JasmineGraph::Telemetry::TraceContext trace_;
        std::string graph_id_;
        
    public:
        TracedTimer(const std::string& operation_name, const std::string& graph_id = "");
        TracedTimer(const std::string& operation_name, const JasmineGraph::Telemetry::TraceContext& parent, 
                    const std::string& graph_id = "");
        ~TracedTimer();
        
        // Get trace context for creating child operations
        const JasmineGraph::Telemetry::TraceContext& getTrace() const { return trace_; }
    };
};

// Macro for easy function timing
#define TELEMETRY_TIME_FUNCTION(graph_id) TelemetryUtil::Timer timer(__FUNCTION__, graph_id)
#define TELEMETRY_TIME_FUNCTION_NAMED(name, graph_id) TelemetryUtil::Timer timer(name, graph_id)

// Macros for distributed tracing
#define TELEMETRY_TRACE_FUNCTION(graph_id) TelemetryUtil::TracedTimer tracer(__FUNCTION__, graph_id)
#define TELEMETRY_TRACE_CHILD(parent_tracer, operation, graph_id) \
    TelemetryUtil::TracedTimer tracer(operation, parent_tracer.getTrace(), graph_id)

#endif //JASMINEGRAPH_TELEMETRYUTIL_H
