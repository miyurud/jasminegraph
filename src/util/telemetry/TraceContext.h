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

#ifndef JASMINEGRAPH_TRACECONTEXT_H
#define JASMINEGRAPH_TRACECONTEXT_H

#include <string>
#include <random>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <thread>

namespace JasmineGraph {
namespace Telemetry {

class TraceContext {
private:
    std::string trace_id_;
    std::string span_id_;
    std::string parent_span_id_;
    std::string operation_name_;
    std::chrono::high_resolution_clock::time_point start_time_;
    bool is_finished_;

public:
    // Constructor for root span (no parent)
    TraceContext(const std::string& operation_name) 
        : operation_name_(operation_name), is_finished_(false) {
        trace_id_ = generateTraceId();
        span_id_ = generateSpanId();
        parent_span_id_ = "";
        start_time_ = std::chrono::high_resolution_clock::now();
    }
    
    // Constructor for child span
    TraceContext(const std::string& operation_name, const TraceContext& parent)
        : operation_name_(operation_name), is_finished_(false) {
        trace_id_ = parent.trace_id_;  // Same trace
        span_id_ = generateSpanId();   // New span
        parent_span_id_ = parent.span_id_;  // Parent reference
        start_time_ = std::chrono::high_resolution_clock::now();
    }
    
    // Getters
    const std::string& getTraceId() const { return trace_id_; }
    const std::string& getSpanId() const { return span_id_; }
    const std::string& getParentSpanId() const { return parent_span_id_; }
    const std::string& getOperationName() const { return operation_name_; }
    
    // Create child span
    TraceContext createChild(const std::string& child_operation) const {
        return TraceContext(child_operation, *this);
    }
    
    // Finish span and return duration
    double finish() {
        if (is_finished_) return 0.0;
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time_);
        is_finished_ = true;
        
        return static_cast<double>(duration.count());
    }
    
    // Get trace headers for HTTP propagation
    std::string getTraceHeader() const {
        return "traceid=" + trace_id_ + ";spanid=" + span_id_;
    }
    
    // Parse trace headers from HTTP request
    static TraceContext fromTraceHeader(const std::string& header, const std::string& operation_name) {
        // Simple parsing: traceid=abc123;spanid=def456
        std::string trace_id, span_id;
        
        size_t trace_pos = header.find("traceid=");
        size_t span_pos = header.find("spanid=");
        
        if (trace_pos != std::string::npos) {
            size_t start = trace_pos + 8;  // length of "traceid="
            size_t end = header.find(';', start);
            if (end == std::string::npos) end = header.length();
            trace_id = header.substr(start, end - start);
        }
        
        if (span_pos != std::string::npos) {
            size_t start = span_pos + 7;  // length of "spanid="
            size_t end = header.find(';', start);
            if (end == std::string::npos) end = header.length();
            span_id = header.substr(start, end - start);
        }
        
        TraceContext context(operation_name);
        if (!trace_id.empty() && !span_id.empty()) {
            context.trace_id_ = trace_id;
            context.parent_span_id_ = span_id;
            context.span_id_ = context.generateSpanId();  // New span for this operation
        }
        
        return context;
    }

private:
    // Generate random trace ID (16 bytes = 32 hex chars)
    std::string generateTraceId() {
        return generateHexId(16);
    }
    
    // Generate random span ID (8 bytes = 16 hex chars)
    std::string generateSpanId() {
        return generateHexId(8);
    }
    
    // Generate random hex ID of specified byte length
    std::string generateHexId(int bytes) {
        static std::random_device rd;
        static std::mt19937 gen(rd());
        static std::uniform_int_distribution<> dis(0, 255);
        
        std::stringstream ss;
        for (int i = 0; i < bytes; ++i) {
            ss << std::hex << std::setw(2) << std::setfill('0') << dis(gen);
        }
        return ss.str();
    }
};

} // namespace Telemetry
} // namespace JasmineGraph

#endif //JASMINEGRAPH_TRACECONTEXT_H
