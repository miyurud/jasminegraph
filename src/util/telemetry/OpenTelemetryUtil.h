#pragma once

#include <opentelemetry/trace/provider.h>
#include <opentelemetry/trace/tracer.h>
#include <opentelemetry/trace/span.h>
#include <opentelemetry/trace/scope.h>
#include <opentelemetry/metrics/provider.h>
#include <opentelemetry/metrics/meter.h>
#include <opentelemetry/common/key_value_iterable.h>
#include <opentelemetry/context/propagation/global_propagator.h>
#include <opentelemetry/context/propagation/text_map_propagator.h>
#include <opentelemetry/context/context.h>

// Forward declaration for telemetry status check
bool isTelemetryEnabled();

// Official SDK includes based on OpenTelemetry C++ documentation
#include <opentelemetry/exporters/ostream/span_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter_options.h>
#include <opentelemetry/sdk/trace/exporter.h>
#include <opentelemetry/sdk/trace/processor.h>
#include <opentelemetry/sdk/trace/simple_processor_factory.h>
#include <opentelemetry/sdk/trace/batch_span_processor_factory.h>
#include <opentelemetry/sdk/trace/tracer_provider_factory.h>

#include <string>
#include <map>
#include <chrono>
#include <memory>

namespace telemetry = opentelemetry;
namespace trace_api = telemetry::trace;
namespace metrics_api = telemetry::metrics;
namespace nostd = telemetry::nostd;
namespace context = telemetry::context;

/**
 * Modern OpenTelemetry-based telemetry utility providing automatic parent-child tracing
 * and simplified metric recording. Replaces custom telemetry implementation.
 */
class OpenTelemetryUtil {
public:
    /**
     * Initialize OpenTelemetry with Prometheus and OTLP exporters
     * @param service_name Name of the service for tracing
     * @param prometheus_endpoint Prometheus push gateway endpoint
     * @param otlp_endpoint OTLP collector endpoint (optional)
     */
    static void initialize(const std::string& service_name, 
                          const std::string& prometheus_endpoint = "http://localhost:9091",
                          const std::string& otlp_endpoint = "");

    /**
     * Get the global tracer instance
     * @param tracer_name Name for the tracer (typically component name)
     * @return Shared pointer to tracer
     */
    static nostd::shared_ptr<trace_api::Tracer> getTracer(const std::string& tracer_name = "jasminegraph");

    /**
     * Get the global meter instance for metrics
     * @param meter_name Name for the meter (typically component name)
     * @return Shared pointer to meter
     */
    static nostd::shared_ptr<metrics_api::Meter> getMeter(const std::string& meter_name = "jasminegraph");

    /**
     * Shutdown telemetry system gracefully
     * Should be called before application exit
     */
    static void shutdown();
private:
    static std::string service_name_;
    static nostd::shared_ptr<trace_api::TracerProvider> tracer_provider_;
    static nostd::shared_ptr<metrics_api::MeterProvider> meter_provider_;
};

/**
 * RAII class for automatic span lifecycle management
 * Use this for timing operations with automatic span creation and cleanup
 */
class ScopedTracer {
public:
    ScopedTracer(const std::string& operation_name, 
                 const std::map<std::string, std::string>& attributes = {});
    
    ~ScopedTracer();

    // Add attributes during execution
    void addAttribute(const std::string& key, const std::string& value);
    void addAttributes(const std::map<std::string, std::string>& attributes);
    
    // Set span status
    void setStatus(trace_api::StatusCode code, const std::string& description = "");

private:
    nostd::shared_ptr<trace_api::Span> span_;
    nostd::unique_ptr<trace_api::Scope> scope_;
    std::chrono::steady_clock::time_point start_time_;
    std::string operation_name_;
};

/**
 * Convenience macro for automatic function tracing
 * Creates a scoped tracer that automatically traces the entire function
 * When telemetry is disabled, this becomes a no-op
 */
#define OTEL_TRACE_FUNCTION() \
    ScopedTracer __function_tracer(__FUNCTION__)

/**
 * Convenience macro for automatic operation tracing
 * @param op_name Name of the operation to trace
 */
#define OTEL_TRACE_OPERATION(op_name) \
    ScopedTracer __operation_tracer(op_name)

/**
 * Convenience macro for tracing with attributes
 * @param op_name Name of the operation
 * @param attrs Map of attributes
 */
#define OTEL_TRACE_WITH_ATTRS(op_name, attrs) \
    ScopedTracer __operation_tracer(op_name, attrs)
