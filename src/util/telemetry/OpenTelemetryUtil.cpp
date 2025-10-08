#include "OpenTelemetryUtil.h"
#include <iostream>
#include <sstream>
#include <vector>
#include <chrono>
#include <iomanip>
#include <array>
#include "opentelemetry/common/key_value_iterable_view.h"
#include "opentelemetry/exporters/otlp/otlp_http_exporter_factory.h"
#include "opentelemetry/exporters/otlp/otlp_http_exporter_options.h"
#include "opentelemetry/sdk/resource/resource.h"
#include "opentelemetry/context/runtime_context.h"
#include "opentelemetry/trace/context.h"
#include "opentelemetry/trace/noop.h"
#include "opentelemetry/nostd/span.h"

using namespace opentelemetry;

// Aliases from official documentation
namespace trace_api = opentelemetry::trace;
namespace trace_sdk = opentelemetry::sdk::trace;
namespace trace_exporter = opentelemetry::exporter::trace;
namespace otlp_exporter = opentelemetry::exporter::otlp;
namespace metrics_api = opentelemetry::metrics;
namespace context_api = opentelemetry::context;

// Static member definitions
std::string OpenTelemetryUtil::service_name_ = "jasminegraph";
nostd::shared_ptr<trace_api::TracerProvider> OpenTelemetryUtil::tracer_provider_;
nostd::shared_ptr<metrics_api::MeterProvider> OpenTelemetryUtil::meter_provider_;

// Thread-local storage for worker context management
thread_local std::unique_ptr<context::Token> OpenTelemetryUtil::context_token_;
thread_local nostd::shared_ptr<trace_api::Span> OpenTelemetryUtil::parent_span_;

// Thread-local storage for remote span context from master
thread_local trace_api::SpanContext OpenTelemetryUtil::remote_span_context_ = trace_api::SpanContext::GetInvalid();
thread_local bool OpenTelemetryUtil::has_remote_context_ = false;

void OpenTelemetryUtil::initialize(const std::string& service_name,
                                  const std::string& otlp_endpoint,
                                  const std::string& prometheus_endpoint,
                                  bool useSimpleProcessor) {
    service_name_ = service_name;

    try {
        // Set up OTLP HTTP exporter with explicit options
        otlp_exporter::OtlpHttpExporterOptions otlp_options;
        otlp_options.url = otlp_endpoint;
        otlp_options.content_type = otlp_exporter::HttpRequestContentType::kJson;

        // Create OTLP HTTP exporter
        auto otlp_http_exporter = otlp_exporter::OtlpHttpExporterFactory::Create(otlp_options);

        // Create resource with service name
        auto resource_attributes = opentelemetry::sdk::resource::ResourceAttributes{
            {"service.name", service_name},
            {"service.version", "1.0.0"}
        };
        auto resource = opentelemetry::sdk::resource::Resource::Create(resource_attributes);

        // Choose processor based on use case
        std::unique_ptr<trace_sdk::SpanProcessor> processor;
        if (useSimpleProcessor) {
            // Use SIMPLE processor for immediate export (workers)
            processor = trace_sdk::SimpleSpanProcessorFactory::Create(std::move(otlp_http_exporter));
        } else {
            // Use BATCH processor with optimized settings for master
            trace_sdk::BatchSpanProcessorOptions batch_options{};
            batch_options.max_queue_size = 512;
            batch_options.schedule_delay_millis = std::chrono::milliseconds(5000);
            batch_options.max_export_batch_size = 128;
            processor = trace_sdk::BatchSpanProcessorFactory::Create(std::move(otlp_http_exporter), batch_options);
        }

        // Create tracer provider with the processor and resource
        auto provider = trace_sdk::TracerProviderFactory::Create(std::move(processor), resource);
        tracer_provider_ = nostd::shared_ptr<trace_api::TracerProvider>(provider.release());

                        // Set the global trace provider
        trace_api::Provider::SetTracerProvider(tracer_provider_);
    } catch (const std::exception& e) {
        std::cerr << "Failed to initialize OpenTelemetry OTLP exporter: " << e.what() << std::endl;
        std::cerr << "Falling back to console exporter..." << std::endl;

        // Create resource with service name for fallback too
        auto fallback_resource = opentelemetry::sdk::resource::Resource::Create({
            {"service.name", service_name},
            {"service.version", "1.0.0"}
        });

        // Fallback - create console exporter
        auto console_exporter = trace_exporter::OStreamSpanExporterFactory::Create();
        auto console_processor = trace_sdk::SimpleSpanProcessorFactory::Create(std::move(console_exporter));
        auto fallback_provider = trace_sdk::TracerProviderFactory::Create(std::move(console_processor),
                                                                            fallback_resource);
        tracer_provider_ = nostd::shared_ptr<trace_api::TracerProvider>(fallback_provider.release());
        trace_api::Provider::SetTracerProvider(tracer_provider_);

        std::cout << "OpenTelemetry fallback initialization completed with console output" << std::endl;
    }
}


nostd::shared_ptr<trace_api::Tracer> OpenTelemetryUtil::getTracer(const std::string& tracer_name) {
    // Check if telemetry is enabled and tracer provider is initialized
    if (!isEnabled()) {
        // Return a no-op tracer when telemetry is disabled
        auto noop_provider = trace_api::Provider::GetTracerProvider();
        return noop_provider->GetTracer("noop", OPENTELEMETRY_ABI_VERSION);
    }
    return trace_api::Provider::GetTracerProvider()->GetTracer(tracer_name, OPENTELEMETRY_ABI_VERSION);
}

nostd::shared_ptr<metrics_api::Meter> OpenTelemetryUtil::getMeter(const std::string& meter_name) {
    return metrics_api::Provider::GetMeterProvider()->GetMeter(meter_name, OPENTELEMETRY_ABI_VERSION);
}

void OpenTelemetryUtil::shutdown() {
    // Check if telemetry is initialized
    if (!isTelemetryEnabled() || !tracer_provider_) {
        return;
    }
    
    if (tracer_provider_) {
        // Flush traces before shutdown

        // Force flush all pending traces
        // We need to get the actual SDK provider to call ForceFlush
        auto sdk_provider = dynamic_cast<trace_sdk::TracerProvider*>(tracer_provider_.get());
        if (sdk_provider) {
            // Force flush with a 5 second timeout
            auto flush_result = sdk_provider->ForceFlush(std::chrono::seconds(5));
            if (flush_result) {
                // Traces flushed successfully
            } else {
                std::cerr << "Warning: OpenTelemetry flush timeout or failed" << std::endl;
            }
        }

        // Now safely reset the provider
        tracer_provider_ = nullptr;
    }

    // Reset context token if it exists
    if (context_token_) {
        context_token_.reset();
    }

    // End and reset parent span if it exists
    if (parent_span_) {
        parent_span_->End();
        parent_span_ = nullptr;
    }

    service_name_.clear();
    std::cout << "OpenTelemetry shutdown completed" << std::endl;
}

bool OpenTelemetryUtil::isEnabled() {
    return tracer_provider_ != nullptr;
}

bool isTelemetryEnabled() {
    return OpenTelemetryUtil::isEnabled();
}

// ScopedTracer Implementation
ScopedTracer::ScopedTracer(const std::string& operation_name,
                          const std::map<std::string, std::string>& attributes)
    : operation_name_(operation_name), start_time_(std::chrono::steady_clock::now()) {

    // Check if telemetry is enabled and properly initialized
    if (!isTelemetryEnabled()) {
        // Telemetry is disabled or not initialized, skip all OpenTelemetry operations
        span_ = nullptr;
        scope_ = nullptr;
        return;
    }

    // Create span with proper parent context

    // Get tracer
    auto tracer = OpenTelemetryUtil::getTracer();

    // Create span with proper parent context inheritance
    trace_api::StartSpanOptions options;

    // Check if we have a remote parent context from setTraceContext()
    if (OpenTelemetryUtil::has_remote_context_ &&
        OpenTelemetryUtil::remote_span_context_.IsValid()) {
        // Use the remote span context as parent (for distributed tracing)
        options.parent = OpenTelemetryUtil::remote_span_context_;
        // Clear the remote context after using it
        OpenTelemetryUtil::has_remote_context_ = false;
    } else {
        // Get the current active context and use it as parent
        auto current_context = context_api::RuntimeContext::GetCurrent();
        auto current_span = trace_api::GetSpan(current_context);

        if (current_span && current_span->GetContext().IsValid()) {
            // Use the current active span as parent (allows proper nesting)
            options.parent = current_span->GetContext();
        }
    }

    // This will now inherit from the appropriate parent context
    span_ = tracer->StartSpan(operation_name, options);

    // Span created successfully

    // Add attributes
    if (!attributes.empty()) {
        for (const auto& attr : attributes) {
            span_->SetAttribute(attr.first, attr.second);
        }
    }

    // Add default component attribute
    span_->SetAttribute("component", "jasminegraph");

    // Make this span active in the current context
    scope_ = nostd::unique_ptr<trace_api::Scope>(new trace_api::Scope(span_));

    // Span scope set and ready for child operations
}

ScopedTracer::~ScopedTracer() {
    if (span_ && span_->IsRecording()) {
        // Calculate duration and add only essential timing info
        auto end_time = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time_);

        // Add only milliseconds duration to reduce trace size
        span_->SetAttribute("duration_ms", static_cast<double>(duration.count()));

        // Set success status if not already set
        span_->SetStatus(trace_api::StatusCode::kOk);

        span_->End();
    }

    // Scope will be automatically destroyed and context restored
    scope_.reset();
}

void ScopedTracer::addAttribute(const std::string& key, const std::string& value) {
    if (span_ && span_->IsRecording()) {
        span_->SetAttribute(key, value);
    }
}

void ScopedTracer::addAttributes(const std::map<std::string, std::string>& attributes) {
    if (span_ && span_->IsRecording()) {
        for (const auto& attr : attributes) {
            span_->SetAttribute(attr.first, attr.second);
        }
    }
}

void ScopedTracer::setStatus(trace_api::StatusCode code, const std::string& description) {
    if (span_ && span_->IsRecording()) {
        span_->SetStatus(code, description);
    }
}

// OpenTelemetryUtil trace context propagation methods implementation

std::string OpenTelemetryUtil::getCurrentTraceContext() {
    try {
        // Get the current active span context
        auto current_context = context_api::RuntimeContext::GetCurrent();
        auto span = trace_api::GetSpan(current_context);
        auto span_context = span->GetContext();

        if (span_context.IsValid()) {
            // Extract trace_id and span_id from current span
            auto trace_id = span_context.trace_id();
            auto span_id = span_context.span_id();
            auto trace_flags = span_context.trace_flags();

            // Format as W3C trace context: version-trace_id-span_id-trace_flags
            std::ostringstream oss;
            oss << "00-";

            // Convert trace_id to hex string
            for (int i = 0; i < 16; ++i) {
                oss << std::hex << std::setfill('0') << std::setw(2) << static_cast<int>(trace_id.Id()[i]);
            }

            oss << "-";

            // Convert span_id to hex string
            for (int i = 0; i < 8; ++i) {
                oss << std::hex << std::setfill('0') << std::setw(2) << static_cast<int>(span_id.Id()[i]);
            }

            oss << "-" << std::hex << std::setfill('0') << std::setw(2) << static_cast<int>(trace_flags.flags());

            std::string context_str = oss.str();
            return context_str;
        } else {
            return "NO_TRACE_CONTEXT";
        }
    } catch (const std::exception& e) {
        return "NO_TRACE_CONTEXT";
    }
}



void OpenTelemetryUtil::setTraceContext(const std::string& context_str) {
    try {
        if (context_str.empty() || context_str == "NO_TRACE_CONTEXT") {
            return;
        }

        // Parse W3C trace context format: version-trace_id-span_id-trace_flags
        std::vector<std::string> parts;
        std::string delimiter = "-";
        std::stringstream ss(context_str);
        std::string item;

        while (std::getline(ss, item, '-')) {
            parts.push_back(item);
        }

        if (parts.size() != 4) {
            return;  // Invalid trace context format
        }

        // Extract components from trace context
        const std::string& version = parts[0];
        const std::string& trace_id_str = parts[1];
        const std::string& span_id_str = parts[2];
        const std::string& flags_str = parts[3];

        // Validate component lengths
        if (trace_id_str.length() != 32 || span_id_str.length() != 16 || version != "00") {
            return;  // Invalid trace context format
        }

        // Convert hex strings to byte arrays
        std::array<uint8_t, 16> trace_id_bytes = {0};
        std::array<uint8_t, 8> span_id_bytes = {0};

        // Parse trace_id (32 hex chars = 16 bytes)
            for (size_t i = 0; i < 16 && i * 2 < trace_id_str.length(); ++i) {
                std::string byte_str = trace_id_str.substr(i * 2, 2);
                trace_id_bytes[i] = static_cast<uint8_t>(std::stoul(byte_str, nullptr, 16));
            }

            // Parse span_id (16 hex chars = 8 bytes)
            for (size_t i = 0; i < 8 && i * 2 < span_id_str.length(); ++i) {
                std::string byte_str = span_id_str.substr(i * 2, 2);
                span_id_bytes[i] = static_cast<uint8_t>(std::stoul(byte_str, nullptr, 16));
            }

            // Parse flags
            uint8_t flags = static_cast<uint8_t>(std::stoul(flags_str, nullptr, 16));

            // Create trace and span IDs using nostd::span for v1.16.1 compatibility
            auto trace_id = trace_api::TraceId(nostd::span<const uint8_t, 16>(
                reinterpret_cast<const uint8_t*>(trace_id_bytes.data()), 16));
            auto parent_span_id = trace_api::SpanId(nostd::span<const uint8_t, 8>(
                reinterpret_cast<const uint8_t*>(span_id_bytes.data()), 8));
            auto trace_flags = trace_api::TraceFlags(flags);

            // Create a span context with the parent information
            auto span_context = trace_api::SpanContext(trace_id, parent_span_id, trace_flags, true);

            if (span_context.IsValid()) {
                // Store the remote span context for use in ScopedTracer
                remote_span_context_ = span_context;
                has_remote_context_ = true;
            } else {
                // Invalid span context created from trace context
            }
    } catch (const std::exception& e) {
        std::cerr << "Error setting trace context: " << e.what() << std::endl;
    }
}

bool OpenTelemetryUtil::receiveAndSetTraceContext(const std::string& trace_context, const std::string& operation_name) {
    // Check if telemetry is enabled
    if (!isTelemetryEnabled()) {
        return false;
    }
    
    // Validate and set trace context if it's valid
    if (trace_context != "NO_TRACE_CONTEXT" && !trace_context.empty()) {
        setTraceContext(trace_context);
        // Trace context set successfully for distributed tracing
        return true;
    } else {
        // No valid trace context received from master
        return false;
    }
}

void OpenTelemetryUtil::addSpanAttribute(const std::string& key, const std::string& value) {
    // Check if telemetry is enabled
    if (!isTelemetryEnabled()) {
        return;
    }
    
    try {
        // Get current context and span
        auto current_context = context::RuntimeContext::GetCurrent();
        auto current_span = trace_api::GetSpan(current_context);

        if (current_span && current_span->GetContext().IsValid()) {
            current_span->SetAttribute(key, value);
        }
    } catch (const std::exception& e) {
        // Attribute setting failed - continue execution
    }
}

void OpenTelemetryUtil::flushTraces() {
    try {
        if (tracer_provider_) {
            auto sdk_provider = dynamic_cast<trace_sdk::TracerProvider*>(tracer_provider_.get());
            if (sdk_provider) {
                // Force flush with a 2 second timeout
                sdk_provider->ForceFlush(std::chrono::seconds(2));
            }
        }
    } catch (const std::exception& e) {
        // Flush failed - traces may be lost
    }
}
