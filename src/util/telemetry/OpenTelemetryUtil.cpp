/**
Copyright 2019-2025 JasmineGraph Team
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

#include "OpenTelemetryUtil.h"
#include <sstream>
#include <vector>
#include <chrono>
#include <iomanip>
#include <array>
#include <atomic>

#include "../logger/Logger.h"

static Logger telemetry_logger;

#ifndef DISABLE_OPENTELEMETRY
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

// Check for testing environment early - disable all OpenTelemetry in tests
static bool isTestingEnvironment() {
    const char* disable_telemetry = std::getenv("DISABLE_TELEMETRY");
    const char* testing = std::getenv("TESTING");
    return (disable_telemetry && std::string(disable_telemetry) == "true") ||
           (testing && std::string(testing) == "true");
}

// Constants for W3C trace-context formatting
namespace {
constexpr int kTraceIdNumBytes = 16;  // 16 bytes = 32 hex chars
constexpr int kSpanIdNumBytes = 8;    // 8 bytes = 16 hex chars
constexpr int kW3CParts = 4;          // version, trace_id, span_id, flags
}

// Helper function to check if OpenTelemetry is properly initialized
bool OpenTelemetryUtil::isInitialized() {
    return tracer_provider_ != nullptr;
}

// OpenTelemetry-specific static members (only when OpenTelemetry is enabled)
nostd::shared_ptr<trace_api::TracerProvider> OpenTelemetryUtil::tracer_provider_{};
nostd::shared_ptr<metrics_api::MeterProvider> OpenTelemetryUtil::meter_provider_{};

// Thread-local storage for worker context management - use safer initialization
thread_local std::unique_ptr<context::Token> OpenTelemetryUtil::context_token_{};
thread_local nostd::shared_ptr<trace_api::Span> OpenTelemetryUtil::parent_span_{};

// Thread-local storage for remote span context from master - use safe initialization
// Use constructor that doesn't fail
thread_local trace_api::SpanContext OpenTelemetryUtil::remote_span_context_{false, false};
thread_local bool OpenTelemetryUtil::has_remote_context_ = false;

#else

// When OpenTelemetry is DISABLED, provide minimal mock implementations without static initialization

// Mock static members for disabled OpenTelemetry (using safe types)
std::shared_ptr<int> OpenTelemetryUtil::tracer_provider_{};
std::shared_ptr<int> OpenTelemetryUtil::meter_provider_{};

// Thread-local storage for worker context management - use safe types
thread_local std::unique_ptr<int> OpenTelemetryUtil::context_token_{};
thread_local std::shared_ptr<int> OpenTelemetryUtil::parent_span_{};

// Thread-local storage for remote span context from master - use mock implementation
thread_local int OpenTelemetryUtil::remote_span_context_{0};
thread_local bool OpenTelemetryUtil::has_remote_context_ = false;

#endif

// Static member definitions - these must be outside conditional compilation
// They are needed regardless of whether OpenTelemetry is enabled or disabled
std::string OpenTelemetryUtil::service_name_ = "";

// Implementation of GetSpan function for both enabled and disabled OpenTelemetry
#ifdef DISABLE_OPENTELEMETRY
// When OpenTelemetry is disabled, implement the mock GetSpan function in trace_api namespace
namespace trace_api {
Span* GetSpan(void*) {
    return nullptr;
}
}
#else
// When OpenTelemetry is enabled, implement GetSpan function using real API
namespace opentelemetry {
namespace trace {
Span* GetSpan(void*) {
    // Convert shared_ptr to raw pointer
    auto span_shared = opentelemetry::trace::GetSpan(opentelemetry::context::RuntimeContext::GetCurrent());
    return span_shared.get();
}
}
}
#endif

#ifndef DISABLE_OPENTELEMETRY

void OpenTelemetryUtil::initialize(const std::string& service_name,
                                  const std::string& otlp_endpoint,
                                  const std::string& prometheus_endpoint,
                                  bool useSimpleProcessor) {
    // Early check for testing environment - completely skip initialization
    if (isTestingEnvironment()) {
        telemetry_logger.info("OpenTelemetry disabled for testing environment");
        return;
    }

    // Check if already initialized
    if (OpenTelemetryUtil::isInitialized()) {
        telemetry_logger.info("OpenTelemetry already initialized, skipping initialization");
        return;
    }

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

        telemetry_logger.info("OpenTelemetry OTLP initialization completed successfully");
    } catch (const std::exception& e) {
        telemetry_logger.error("Failed to initialize OpenTelemetry OTLP exporter: " + std::string(e.what()));
        telemetry_logger.warn("Falling back to console exporter...");

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

        telemetry_logger.info("OpenTelemetry fallback initialization completed with console output");
    }
    telemetry_logger.info("OpenTelemetry initialization completed");
}


nostd::shared_ptr<trace_api::Tracer> OpenTelemetryUtil::getTracer(const std::string& tracer_name) {
    try {
        // Check if telemetry is enabled and tracer provider is initialized
        if (!isEnabled()) {
            // Return a no-op tracer when telemetry is disabled
            auto noop_provider = trace_api::Provider::GetTracerProvider();
            return noop_provider->GetTracer("noop", OPENTELEMETRY_ABI_VERSION);
        }

        auto provider = trace_api::Provider::GetTracerProvider();
        if (provider) {
            return provider->GetTracer(tracer_name, OPENTELEMETRY_ABI_VERSION);
        }
        // Return no-op tracer as fallback
        return trace_api::Provider::GetTracerProvider()->GetTracer("noop", OPENTELEMETRY_ABI_VERSION);
    } catch (...) {
        // Return no-op tracer if anything goes wrong
        return trace_api::Provider::GetTracerProvider()->GetTracer("noop", OPENTELEMETRY_ABI_VERSION);
    }
}

nostd::shared_ptr<metrics_api::Meter> OpenTelemetryUtil::getMeter(const std::string& meter_name) {
    return metrics_api::Provider::GetMeterProvider()->GetMeter(meter_name, OPENTELEMETRY_ABI_VERSION);
}

void OpenTelemetryUtil::shutdown() {
    try {
        telemetry_logger.info("Shutting down OpenTelemetry...");

        // Check if telemetry is initialized
        if (!OpenTelemetryUtil::isInitialized()) {
            telemetry_logger.info("OpenTelemetry not initialized, skipping shutdown");
            return;
        }

        // Force flush all pending traces with error handling
        try {
            auto sdk_provider = dynamic_cast<trace_sdk::TracerProvider*>(tracer_provider_.get());
            if (sdk_provider) {
                // Reduce timeout to prevent hanging
                auto flush_result = sdk_provider->ForceFlush(std::chrono::milliseconds(1000));
                if (!flush_result) {
                    telemetry_logger.warn("OpenTelemetry flush timeout or failed");
                }
                // Call shutdown to properly clean up resources
                sdk_provider->Shutdown();
            }
        } catch (const std::exception& e) {
            telemetry_logger.error("Error during OpenTelemetry flush/shutdown: " + std::string(e.what()));
        } catch (...) {
            telemetry_logger.error("Unknown error during OpenTelemetry flush/shutdown");
        }

        // Reset providers safely
        tracer_provider_ = nullptr;
        meter_provider_ = nullptr;

        // Reset thread-local context token if it exists
        try {
            if (context_token_) {
                context_token_.reset();
                context_token_ = nullptr;
            }
        } catch (...) {
            // Ignore errors during context token cleanup
        }

        // End and reset parent span if it exists
        try {
            if (parent_span_) {
                parent_span_->End();
                parent_span_ = nullptr;
            }
        } catch (...) {
            // Ignore errors during span cleanup
        }

        // Reset remote context safely
        try {
            has_remote_context_ = false;
            remote_span_context_ = trace_api::SpanContext::GetInvalid();
        } catch (...) {
            // Ignore errors during remote context cleanup
        }

        service_name_.clear();

        telemetry_logger.info("OpenTelemetry shutdown completed");
    } catch (const std::exception& e) {
        telemetry_logger.error("Critical error during OpenTelemetry shutdown: " + std::string(e.what()));
    } catch (...) {
        telemetry_logger.error("Unknown critical error during OpenTelemetry shutdown");
    }
}

bool OpenTelemetryUtil::isEnabled() {
    try {
        // Early check for testing environment
        if (isTestingEnvironment()) {
            static std::atomic<bool> logged{false};
            bool expected = false;
            if (logged.compare_exchange_strong(expected, true)) {
                telemetry_logger.info("OpenTelemetry disabled in testing environment");
            }
            return false;
        }

        return OpenTelemetryUtil::isInitialized();
    } catch (...) {
        // If we can't safely check the provider, assume disabled
        return false;
    }
}

// ScopedTracer Implementation
ScopedTracer::ScopedTracer(const std::string& operation_name,
                          const std::map<std::string, std::string>& attributes)
    : operation_name_(operation_name), start_time_(std::chrono::steady_clock::now()),
      span_(nullptr), scope_(nullptr) {

    try {
        // Early check for testing environment - completely skip OpenTelemetry in tests
        if (isTestingEnvironment()) {
            return;
        }

        // Check if telemetry is enabled and properly initialized
        if (!OpenTelemetryUtil::isEnabled()) {
            // Telemetry is disabled or not initialized, skip all OpenTelemetry operations
            return;
        }

        // Get tracer safely
        auto tracer = OpenTelemetryUtil::getTracer();
        if (!tracer) {
            return;
        }

        // Create span with proper parent context inheritance
        trace_api::StartSpanOptions options;

        try {
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
        } catch (...) {
            // If context operations fail, continue without parent context
        }

        // Create span safely
        span_ = tracer->StartSpan(operation_name, options);
        if (!span_) {
            return;
        }

        // Add attributes safely
        try {
            if (!attributes.empty()) {
                for (const auto& attr : attributes) {
                    span_->SetAttribute(attr.first, attr.second);
                }
            }

            // Add default component attribute
            span_->SetAttribute("component", "jasminegraph");
        } catch (...) {
            // Ignore attribute errors - span is still valid
        }

        // Make this span active in the current context safely
        try {
            scope_ = nostd::unique_ptr<trace_api::Scope>(new trace_api::Scope(span_));
        } catch (...) {
            // If scope creation fails, span is still valid for timing
        }
    } catch (...) {
        // Ultimate safety net - ensure we don't crash on construction
        span_ = nullptr;
        scope_ = nullptr;
    }
}

ScopedTracer::~ScopedTracer() {
    try {
        if (span_ && span_->IsRecording()) {
            try {
                // Calculate duration and add only essential timing info
                auto end_time = std::chrono::steady_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time_);

                // Add only milliseconds duration to reduce trace size
                span_->SetAttribute("duration_ms", static_cast<double>(duration.count()));

                // Set success status if not already set
                span_->SetStatus(trace_api::StatusCode::kOk);

                span_->End();
            } catch (...) {
                // Ignore errors during span finalization to prevent crash
            }
        }

        // Scope will be automatically destroyed and context restored
        try {
            scope_.reset();
        } catch (...) {
            // Ignore errors during scope cleanup
        }
    } catch (...) {
        // Ultimate safety net - never throw from destructor
    }
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

            // Convert trace_id (16 bytes) to hex string
            for (int i = 0; i < kTraceIdNumBytes; ++i) {
                oss << std::hex << std::setfill('0') << std::setw(2) << static_cast<int>(trace_id.Id()[i]);
            }

            oss << "-";

            // Convert span_id (8 bytes) to hex string
            for (int i = 0; i < kSpanIdNumBytes; ++i) {
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

        if (static_cast<int>(parts.size()) != kW3CParts) {
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
    std::array<uint8_t, kTraceIdNumBytes> trace_id_bytes = {0};
    std::array<uint8_t, kSpanIdNumBytes> span_id_bytes = {0};

        // Parse trace_id (32 hex chars = 16 bytes)
            for (size_t i = 0; i < kTraceIdNumBytes && i * 2 < trace_id_str.length(); ++i) {
                std::string byte_str = trace_id_str.substr(i * 2, 2);
                trace_id_bytes[i] = static_cast<uint8_t>(std::stoul(byte_str, nullptr, 16));
            }

            // Parse span_id (16 hex chars = 8 bytes)
            for (size_t i = 0; i < kSpanIdNumBytes && i * 2 < span_id_str.length(); ++i) {
                std::string byte_str = span_id_str.substr(i * 2, 2);
                span_id_bytes[i] = static_cast<uint8_t>(std::stoul(byte_str, nullptr, 16));
            }

            // Parse flags
            uint8_t flags = static_cast<uint8_t>(std::stoul(flags_str, nullptr, 16));

            // Create trace and span IDs using nostd::span for v1.16.1 compatibility
            auto trace_id = trace_api::TraceId(nostd::span<const uint8_t, kTraceIdNumBytes>(
                reinterpret_cast<const uint8_t*>(trace_id_bytes.data()), kTraceIdNumBytes));
            auto parent_span_id = trace_api::SpanId(nostd::span<const uint8_t, kSpanIdNumBytes>(
                reinterpret_cast<const uint8_t*>(span_id_bytes.data()), kSpanIdNumBytes));
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
        telemetry_logger.warn("Error setting trace context: " + std::string(e.what()));
    }
}

bool OpenTelemetryUtil::receiveAndSetTraceContext(const std::string& trace_context, const std::string& operation_name) {
    // Check if telemetry is enabled
    if (!OpenTelemetryUtil::isEnabled()) {
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
    if (!OpenTelemetryUtil::isEnabled()) {
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
    if (isTestingEnvironment() || !OpenTelemetryUtil::isInitialized()) {
        return;
    }

    try {
        auto provider = opentelemetry::trace::Provider::GetTracerProvider();
        if (provider) {
            auto sdk_provider = static_cast<opentelemetry::sdk::trace::TracerProvider*>(provider.get());
            // Use ForceFlush directly on the provider
            sdk_provider->ForceFlush(std::chrono::milliseconds(1000));
        }
    } catch (const std::exception& e) {
        // Flush failed - traces may be lost
        telemetry_logger.warn(std::string("Failed to flush OpenTelemetry traces: ") + e.what());
    }
}

#else

// Stub implementations when OpenTelemetry is disabled

void OpenTelemetryUtil::initialize(const std::string& service_name,
                                  const std::string& otlp_endpoint,
                                  const std::string& prometheus_endpoint,
                                  bool useSimpleProcessor) {
    service_name_ = service_name;
    telemetry_logger.info("OpenTelemetry disabled (stub implementation)");
}

nostd::shared_ptr<trace_api::Tracer> OpenTelemetryUtil::getTracer(const std::string& tracer_name) {
    return nostd::shared_ptr<trace_api::Tracer>();
}

nostd::shared_ptr<metrics_api::Meter> OpenTelemetryUtil::getMeter(const std::string& meter_name) {
    return nostd::shared_ptr<metrics_api::Meter>();
}

void OpenTelemetryUtil::shutdown() {
    telemetry_logger.info("OpenTelemetry shutdown (stub implementation)");
}

bool OpenTelemetryUtil::isEnabled() {
    return false;
}

bool OpenTelemetryUtil::isInitialized() {
    return false;
}

std::string OpenTelemetryUtil::getCurrentTraceContext() {
    return "";
}

void OpenTelemetryUtil::setTraceContext(const std::string& context_str) {
    // No-op
}

bool OpenTelemetryUtil::receiveAndSetTraceContext(const std::string& trace_context,
                                                  const std::string& operation_name) {
    return false;
}

void OpenTelemetryUtil::addSpanAttribute(const std::string& key, const std::string& value) {
    // No-op
}

void OpenTelemetryUtil::flushTraces() {
    // No-op
}

// ScopedTracer stub implementation
ScopedTracer::ScopedTracer(const std::string& operation_name,
                          const std::map<std::string, std::string>& attributes)
    : operation_name_(operation_name), start_time_(std::chrono::steady_clock::now()) {
    // No-op
}

ScopedTracer::~ScopedTracer() {
    // No-op
}

void ScopedTracer::addAttribute(const std::string& key, const std::string& value) {
    // No-op
}

void ScopedTracer::addAttributes(const std::map<std::string, std::string>& attributes) {
    // No-op
}

void ScopedTracer::setStatus(trace_api::StatusCode code, const std::string& description) {
    // No-op
}

#endif
