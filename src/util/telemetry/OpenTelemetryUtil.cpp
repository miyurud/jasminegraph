#include "OpenTelemetryUtil.h"
#include <iostream>
#include "opentelemetry/common/key_value_iterable_view.h"
#include "opentelemetry/exporters/otlp/otlp_http_exporter_factory.h"
#include "opentelemetry/exporters/otlp/otlp_http_exporter_options.h"
#include "opentelemetry/sdk/resource/resource.h"

using namespace opentelemetry;

// Aliases from official documentation
namespace trace_api = opentelemetry::trace;
namespace trace_sdk = opentelemetry::sdk::trace;
namespace trace_exporter = opentelemetry::exporter::trace;
namespace otlp_exporter = opentelemetry::exporter::otlp;
namespace metrics_api = opentelemetry::metrics;

// Static member definitions
std::string OpenTelemetryUtil::service_name_ = "jasminegraph";
nostd::shared_ptr<trace_api::TracerProvider> OpenTelemetryUtil::tracer_provider_;
nostd::shared_ptr<metrics_api::MeterProvider> OpenTelemetryUtil::meter_provider_;

void OpenTelemetryUtil::initialize(const std::string& service_name, 
                                  const std::string& prometheus_endpoint,
                                  const std::string& otlp_endpoint) {
    service_name_ = service_name;
    
    try {
        // Determine the trace export endpoint
        std::string trace_endpoint = otlp_endpoint.empty() ? "http://localhost:4318/v1/traces" : otlp_endpoint;
        
        std::cout << "OpenTelemetry initializing for service: " << service_name << std::endl;
        std::cout << "Traces will be sent to: " << trace_endpoint << std::endl;
        
        // Create OTLP HTTP exporter for sending traces to Tempo
        otlp_exporter::OtlpHttpExporterOptions otlp_options;
        otlp_options.url = trace_endpoint;
        otlp_options.content_type = otlp_exporter::HttpRequestContentType::kJson;
        
        // Create OTLP HTTP exporter
        auto otlp_http_exporter = otlp_exporter::OtlpHttpExporterFactory::Create(otlp_options);
        
        // Create resource with service name
        auto resource_attributes = opentelemetry::sdk::resource::ResourceAttributes{
            {"service.name", service_name}
        };
        auto resource = opentelemetry::sdk::resource::Resource::Create(resource_attributes);
        
        // Create batch span processor with default settings for optimal performance and lower overhead
        trace_sdk::BatchSpanProcessorOptions batch_options{};
        auto processor = trace_sdk::BatchSpanProcessorFactory::Create(std::move(otlp_http_exporter), batch_options);
        
        // Create tracer provider with the processor and resource
        auto provider = trace_sdk::TracerProviderFactory::Create(std::move(processor), resource);
        tracer_provider_ = nostd::shared_ptr<trace_api::TracerProvider>(provider.release());
        
        // Set the global trace provider
        trace_api::Provider::SetTracerProvider(tracer_provider_);
        
        std::cout << "OpenTelemetry initialized successfully for service: " << service_name << std::endl;
        std::cout << "Using OTLP HTTP exporter for traces to Tempo" << std::endl;
        
        if (!prometheus_endpoint.empty()) {
            std::cout << "Prometheus endpoint: " << prometheus_endpoint << std::endl;
        }
        
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
        auto fallback_provider = trace_sdk::TracerProviderFactory::Create(std::move(console_processor), fallback_resource);
        tracer_provider_ = nostd::shared_ptr<trace_api::TracerProvider>(fallback_provider.release());
        trace_api::Provider::SetTracerProvider(tracer_provider_);
        
        std::cout << "OpenTelemetry fallback initialization completed with console output" << std::endl;
    }
}

nostd::shared_ptr<trace_api::Tracer> OpenTelemetryUtil::getTracer(const std::string& tracer_name) {
    return trace_api::Provider::GetTracerProvider()->GetTracer(tracer_name, OPENTELEMETRY_ABI_VERSION);
}

nostd::shared_ptr<metrics_api::Meter> OpenTelemetryUtil::getMeter(const std::string& meter_name) {
    return metrics_api::Provider::GetMeterProvider()->GetMeter(meter_name, OPENTELEMETRY_ABI_VERSION);
}

void OpenTelemetryUtil::shutdown() {
    if (tracer_provider_) {
        std::cout << "Flushing OpenTelemetry traces before shutdown..." << std::endl;
        
        // Force flush all pending traces
        // We need to get the actual SDK provider to call ForceFlush
        auto sdk_provider = dynamic_cast<trace_sdk::TracerProvider*>(tracer_provider_.get());
        if (sdk_provider) {
            // Force flush with a 5 second timeout
            auto flush_result = sdk_provider->ForceFlush(std::chrono::seconds(5));
            if (flush_result) {
                std::cout << "OpenTelemetry traces flushed successfully" << std::endl;
            } else {
                std::cerr << "Warning: OpenTelemetry flush timeout or failed" << std::endl;
            }
        }
        
        // Now safely reset the provider
        tracer_provider_ = nullptr;
    }
    
    service_name_.clear();
    std::cout << "OpenTelemetry shutdown completed" << std::endl;
}

// ScopedTracer Implementation
ScopedTracer::ScopedTracer(const std::string& operation_name, 
                          const std::map<std::string, std::string>& attributes)
    : operation_name_(operation_name), start_time_(std::chrono::steady_clock::now()) {
    
    // Check if telemetry is enabled - if not, this becomes a no-op
    if (!isTelemetryEnabled()) {
        span_ = nullptr;
        scope_ = nullptr;
        return;
    }
    
    // Get tracer
    auto tracer = OpenTelemetryUtil::getTracer();
    
    // Create span with proper parent context inheritance
    trace_api::StartSpanOptions options;
    
    // This will automatically inherit from the current active span context
    span_ = tracer->StartSpan(operation_name, options);
    
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
}

ScopedTracer::~ScopedTracer() {
    if (span_ && span_->IsRecording()) {
        // Calculate duration
        auto end_time = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time_);
        
        // Add duration as attribute
        span_->SetAttribute("duration_microseconds", static_cast<double>(duration.count()));
        span_->SetAttribute("duration_milliseconds", static_cast<double>(duration.count()) / 1000.0);
        
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
