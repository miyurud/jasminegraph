#include "../../util/telemetry/OpenTelemetryUtil.h"
#include <iostream>
#include <cstdlib>

// Service type enumeration for telemetry initialization
enum class ServiceType {
    MASTER,
    WORKER
};

// Global flag to control OpenTelemetry initialization
static bool g_telemetry_enabled = false;

/**
 * Initialize OpenTelemetry for JasmineGraph services
 * @param serviceType Type of service (MASTER or WORKER)
 */
void initializeOpenTelemetry(ServiceType serviceType) {
    // Check environment variables to conditionally disable telemetry
    const char* disableTelemetry = std::getenv("DISABLE_TELEMETRY");
    const char* testMode = std::getenv("TEST_MODE");
    const char* testing = std::getenv("TESTING");
    
    if (disableTelemetry && std::string(disableTelemetry) == "true") {
        std::cout << "OpenTelemetry disabled via DISABLE_TELEMETRY environment variable" << std::endl;
        g_telemetry_enabled = false;
        return;
    }
    
    if (testMode && std::string(testMode) == "true") {
        std::cout << "OpenTelemetry disabled in test mode" << std::endl;
        g_telemetry_enabled = false;
        return;
    }
    
    if (testing && std::string(testing) == "true") {
        std::cout << "OpenTelemetry disabled in testing environment" << std::endl;
        g_telemetry_enabled = false;
        return;
    }

    try {
        std::string serviceName;
        std::string otlpEndpoint;
        bool useSimpleProcessor;

        if (serviceType == ServiceType::MASTER) {
            std::cout << "Initializing OpenTelemetry for master..." << std::endl;
            serviceName = "jasminegraph-master";
            otlpEndpoint = "http://tempo:4318/v1/traces";  // Docker-compose network
            useSimpleProcessor = false;  // Use BatchProcessor for master
        } else {
            std::cout << "Initializing OpenTelemetry for worker..." << std::endl;
            serviceName = "jasminegraph-worker";
            otlpEndpoint = "http://172.28.5.1:4318/v1/traces";  // Host IP for worker containers
            useSimpleProcessor = true;   // Use SimpleProcessor for immediate export
        }

        // Initialize with unified configuration
        OpenTelemetryUtil::initialize(
            serviceName,
            otlpEndpoint,
            "http://pushgateway:9091",  // Prometheus endpoint (optional)
            useSimpleProcessor);

        g_telemetry_enabled = true;
        std::cout << "OpenTelemetry initialized successfully for "
                  << (serviceType == ServiceType::MASTER ? "master" : "worker")
                  << (useSimpleProcessor ? " with simple processor" : " with batch processor")
                  << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Failed to initialize OpenTelemetry: " << e.what() << std::endl;
        std::cerr << "Continuing without telemetry..." << std::endl;
        g_telemetry_enabled = false;
    }
}

/**
 * Initialize OpenTelemetry for master (legacy wrapper for backward compatibility)
 */
void initializeOpenTelemetry() {
    initializeOpenTelemetry(ServiceType::MASTER);
}

/**
 * Initialize OpenTelemetry for worker (legacy wrapper for backward compatibility)
 */
void initializeWorkerTelemetry() {
    initializeOpenTelemetry(ServiceType::WORKER);
}

/**
 * Shutdown OpenTelemetry gracefully
 * This should be called before application exit in main.cpp
 */
void shutdownOpenTelemetry() {
    if (g_telemetry_enabled) {
        OpenTelemetryUtil::shutdown();
        g_telemetry_enabled = false;
        std::cout << "OpenTelemetry shutdown completed" << std::endl;
    }
}

/**
 * Check if telemetry is currently enabled
 */
bool isTelemetryEnabled() {
    return g_telemetry_enabled;
}
