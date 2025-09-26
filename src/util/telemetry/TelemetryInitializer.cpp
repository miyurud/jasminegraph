#include "../../util/telemetry/OpenTelemetryUtil.h"
#include <iostream>
#include <cstdlib>

// Global flag to control OpenTelemetry initialization
static bool g_telemetry_enabled = false;

/**
 * Initialize OpenTelemetry for the JasmineGraph application
 * This should be called once during application startup in main.cpp
 */
void initializeOpenTelemetry() {
    try {
        std::cout << "Initializing OpenTelemetry..." << std::endl;
        
        // Initialize with proper endpoints when telemetry is enabled
        OpenTelemetryUtil::initialize(
            "jasminegraph-master",
            "http://pushgateway:9091",         // Pushgateway hostname
            "http://tempo:4318/v1/traces"      // Tempo hostname
        );
        
        g_telemetry_enabled = true;
        std::cout << "OpenTelemetry initialized successfully" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Failed to initialize OpenTelemetry: " << e.what() << std::endl;
        std::cerr << "Continuing without telemetry..." << std::endl;
        g_telemetry_enabled = false;
    }
}

/**
 * Initialize OpenTelemetry for worker processes
 * This should be called once during worker startup
 */
void initializeWorkerTelemetry() {
    try {
        std::cout << "Initializing OpenTelemetry for worker..." << std::endl;
        
        // Initialize with worker service name and force simple processor for immediate export
        // Use host IP for Tempo since worker containers are not in docker-compose network
        OpenTelemetryUtil::initializeWithSimpleProcessor(
            "jasminegraph-worker",
            "http://pushgateway:9091",         // Pushgateway hostname
            "http://172.28.5.1:4318/v1/traces" // Tempo via host IP
        );
        
        g_telemetry_enabled = true;
        std::cout << "OpenTelemetry initialized successfully for worker with simple processor" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Failed to initialize OpenTelemetry for worker: " << e.what() << std::endl;
        std::cerr << "Continuing without telemetry..." << std::endl;
        g_telemetry_enabled = false;
    }
}

/**
 * Check if telemetry is currently enabled
 */
bool isTelemetryEnabled() {
    return g_telemetry_enabled;
}

/**
 * Shutdown OpenTelemetry gracefully
 * This should be called before application exit in main.cpp
 */
void shutdownOpenTelemetry() {
    OpenTelemetryUtil::shutdown();
    std::cout << "OpenTelemetry shutdown completed" << std::endl;
}
