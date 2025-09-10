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
    // Check environment variable to enable/disable telemetry
    const char* enable_telemetry = std::getenv("ENABLE_TELEMETRY");
    
    if (enable_telemetry && std::string(enable_telemetry) == "true") {
        try {
            std::cout << "Initializing OpenTelemetry..." << std::endl;
            
            // Initialize with proper endpoints when telemetry is enabled
            OpenTelemetryUtil::initialize(
                "jasminegraph-master",
                "http://172.17.0.2:9091",         // Pushgateway IP
                "http://172.17.0.3:4318/v1/traces" // Tempo IP
            );
            
            g_telemetry_enabled = true;
            std::cout << "OpenTelemetry initialized successfully" << std::endl;
            
        } catch (const std::exception& e) {
            std::cerr << "Failed to initialize OpenTelemetry: " << e.what() << std::endl;
            std::cerr << "Continuing without telemetry..." << std::endl;
            g_telemetry_enabled = false;
        }
    } else {
        std::cout << "OpenTelemetry disabled - skipping initialization" << std::endl;
        std::cout << "To enable: docker run -e ENABLE_TELEMETRY=true ..." << std::endl;
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
