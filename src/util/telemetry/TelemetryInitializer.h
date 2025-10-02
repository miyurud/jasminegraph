#pragma once

// Service type enumeration for telemetry initialization
enum class ServiceType {
    MASTER,
    WORKER
};

/**
 * Initialize OpenTelemetry for JasmineGraph services (unified API)
 * @param serviceType Type of service (ServiceType::MASTER or ServiceType::WORKER)
 * 
 * This is the recommended way to initialize telemetry:
 * - Master: Uses BatchProcessor with docker-compose Tempo endpoint
 * - Worker: Uses SimpleProcessor with host IP Tempo endpoint for immediate export
 */
void initializeOpenTelemetry(ServiceType serviceType);

/**
 * Initialize OpenTelemetry for master (legacy wrapper)
 * This should be called once during application startup in main.cpp
 */
void initializeOpenTelemetry();

/**
 * Initialize OpenTelemetry for worker processes (legacy wrapper)
 * This should be called once during worker startup
 */
void initializeWorkerTelemetry();

/**
 * Shutdown OpenTelemetry gracefully  
 * This should be called before application exit in main.cpp
 */
void shutdownOpenTelemetry();

/**
 * Check if telemetry is currently enabled
 */
bool isTelemetryEnabled();
