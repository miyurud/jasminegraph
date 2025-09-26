#pragma once

/**
 * Initialize OpenTelemetry for the JasmineGraph application
 * This should be called once during application startup in main.cpp
 * 
 * Telemetry is disabled by default. To enable, set environment variable:
 * ENABLE_TELEMETRY=true
 */
void initializeOpenTelemetry();

/**
 * Initialize OpenTelemetry for worker processes
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
