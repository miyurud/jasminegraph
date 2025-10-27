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
 * This is the recommended way to initialize telemetry. The OTLP endpoint is
 * set to the docker-compose Tempo service (http://tempo:4318/v1/traces) 
 * Master uses a BatchProcessor; worker uses SimpleProcessor for immediate export.
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
