/**
Simple integration hook exposing a process-wide temporal facade instance.
Existing components can call TemporalIntegration::instance() to access temporal APIs.
*/

#pragma once

#include <chrono>
#include <memory>
#include <string>

#include "TemporalFacade.h"

namespace jasminegraph {

class TemporalIntegration {
public:
    // Initialize global facade. Safe to call multiple times; subsequent calls are ignored.
    static void initialize(const std::string& baseDir, std::chrono::seconds snapshotInterval);

    // Access the global facade. Returns nullptr if not initialized.
    static TemporalFacade* instance();

private:
    static std::unique_ptr<TemporalFacade> facade;
};

} // namespace jasminegraph


