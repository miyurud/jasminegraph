#include "TemporalIntegration.h"

namespace jasminegraph {

std::unique_ptr<TemporalFacade> TemporalIntegration::facade;

void TemporalIntegration::initialize(const std::string& baseDir, std::chrono::seconds snapshotInterval) {
    if (facade) return;
    facade.reset(new TemporalFacade(baseDir, snapshotInterval));
}

TemporalFacade* TemporalIntegration::instance() {
    return facade.get();
}

} // namespace jasminegraph


