/**
Kafka JSONL stream ingestor that buffers records per Δt and forwards to SnapshotManager.
*/

#pragma once

#include <memory>
#include <string>

#include "SnapshotManager.h"

namespace jasminegraph {

class TemporalStreamIngestor {
public:
    struct Config {
        std::string kafkaTopic;
        std::string kafkaConfigPath; // reuse existing config loader
        SnapshotManager::Config snapshotCfg;
    };

    explicit TemporalStreamIngestor(const Config& cfg);
    ~TemporalStreamIngestor();

    // Non-blocking start; uses existing Kafka wrapper to consume.
    void start();
    void stop();

private:
    struct Impl; std::unique_ptr<Impl> impl;
};

} // namespace jasminegraph


