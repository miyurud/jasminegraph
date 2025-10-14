#include "TemporalStreamIngestor.h"

#include <atomic>
#include <thread>

#include "../util/kafka/KafkaCC.h"

namespace jasminegraph {

struct TemporalStreamIngestor::Impl {
    Config cfg;
    std::unique_ptr<SnapshotManager> snapshotMgr;
    std::atomic<bool> running{false};
    std::thread worker;

    Impl(const Config& c)
        : cfg(c), snapshotMgr(new SnapshotManager(c.snapshotCfg)) {}
};

TemporalStreamIngestor::TemporalStreamIngestor(const Config& cfg) : impl(new Impl(cfg)) {}
TemporalStreamIngestor::~TemporalStreamIngestor() { stop(); }

void TemporalStreamIngestor::start() {
    if (impl->running.exchange(true)) return;
    impl->worker = std::thread([this]() {
        // TODO: Use KafkaCC to consume JSONL messages; parse into StreamEdgeRecord and call ingest.
        while (impl->running.load()) {
            // Placeholder: sleep/yield
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    });
}

void TemporalStreamIngestor::stop() {
    if (!impl->running.exchange(false)) return;
    if (impl->worker.joinable()) impl->worker.join();
}

} // namespace jasminegraph


