/**
Copyright 2026 JasminGraph Team
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

#include "JasmineGraphIncrementalLocalStore.h"
#include "../../temporalstore/TemporalStore.h"
#include "../../temporalstore/DataAggregator.h"

/**
 * Enable temporal storage for this incremental store
 */
void JasmineGraphIncrementalLocalStore::enableTemporalStorage(
    TemporalStore* store, DataAggregator* aggregator) {

    if (!store || !aggregator) {
        return;  // Invalid parameters
    }

    temporalStore = store;
    dataAggregator = aggregator;
    temporalEnabled = true;
}

/**
 * Disable temporal storage
 */
void JasmineGraphIncrementalLocalStore::disableTemporalStorage() {
    temporalEnabled = false;
    temporalStore = nullptr;
    dataAggregator = nullptr;
}

/**
 * Record edge addition to temporal store
 */
void JasmineGraphIncrementalLocalStore::recordEdgeAddition(
    const std::string& source, const std::string& dest) {

    if (!temporalEnabled || !temporalStore || !dataAggregator) {
        return;
    }

    // Get current snapshot ID
    uint32_t currentSnapshot = temporalStore->getCurrentSnapshotId();

    // Buffer the edge update
    dataAggregator->bufferEdgeUpdate(
        gc.partitionID,  // partition ID
        source,
        dest,
        currentSnapshot,
        true);  // isAddition = true

    // Check if we should flush
    if (dataAggregator->shouldFlush(gc.partitionID)) {
        flushTemporalUpdates();
    }

    // Check if we should create new snapshot
    if (temporalStore->shouldCreateSnapshot()) {
        temporalStore->closeCurrentSnapshot();
        temporalStore->openNewSnapshot();
    }
}

/**
 * Record edge deletion to temporal store
 */
void JasmineGraphIncrementalLocalStore::recordEdgeDeletion(
    const std::string& source, const std::string& dest) {

    if (!temporalEnabled || !temporalStore || !dataAggregator) {
        return;
    }

    // Get current snapshot ID
    uint32_t currentSnapshot = temporalStore->getCurrentSnapshotId();

    // Buffer the edge removal
    dataAggregator->bufferEdgeUpdate(
        gc.partitionID,
        source,
        dest,
        currentSnapshot,
        false);  // isAddition = false

    // Check if we should flush
    if (dataAggregator->shouldFlush(gc.partitionID)) {
        flushTemporalUpdates();
    }
}

/**
 * Record property update to temporal store
 */
void JasmineGraphIncrementalLocalStore::recordPropertyUpdate(
    const std::string& nodeOrEdgeId,
    const std::string& key,
    const std::string& value,
    bool isNodeProperty) {

    if (!temporalEnabled || !temporalStore || !dataAggregator) {
        return;
    }

    // Get current snapshot ID
    uint32_t currentSnapshot = temporalStore->getCurrentSnapshotId();

    // Buffer the property update
    dataAggregator->bufferPropertyUpdate(
        gc.partitionID,
        nodeOrEdgeId,
        key,
        value,
        currentSnapshot,
        isNodeProperty);
}

/**
 * Flush buffered temporal updates to TemporalStore
 */
void JasmineGraphIncrementalLocalStore::flushTemporalUpdates() {
    if (!temporalEnabled || !temporalStore || !dataAggregator) {
        return;
    }

    // Flush this partition's buffer
    dataAggregator->flushPartition(gc.partitionID, temporalStore);
}
