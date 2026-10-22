# CentralStore Specification

**Location**: `src/centralstore/`
**Status**: Reverse-Engineered Spec (Brownfield)
**Date**: 2026-10-09

## 1. Overview
In JasmineGraph's distributed graph partitioning model, edges whose source and target vertices reside within the same partition are stored in the `LocalStore`. Conversely, cross-partition edges ("edge cuts" or boundary edges) are managed by the `CentralStore`. The `CentralStore` ensures distributed query engines can resolve cross-boundary adjacencies and multi-partition algorithms without requiring full graph replication.

## 2. Core Architecture

### 2.1 Central Store Implementations
* **`JasmineGraphHashMapCentralStore.h`**:
  * Inherits from `JasmineGraphLocalStore`.
  * Maintains the boundary adjacency structure in memory using `centralSubgraphMap` (`std::map<long, std::unordered_set<long>>`).
  * Provides in-degree and out-degree distributions for boundary vertices (`getInDegreeDistributionHashMap`, `getOutDegreeDistributionHashMap`).
* **`JasmineGraphHashMapDuplicateCentralStore.h`**:
  * Extends the base HashMap store to maintain duplicate entries for boundary edges across both incident partition directions.
  * Ensures symmetric lookups when traversing inter-partition connections from either worker.

### 2.2 Serialization & Binary Layout
* **FlatBuffers Encoding**:
  * Boundary edge mappings are serialized using FlatBuffers (`PartEdgeMapStore`, `EdgeStoreEntry`, `AttributeStore`).
  * Files are named using the convention `<graphId>_centralstore_<partitionId>` and duplicate files `<graphId>_centralstore_dp_<partitionId>`.
* **Composite Central Stores**:
  * When partition counts exceed configured cluster thresholds (`COMPOSITE_CENTRAL_STORE_WORKER_THRESHOLD`), `MetisPartitioner` groups and merges partition boundary sets into composite central stores (`<part1>_<part2>_centralstore`).
  * This grouping minimizes network socket overhead during distributed triangle counting and aggregations.

## 3. Distributed Query Protocols & Aggregation

### 3.1 Worker Communication (`JasmineGraphInstanceService.cpp`)
* **`send_centralstore_to_aggregator_command`**: Sends compressed boundary store files to designated aggregator worker instances.
* **`aggregate_centralstore_triangles_command` / `aggregate_composite_centralstore_triangles_command`**:
  * Coordinates triangle counting over boundary edges.
  * Aggregator workers load neighboring boundary graphs into memory and intersect central store adjacencies against local stores.

## 4. Implicit Contracts & Constraints (Important for AI Agents)
* **Separation from LocalStore**: Never insert cross-partition boundary edges into local store instances. Local stores must only contain edges completely internal to that worker's partition ID.
* **File Naming & Path Conventions**: Always resolve central store paths using `org.jasminegraph.server.instance.datafolder` and `org.jasminegraph.server.instance.aggregatefolder`.
* **Memory Management**: Central store FlatBuffer allocations are read directly from memory-mapped or heap buffers; ensure data buffers are properly deleted after deserialization to prevent memory leaks in worker processes.
