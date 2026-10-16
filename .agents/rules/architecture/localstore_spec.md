# LocalStore Specification

**Location**: `src/localstore/`
**Status**: Reverse-Engineered Spec (Brownfield)
**Date**: 2026-09-21

## 1. Overview
While the Partitioner divides the graph, the `LocalStore` is responsible for how those divided edges are actually represented in RAM and on disk within a single worker node.

## 2. Core Architecture

### 2.1 HashMap Local Store (`JasmineGraphHashMapLocalStore.h`)
* **Mechanism**: One of the primary implementations uses nested HashMaps (`std::unordered_map`) or vectors to store adjacency lists locally.
* **Factory Pattern**: The `JasmineGraphLocalStoreFactory` is used to instantiate different types of local stores depending on the graph type or configuration.
### 2.2 Incremental Local Store (`incremental/JasmineGraphIncrementalLocalStore.h`)
* **Mechanism**: Interfaces with the low-level `NativeStore` (`NodeManager`) to support continuous, dynamic edge and property ingestion without reloading partition state.

### 2.3 LocalStore vs CentralStore Separation
* **Intra-Partition Storage**: `LocalStore` strictly manages edges where both source and target vertices reside inside the local partition.
* **Cross-Partition Boundary Edges**: Edges that cross partition boundaries (edge cuts) are stored and managed separately via `CentralStore` (see `centralstore_spec.md`).
