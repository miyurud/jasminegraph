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

## 3. Implicit Contracts & Constraints (Important for AI Agents)
* **Memory Exhaustion**: Storing large graphs in HashMaps has significant memory overhead due to pointer chasing and load factors. Agents attempting to optimize graph traversals should avoid duplicating this data and instead pass references or indices.
* **Abstraction Breach**: Agents must interact with local data through the `JasmineGraphLocalStore` abstract interface rather than casting to the `HashMap` implementation, ensuring compatibility if the system upgrades to a Compressed Sparse Row (CSR) format in the future.
