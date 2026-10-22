# TemporalStore Specification

**Location**: `src/temporalstore/`
**Status**: Reverse-Engineered Spec (Brownfield)
**Date**: 2026-09-16

## 1. Overview
The `TemporalStore` component is responsible for managing and querying historical graph snapshots within JasmineGraph. It provides the core functionality for Temporal Graph algorithms by tracking the exact lifespan of edges and the evolution of node/edge properties over time.

## 2. Core Architecture

The system is designed around highly optimized, memory-efficient data structures to handle large-scale temporal data:

### 2.1 Edge Lifespan Tracking (`EdgeLifespanBitmap.h`)
Instead of storing multiple copies of the graph for each snapshot, the store uses **Roaring Bitmaps** (`CRoaring` library) to track edge existence.
* **Mechanism**: Every unique edge (Source $\rightarrow$ Destination) has a dedicated `EdgeLifespanBitmap`. Each bit index in the bitmap corresponds to a `snapshotId`. If bit `N` is set to `1`, the edge existed in snapshot `N`.

### 2.2 Property Tracking (`PropertyIntervalDictionary.h`)
While edge existence is tracked via bitmaps, the properties of nodes and edges are tracked using a `PropertyIntervalDictionary`. This dictionary allows the store to add or update properties at specific snapshot intervals and query the exact property value at any historical `snapshotId`.

### 2.3 Snapshot Management (`SnapshotManager.h`)
The lifecycle of a snapshot is managed by `SnapshotManager`. Snapshots are defined by either time thresholds (e.g., every 60 seconds) or edge thresholds (e.g., every 10,000 edges).

## 3. Key Operations & Algorithms

### 3.1 Edge Insertion (`addEdge`)
When a new edge is added:
1. The system locks the internal state using a `std::mutex`.
2. It looks up or creates the `EdgeKey` in the `edgeBitmaps_` hash map.
3. It sets the bit corresponding to the `currentSnapshotId` to `true`.
4. It notifies the `SnapshotManager` to record the edge, which may trigger a snapshot rotation if thresholds are met.

### 3.2 Temporal Querying (`TemporalQueryExecutor.h`, `TemporalStore.h`)
Temporal querying supports point-in-time verification, time-window aggregations, and edge evolution tracking:
* **Edge Existence Check (`edgeExistsAtSnapshot`)**: Determines whether an edge existed at a specific snapshot by checking the corresponding bit index in the edge's `EdgeLifespanBitmap`.
* **Snapshot Edge Retrieval (`getEdgesAtSnapshot`)**: Scans registered edge bitmaps to return all active directed edges for a target snapshot ID.
* **Property Inspection (`getNodePropertyAtSnapshot`, `getEdgePropertyAtSnapshot`)**: Queries historical node and edge attributes valid at a given snapshot via interval dictionary lookups.
* **Time Range & Evolution Tracking (`trackEdgeEvolution`, `getEdgesInTimeRange`)**: Reconstructs state transitions and extracts topological deltas across consecutive snapshot windows.

### 3.3 Streaming Triangle Counting (`countTrianglesAtSnapshot`)
An optimized algorithm is implemented for streaming triangle counting over temporal snapshots:
1. **Index Mapping**: It maps string node IDs to contiguous `uint32_t` indices for the active edges in the requested snapshot.
2. **Adjacency Bitmaps**: It builds an undirected adjacency list where each node's neighbors are stored in a Roaring Bitmap.
3. **Bitmap Intersection**: For each edge `(u, v)`, it computes the intersection of `neighbors(u)` and `neighbors(v)` using bitmap bitwise AND (`roaring_bitmap_and`). The cardinality of this intersection equals the number of common neighbors (i.e., triangles containing `(u, v)`).

## 4. Persistence Mechanism
To prevent memory exhaustion, the `TemporalStore` implements a windowed persistence model:
* **`saveBitmapIndexToDisk`**: Persists the in-memory window as an append-only delta file and flushes the in-memory bitmaps.
* **`appendSnapshotMetaToDisk`**: Saves metadata about the snapshot (total edges, new edges) to a binary file.
* **`loadBitmapIndexFromDisk`**: Restores the `edgeBitmaps_` state into memory from a `.ebm` file for querying.

## 5. Implicit Contracts & Constraints (Important for AI Agents)
* **Thread Safety**: The `TemporalStore` uses a coarse-grained `std::mutex` (`mutex_`) for almost all operations. When implementing new features, ensure that the lock is held as briefly as possible to avoid bottlenecking concurrent insertions. (e.g., `SnapshotManager` counter updates happen *outside* the lock in `addEdge`).
* **Memory Limits**: The system relies on the persistence layer to flush bitmaps. If the persistence thresholds are bypassed, the `edgeBitmaps_` unordered map will cause OOM errors.
* **Directed vs Undirected**: The core storage treats edges as directed (`sourceId` $\rightarrow$ `destId`), but specific algorithms like `countTrianglesAtSnapshot` internally treat them as undirected to find common neighbors.
