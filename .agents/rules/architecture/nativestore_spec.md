# NativeStore Specification

**Location**: `src/nativestore/`
**Status**: Reverse-Engineered Spec (Brownfield)
**Date**: 2026-10-09

## 1. Overview
The `NativeStore` is JasmineGraph's low-level, custom binary storage engine for persistent graph storage. Unlike generic key-value or document databases, it implements a slotted, fixed-size block storage layout directly on raw disk files, enabling $O(1)$ block-address lookups, linked-list relationship pointer traversal, and incremental edge ingestion.

## 2. Core Architecture & Block Layout

The engine operates on pre-structured binary database files scoped per partition (`<instanceDataFolder>/g<graphId>_p<partitionId>_*`):

### 2.1 Node Management (`NodeManager.h`, `NodeBlock.h`)
* **`NodeBlock` (`_nodes.db`)**:
  * Fixed block size: `BLOCK_SIZE = 140` bytes (`LABEL_SIZE = 118` bytes).
  * Stores node metadata, in-use flag, label string, pointers to edge relations (`edgeRef`), central edge cuts (`centralEdgeRef`, `edgeRefPID`), property head (`propRef`), and metadata property head (`metaPropRef`).
  * Block offset calculation: `address = blockIndex * NodeBlock::BLOCK_SIZE`.
* **Index Files**:
  * `_nodes.index.db`: Maps external string node IDs to contiguous integer block indices (`nodeIndex`).
  * `_edgeIndex.db`: Tracks edge sequential indices (`edgeIndex`).

### 2.2 Relationship Storage (`RelationBlock.h`)
* **`RelationBlock` (`_relations.db` and `_central_relations.db`)**:
  * Employs doubly-linked relationship records connecting incident nodes.
  * Fields include `source`, `destination`, `sourceNext`, `sourcePrevious`, `destinationNext`, `destinationPrevious`, next partition IDs (`nextPid`), relation type, and property pointers (`propertyAddress`, `metaPropertyAddress`).
  * Maintains separate files for local intra-partition relationships (`_relations.db`, `BLOCK_SIZE`) versus inter-partition central cuts (`_central_relations.db`, `CENTRAL_BLOCK_SIZE`).

### 2.3 Property Storage (`PropertyLink.h`, `MetaPropertyLink.h`, `PropertyEdgeLink.h`)
* **Linked Property Chains (`_properties.db`, `_edge_properties.db`)**:
  * Stored as singly linked list blocks on disk (`PROPERTY_BLOCK_SIZE = MAX_NAME_SIZE (30) + MAX_VALUE_SIZE (400) + sizeof(unsigned int)`).
  * If a node or edge has multiple properties, each block points to the address of the next property block.

### 2.4 Streaming Ingestion & Networking (`DataPublisher.h`)
* **`DataPublisher`**:
  * Connects over TCP to worker data ports (`SERVER_DATA_PORT`).
  * Streams incoming edge records and inter-partition cuts directly to the worker's native store ingestion loop (`publishBatch`, `publish_central_relation`).
* **`JasmineGraphIncrementalLocalStore.h`**:
  * Integrates `NodeManager` with streaming ingest pipelines, enabling dynamic ingestion of JSON-formatted edge streams and properties without full-graph reloading.

## 3. Implicit Contracts & Constraints (Important for AI Agents)
* **Block Size Alignment**: All reads and writes to `_nodes.db`, `_relations.db`, and `_central_relations.db` MUST strictly align with their respective block size constants. Corrupting block boundaries causes cascading index deserialization failures.
* **Thread Safety**: Disk modifications in `NodeManager` and property links are synchronized via POSIX mutexes (`lockNodeAdd`, `lockEdgeAdd`, `lockPropertyLink`). AI agents extending native store operations must acquire the appropriate mutex before mutating file cursors (`seekp`/`seekg`).
* **Local vs Central Pointers**: Always maintain the distinction between local edge pointers (`edgeRef`) and cross-partition edge cuts (`centralEdgeRef`). Cross-partition traversals require tracking the target partition ID (`edgeRefPID`).
