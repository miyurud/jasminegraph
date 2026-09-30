# Distributed Partitioner Specification

**Location**: `src/partitioner/`
**Status**: Reverse-Engineered Spec (Brownfield)
**Date**: 2026-09-21

## 1. Overview
The Partitioner is responsible for dividing massive graph datasets into smaller, manageable chunks that can be distributed across the JasmineGraph cluster. It ensures that the workload is balanced across workers while minimizing cross-partition edges (edge cuts).

## 2. Core Architecture

### 2.1 The Metis Partitioner (`MetisPartitioner.h`)
* **Mechanism**: JasmineGraph integrates with the **METIS** library (a highly optimized C library for graph partitioning). The `MetisPartitioner` reformats raw edge-lists into the METIS format, invokes `partitionWithGPMetis`, and processes the output.
* **Outputs**: It generates multiple types of files:
  * **Partition Files**: The local edges assigned entirely to a specific worker.
  * **Central Store Files**: Edges that cross partition boundaries (edge cuts). These are sent to a shared/central store accessible to multiple workers to resolve distributed queries.
  * **Attribute Files**: Node and edge properties divided to match their respective topological partitions.

### 2.2 Streaming vs Local Partitioning
The directory structure distinguishes between `local` (offline, static graph partitioning) and `stream` (online, dynamic partitioning as data arrives). 

## 3. Implicit Contracts & Constraints (Important for AI Agents)
* **ID Reformatting**: Raw datasets often have non-sequential string or integer IDs. The `MetisPartitioner` strictly reformats these into contiguous integers starting from `1` (via `vertexToIDMap`) to create the sequential format required by METIS (`xadj`, `adjncy`). **Agents must map back to original IDs** (using `idToVertexMap`) before returning results to the user.
* **Central Store Overhead**: AI agents developing graph traversals must explicitly account for the "Central Store". If a query crosses a partition, the local worker must communicate with the master or another worker to fetch the missing edge from the central store.
