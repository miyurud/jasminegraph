# StreamingDB Specification

**Location**: `src/streamingdb/`
**Status**: Reverse-Engineered Spec (Brownfield)
**Date**: 2026-09-21

## 1. Overview
The StreamingDB component handles real-time graph mutations, which is distinct from the static, offline batch ingestion handled by the `MetisPartitioner`.

## 2. Core Architecture

### 2.1 Streaming State Management (`StreamingSQLiteDBInterface.h`)
* **Mechanism**: Similar to `metadb`, it uses an SQLite backend specifically tailored for fast-moving streaming metadata.
* **Buffering**: It acts as an intermediary buffer. Instead of halting query execution to restructure the graph every time a new edge arrives, the streaming DB tracks deltas.

## 3. Implicit Contracts & Constraints (Important for AI Agents)
* **Delta Resolution**: When an AI agent writes a query algorithm (e.g., checking if a path exists), it must ensure the executor checks BOTH the static `LocalStore` AND the `StreamingDB` buffers to get the most up-to-date view of the graph.
* **Flush Thresholds**: The streaming buffers will eventually flush and merge into the static partitions. Agents modifying ingestion must respect these flush thresholds to prevent disk thrashing.
