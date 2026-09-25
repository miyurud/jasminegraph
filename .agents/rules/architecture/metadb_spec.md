# MetaDB Specification

**Location**: `src/metadb/`
**Status**: Reverse-Engineered Spec (Brownfield)
**Date**: 2026-09-21

## 1. Overview
The MetaDB component serves as the global registry and source of truth for the distributed JasmineGraph cluster. It tracks which graphs are loaded, where their partitions physically reside, and the health status of worker nodes.

## 2. Core Architecture

### 2.1 The Interface (`SQLiteDBInterface.h`)
* **Mechanism**: Implemented as a wrapper around an embedded SQLite database (`sqlite3` library).
* **Usage**: The `JasmineGraphServer` instantiates this interface at startup and uses it to persistently track cluster topology across reboots.

### 2.2 State Management
* **Graph Registry**: Maps a human-readable `graphId` to its internal partitioning metadata.
* **Worker Registry**: Tracks IP addresses, ports, and the specific partition IDs assigned to each worker.

## 3. Implicit Contracts & Constraints (Important for AI Agents)
* **Single Source of Truth**: Any distributed feature added to JasmineGraph (like a new Temporal Index) MUST register its metadata through `SQLiteDBInterface`. If an agent hardcodes state into memory without persisting it here, the cluster will permanently lose track of that data if the Master node restarts.
* **Thread Safety**: SQLite connections can be sensitive to concurrent threaded access depending on how they are compiled. Agents should use the provided `sqliteMutex` in the `Utils` class when modifying the MetaDB concurrently.
