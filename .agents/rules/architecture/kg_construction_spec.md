# Knowledge Graph Construction Specification

**Location**: `src/knowledgegraph/construction/`
**Status**: Reverse-Engineered Spec (Brownfield)
**Date**: 2026-09-21

## 1. Overview
The Knowledge Graph Construction component handles the ingestion of raw, unstructured text (from local files or HDFS) and dynamically converts it into a structured temporal knowledge graph by extracting tuples (Subject, Predicate, Object) using Large Language Models (LLMs).

## 2. Core Architecture

### 2.1 Ingestion Pipeline (`Pipeline.h`)
The `Pipeline` is the master orchestrator for data ingestion.
* **Mechanism**: It reads data from HDFS or local storage in chunks.
* **Distribution**: It pushes these text chunks (`Chunk` struct containing `doc_id`, `text`, `chunk_size`) to a thread-safe `dataBuffer`. It is responsible for streaming these chunks to designated worker nodes (`JasmineGraphServer::worker`) in the cluster for distributed processing.
* **Entity Resolution**: The pipeline maintains global `nodeIndex` and `edgeIndex` maps (guarded by `entityResolutionMutex`) to ensure that identical entities extracted from different chunks are mapped to the same internal IDs.

### 2.2 LLM Tuple Streamers (`OllamaTupleStreamer.h` for local Ollama, `VLLMTupleStreamer.h` for vLLM)
Instead of waiting for an LLM to process an entire document, the system streams the extraction process to minimize latency and memory usage.
* **Mechanism**: Inherits from a base `TupleStreamer`. Uses `libcurl` (or similar HTTP clients) to connect to local or remote LLM inference engines (Ollama or vLLM).
* **Streaming Parser**: Implements a `StreamCallback` that parses incoming JSON chunks from the LLM in real-time. It uses depth-tracking state variables (`braceDepth`/`bracketDepth` in Ollama, `braceDepth`/`braceStack` in vLLM) to track and extract partial JSON objects (tuples) as they arrive over the network.
* **Buffering**: Extracted tuples are pushed directly into a thread-safe `SharedBuffer` for immediate insertion into the graph partition.

## 3. Implicit Contracts & Constraints (Important for AI Agents)
* **Real-time Parsing Vulnerability**: The `OllamaTupleStreamer::StreamContext` parses incomplete JSON strings character-by-character to detect complete tuples quickly. AI agents MUST NOT replace this with standard `nlohmann::json::parse()` on partial streams, as it will throw exceptions. Once a complete tuple substring is detected by the StreamContext, standard parsing can then be safely applied to that complete chunk.
* **Distributed Synchronization**: The `Pipeline` class heavily uses `std::mutex`, `std::condition_variable`, and `std::atomic`. When modifying the buffer logic, agents must ensure `dataBufferCV.notify_all()` is called appropriately to prevent worker thread deadlocks.
* **Metadata State**: The pipeline uses an `SQLiteDBInterface` pointer (`sqlite`) to persist the construction state. This allows the cluster to resume construction if a node fails. Agents must ensure all new metadata fields are synced to this SQLite DB.
