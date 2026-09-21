# VectorStore Specification

**Location**: `src/vectorstore/`
**Status**: Reverse-Engineered Spec (Brownfield)
**Date**: 2026-09-16

## 1. Overview
The `VectorStore` component is responsible for generating and storing vector embeddings of node data, enabling semantic search and Retrieval-Augmented Generation (RAG) capabilities within JasmineGraph. 

## 2. Core Architecture

### 2.1 Embedding Generation (`TextEmbedder.h`)
The `TextEmbedder` class acts as the bridge to external LLM or embedding models.
* **Mechanism**: It wraps an `HttpClient` to send POST requests to a specified endpoint/model name. 
* **Operations**: It supports both single text embedding (`embed`) and batch generation (`batch_embed`).

### 2.2 Vector Storage and Search (`FaissIndex.h`)
JasmineGraph leverages Facebook AI Similarity Search (FAISS) for its underlying vector storage.
* **Mechanism**: `FaissIndex` is a thread-safe Singleton wrapper around `faiss::IndexFlatL2` (which performs exact L2 distance search).
* **ID Mapping**: FAISS natively only supports 64-bit integer IDs (`idx_t`). Because JasmineGraph uses string-based `nodeId`s, `FaissIndex` maintains bidirectional hash maps (`nodeIdToEmbeddingIdMap` and `embeddingIdToNodeIdMap`) to transparently map between string IDs and FAISS internal IDs.

## 3. Key Operations

* **Insertion (`add`)**: Accepts an embedding vector and a `nodeId`. It assigns an internal `idx_t`, adds the vector to the FAISS index, and updates the bidirectional maps.
* **Semantic Search (`search`)**: Takes a query vector and a parameter `k` (number of nearest neighbors). Returns a list of pairs `(id, distance)` representing the closest nodes.
* **Persistence (`save`/`load`)**: The index can be serialized to disk. This includes saving both the underlying FAISS index and the custom bidirectional ID mappings.

## 4. Implicit Contracts & Constraints (Important for AI Agents)
* **Singleton Pattern**: `FaissIndex` manages instances via a static factory `getInstance(embeddingDim, filepath)`. AI agents must not attempt to construct it directly; they must use the factory method.
* **Thread Safety**: Access to the FAISS index and the ID mapping structures is guarded by a `std::mutex` (`mtx`). While it is thread-safe, heavy concurrent read/write workloads might experience lock contention.
* **In-Memory Limitations**: `IndexFlatL2` holds all vectors in RAM. If the dataset exceeds available memory, the system will face Out-Of-Memory (OOM) errors. For massive temporal graphs, future specs should consider IVF (Inverted File) FAISS indices instead of Flat.
