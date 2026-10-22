# Query Executor Specification (Intra-Partition)

**Location**: `src/query/processor/executor/`
**Status**: Reverse-Engineered Spec (Brownfield)
**Date**: 2026-09-16

## 1. Overview
The `IntraPartitionParallelExecutor` is the engine that drives parallel execution of graph queries within a single JasmineGraph partition. It is designed to maximize CPU utilization by automatically chunking workloads and dispatching them across a dynamically sized thread pool.

## 2. Core Architecture

### 2.1 Dynamic Thread Pool (`DynamicThreadPool`)
The executor relies on a `DynamicThreadPool` that automatically detects the host system's hardware concurrency (CPU core count) and spawns an optimal number of worker threads.
* **Task Queue**: Workers pull `std::function` tasks from a shared thread-safe queue.

### 2.2 Thread-Safe Buffers and Data (`ThreadSafeBuffer`, `PreloadedNodeData`)
* **`PreloadedNodeData`**: Graph nodes are preloaded into this struct before parallel processing. This is a crucial architectural decision to avoid thread-safety issues (data races) that would occur if multiple threads tried to hit a shared `NodeManager` simultaneously.
* **`ThreadSafeBuffer`**: A generic thread-safe queue utilizing `std::condition_variable` and timeouts for Producer-Consumer workflows.

## 3. Key Operations & Algorithms

### 3.1 Adaptive Chunking
Instead of dispatching item-by-item (which introduces massive locking overhead), the system uses `calculateOptimalChunkSize` to divide the total workload (`totalItemCount`) evenly among the available workers into `WorkChunk`s. 

### 3.2 Parallel Processing (`processInParallel`)
This is the main entry point for query algorithms:
1. Calculates chunk sizes based on the dataset size and worker count.
2. Uses a template `Processor` function/lambda.
3. Enqueues the chunks to the `DynamicThreadPool` using `executeChunkedTasks`.
4. Returns a vector of `std::future` results.

### 3.3 Result Merging (`mergeResults`)
After futures are resolved, this utility efficiently concatenates the chunked vectors into a single contiguous result set, pre-allocating memory based on estimated sizes to avoid reallocation overhead.

## 4. Distributed Graph Machine Learning & Embedding Infrastructure

JasmineGraph provides a distributed graph machine learning pipeline designed for parallel node embedding and link prediction using **GraphSAGE**:

### 4.1 Architecture & Process Separation
* **C++ Master & Worker Orchestration**:
  * `JasmineGraphTrainingSchedular.cpp`: Determines optimal execution schedules for graph partitions per host. It queries partition metadata from MetaDB, estimates RAM usage based on vertex/feature counts, and applies memory-packing algorithms to avoid host out-of-memory errors during training.
  * `JasmineGraphServer.cpp` & `JasmineGraphInstanceService.cpp`: Dispatches training tasks (`initiateCommunication`, `initiateOrgCommunication`) across the cluster, coordinating socket handshakes to spin up the Python server and worker clients.
* **Python Server (`src_python/fl_server.py`, `org_server.py`, `org_agg.py`)**:
  * Acts as the parameter coordinator/aggregator.
  * Listens for socket connections from worker clients, collects local model weights, computes federated aggregations (weighted FedAvg based on partition size), persists checkpointed weight matrices (`weights_graphID:<id>_V<round>.npy`), and broadcasts global model updates.
* **Python Worker Clients (`src_python/fl_client.py`)**:
  * Spawned on distributed worker nodes for assigned partition IDs.
  * Loads partition nodes and edges from partitioned CSVs (`<graphID>_nodes_<partitionID>.csv`, `<graphID>_edges_<partitionID>.csv`).
  * Interacts with `fl_server` over TCP sockets to receive weights, execute training rounds, and transmit model parameter updates.
* **GraphSAGE Model Implementation (`src_python/models/supervised.py`)**:
  * Built using **StellarGraph** (`GraphSAGE`, `GraphSAGELinkGenerator`) and **TensorFlow/Keras**.
  * Implements neighbor sampling and multi-layer aggregation to compute low-dimensional node embeddings in parallel across partition shards.

## 5. Implicit Contracts & Constraints (Important for AI Agents)
* **Preloading Requirement**: When writing new parallel query algorithms (e.g., Temporal Traversals), the AI agent MUST preload node and edge properties into thread-local structures (like `PreloadedNodeData`) *before* invoking `processInParallel`. Do not pass shared database state handlers into the parallel processor lambdas.
* **Overhead Thresholds (`shouldUseParallelProcessing`)**: The system contains logic to bypass parallelism if the dataset is too small. AI agents should respect this check to prevent context-switching overhead on tiny queries.
* **No Nested Parallelism**: Do not instantiate an `IntraPartitionParallelExecutor` *inside* a task running within another `IntraPartitionParallelExecutor`, as this will lead to thread starvation and deadlocks.
* **Python Subprocess Boundaries**: When managing ML training or embedding generation, agents must respect the C++ to Python execution bridge. Training processes are decoupled via CLI invocations and socket messaging; never execute long blocking ML tasks directly on the master socket loop.
