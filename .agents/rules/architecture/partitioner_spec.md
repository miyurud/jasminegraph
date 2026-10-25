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

### 2.2 Hash Partitioner (`HDFSMultiThreadedHashPartitioner.h`, `Partitioner.cpp`)
* **Mechanism**: Assigns vertices and edges deterministically using hash modulo operations (`std::hash<std::string>{}(nodeId) % numberOfPartitions`).
* **Characteristics**: Fast, completely stateless, and lock-free across streaming workers. If source and target nodes hash to different partition IDs, the edge is recorded as an edge cut.

### 2.3 FENNEL Partitioner (`Partitioner.cpp`)
* **Mechanism**: A streaming graph partitioning algorithm that balances partition load while minimizing edge cuts through an intra-partition cost vs. inter-partition benefit scoring function.
* **Formula**: For a candidate vertex $v$ and partition $S_i$, the score is $|N(v) \cap S_i| - \partial c(|S_i|)$, where $|N(v) \cap S_i|$ is the number of neighbors already in $S_i$, and the marginal cost is $\partial c(|S_i|) = \alpha \cdot ((|S_i| + 1)^\gamma - |S_i|^\gamma)$ (with $\gamma = 1.5$ and $\alpha = m \cdot k^{\gamma - 1} / n^\gamma$). The vertex is assigned to the partition maximizing this score.

### 2.4 LDG Partitioner (`Partitioner.cpp`)
* **Mechanism**: Linear Deterministic Greedy (Stanton & Kliot) streaming partitioning algorithm.
* **Formula**: Assigns vertices dynamically based on greedy neighbor overlap weighted by remaining capacity: $|N(v) \cap S_i| \times \left(1 - \frac{|S_i|}{n/k}\right)$, where $n$ is total vertices and $k$ is the partition count. Partitions nearing capacity are penalized linearly.

### 2.5 Sheep Partitioner
* **Mechanism**: An edge-centric streaming partitioner designed to minimize vertex replication across distributed workers.
* **Characteristics**: Evaluates partition assignments for incoming edge streams by balancing vertex-cut replication ratios against worker storage balance, avoiding unnecessary cross-partition edge transfers.

### 2.6 Streaming vs Local Partitioning Execution
* **`local/` (Offline)**: Preprocesses static graph topologies (e.g., via METIS) to generate serialized edge lists, central store cut files, and attribute tables prior to loading.
* **`stream/` (Online)**: Partitions continuous edge streams dynamically (from Kafka topics or HDFS blocks) using streaming algorithms (Hash, FENNEL, LDG) without requiring the entire global topology upfront. 

## 3. Implicit Contracts & Constraints (Important for AI Agents)
* **ID Reformatting**: Raw datasets often have non-sequential string or integer IDs. The `MetisPartitioner` strictly reformats these into contiguous integers starting from `1` (via `vertexToIDMap`) to create the sequential format required by METIS (`xadj`, `adjncy`). **Agents must map back to original IDs** (using `idToVertexMap`) before returning results to the user.
* **Central Store Overhead**: AI agents developing graph traversals must explicitly account for the "Central Store". If a query crosses a partition, the local worker must communicate with the master or another worker to fetch the missing edge from the central store.
