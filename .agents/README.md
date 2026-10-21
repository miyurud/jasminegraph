# JasmineGraph Agent Rules & Architecture Reference

This directory contains the authoritative standards, behavioral constraints, and reverse-engineered architectural specifications governing all automated coding agents (Antigravity, Claude Code, GitHub Copilot, etc.) and human contributors working on JasmineGraph.

## 1. Core Rules & Standards

| Document | Purpose | Applicability |
| :--- | :--- | :--- |
| [01_coding_standards.md](file:///d:/Projects/fyp/jasminegraph/.agents/rules/01_coding_standards.md) | C++ memory management (smart vs non-owning pointers), non-blocking network socket patterns, formatting, legacy modernization (RFC path), and secrets/security. | **Mandatory** (Agent & Human) |
| [02_agent_behavior.md](file:///d:/Projects/fyp/jasminegraph/.agents/rules/02_agent_behavior.md) | Operational constraints: 150–200 LOC per task, Conventional Commits, Docker build commands, Valgrind leak check requirements, and human review gate. | **Mandatory** (Agent Enforced) |

---

## 2. Architecture Specifications (`.agents/rules/architecture/`)

These documents establish the component-level contracts and architectural invariants reverse-engineered from the codebase:

| Specification | Target Subsystem | Core Responsibilities & Invariants |
| :--- | :--- | :--- |
| [centralstore_spec.md](file:///d:/Projects/fyp/jasminegraph/.agents/rules/architecture/centralstore_spec.md) | `src/centralstore/` | Cross-partition boundary edges ("edge cuts"), FlatBuffers serialization (`_centralstore_`, `_centralstore_dp_`), composite central store groupings, and inter-worker triangle aggregation. |
| [nativestore_spec.md](file:///d:/Projects/fyp/jasminegraph/.agents/rules/architecture/nativestore_spec.md) | `src/nativestore/` | Custom low-level slotted binary block store (`_nodes.db` [140B], `_relations.db` [152B], `_central_relations.db` [156B]), linked property chains (`_properties.db`), and streaming socket `DataPublisher`. |
| [localstore_spec.md](file:///d:/Projects/fyp/jasminegraph/.agents/rules/architecture/localstore_spec.md) | `src/localstore/` | Worker-local intra-partition adjacency maps (`JasmineGraphHashMapLocalStore`), store factories, and `JasmineGraphIncrementalLocalStore` wrapping native store disk blocks. |
| [partitioner_spec.md](file:///d:/Projects/fyp/jasminegraph/.agents/rules/architecture/partitioner_spec.md) | `src/partitioner/` | Graph partitioners: offline METIS (`MetisPartitioner`, contiguous 1-indexed IDs), streaming algorithms (Hash, FENNEL, LDG), and degree-sorted elimination trees ([SheepPartitioner.h](file:///d:/Projects/fyp/jasminegraph/src/partitioner/local/SheepPartitioner.h)). |
| [query_executor_spec.md](file:///d:/Projects/fyp/jasminegraph/.agents/rules/architecture/query_executor_spec.md) | `src/query/` & `src_python/` | Multi-threaded intra-partition parallel executor (`DynamicThreadPool`, `PreloadedNodeData`) and distributed Python GraphSAGE ML embedding infrastructure (`fl_server.py`, `fl_client.py`). |
| [server_backend_spec.md](file:///d:/Projects/fyp/jasminegraph/.agents/rules/architecture/server_backend_spec.md) | `src/server/`, `k8s/`, Docker | Master coordinator (`JasmineGraphServer`), worker daemons (`JasmineGraphInstance`), Docker deployment mode (`PROFILE_DOCKER`), and Kubernetes orchestration (`PROFILE_K8S`, `K8sWorkerController`). |
| [temporalstore_spec.md](file:///d:/Projects/fyp/jasminegraph/.agents/rules/architecture/temporalstore_spec.md) | `src/temporalstore/` | Temporal graph storage: edge lifespan tracking via Roaring Bitmaps, `PropertyIntervalDictionary`, `SnapshotManager`, point-in-time querying, and streaming triangle counting. |
| [kg_construction_spec.md](file:///d:/Projects/fyp/jasminegraph/.agents/rules/architecture/kg_construction_spec.md) | `src/knowledgegraph/construction/` | Ingestion pipeline (`Pipeline.h`, `dataBuffer`), streaming LLM tuple extractors (`OllamaTupleStreamer`, `VLLMTupleStreamer`), character-by-character bracket-depth JSON parsing, and entity resolution. |
| [metadb_spec.md](file:///d:/Projects/fyp/jasminegraph/.agents/rules/architecture/metadb_spec.md) | `src/metadb/` | Cluster metadata registry (`SQLiteDBInterface.h`), partition-to-worker mappings, and thread-safe concurrent database access using `Utils::sqliteMutex`. |
| [rag_agent_spec.md](file:///d:/Projects/fyp/jasminegraph/.agents/rules/architecture/rag_agent_spec.md) | `src/rag/agent/` | GraphRAG agent: decoupled Planning Phase (`Planner.h`, `PlanTypes.h`, `SBSPlan`), Pimpl orchestrator (`Agent.h`), and Responding Phase (`Responder.h`). |
| [streamingdb_spec.md](file:///d:/Projects/fyp/jasminegraph/.agents/rules/architecture/streamingdb_spec.md) | `src/streamingdb/` | Real-time streaming edge deltas (`StreamingSQLiteDBInterface.h`), buffer-to-disk thresholds, and dual-state query resolution across static local stores and delta buffers. |
| [vectorstore_spec.md](file:///d:/Projects/fyp/jasminegraph/.agents/rules/architecture/vectorstore_spec.md) | `src/vectorstore/` | Embedding generation (`TextEmbedder.h`) and FAISS vector index wrapper (`FaissIndex.h`, `IndexFlatL2`, thread-safe keyed factory `getInstance(dim, path)`). |

---

## 3. Governance & Maintainer Contact

*   **Repository Maintainer**: **@miyurud** and the JasmineGraph team.
*   **Human Gate**: All code generated by AI coding agents must undergo review and approval by human maintainers prior to merging into `master`. Auto-merging is strictly disabled.
*   **Exception Requests**: For tasks that require exceeding scope limits (> 200 LOC, vendor dependencies, large test fixtures), open an issue titled `[Exception Request]` and receive maintainer approval before submitting changes.
