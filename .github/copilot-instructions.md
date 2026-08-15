# Copilot Instructions for JasmineGraph

JasmineGraph is a **C++17 distributed graph database server** built on the Acacia architecture. It supports standalone and clustered deployments (Docker, Kubernetes), and provides graph analytics, Cypher query processing, knowledge graph construction, and ML capabilities over partitioned graphs.

---

## Architecture Overview

The codebase is organized under `src/` into the following major subsystems:

| Directory | Responsibility |
|---|---|
| `src/server/` | Master server (`JasmineGraphServer`) and worker instance (`JasmineGraphInstance`, `JasmineGraphInstanceService`) logic |
| `src/frontend/` | Client-facing TCP frontend (`JasmineGraphFrontEnd`), Cypher executor pipeline, and job scheduling |
| `src/backend/` | Internal backend protocol handlers (`JasmineGraphBackend`) |
| `src/nativestore/` | File-backed native graph storage: `NodeManager`, `NodeBlock`, `RelationBlock`, `PropertyLink`, `PropertyEdgeLink` |
| `src/localstore/` | In-memory partition-local graph stores (`JasmineGraphHashMapLocalStore`, incremental store) |
| `src/centralstore/` | Cross-partition central stores (`JasmineGraphHashMapCentralStore`) |
| `src/partitioner/` | Graph partitioning: METIS-based (`MetisPartitioner`), RDF, stream, and HDFS partitioners |
| `src/query/` | Query algorithms (triangle counting, BFS, PageRank, link prediction), Cypher AST/semantic analysis, query planner, and runtime |
| `src/ml/` | ML training scheduler (`JasmineGraphTrainingSchedular`) |
| `src/knowledgegraph/` | Knowledge graph construction pipeline with LLM tuple streamers (Ollama, vLLM) |
| `src/vectorstore/` | FAISS-backed vector store (`FaissIndex`) and text embedder (`TextEmbedder`) |
| `src/rag/` | RAG agent framework: `Agent`, `Planner`, `Responder`, `PlanDecoder` |
| `src/metadb/` | SQLite metadata DB (`SQLiteDBInterface`) |
| `src/performancedb/` | SQLite performance metrics DB (`PerformanceSQLiteDBInterface`) |
| `src/streamingdb/` | SQLite streaming metadata DB (`StreamingSQLiteDBInterface`) |
| `src/util/` | Logging (`Logger`), utilities (`Utils`, `Conts`), Kafka integration, HDFS connector, OpenTelemetry, scheduler |
| `src/k8s/` | Kubernetes interface and worker controller |
| `src/scale/` | Dynamic cluster scaling logic |
| `src/performance/` | Performance metric collection and utilities |

**Deployment profiles** are defined as integer constants in `globals.h`:
- `PROFILE_NATIVE` (0) — single machine
- `PROFILE_DOCKER` (1) — Docker-based cluster
- `PROFILE_K8S` (2) — Kubernetes cluster

The active profile is stored in the global `jasminegraph_profile`.

---

## Architecture Principles

- **Master/Worker split**: `JasmineGraphServer` orchestrates a master node; `JasmineGraphInstance`/`JasmineGraphInstanceService` runs on each worker. Communication uses raw TCP sockets with text-based protocols defined in the `*Protocol.h` files.
- **Graph partitioning**: Graphs are partitioned (via METIS or streaming hash) into local partition files. Each partition is assigned to a worker. Central stores hold inter-partition edges. All partition metadata is persisted in the SQLite metadb.
- **Executor pattern**: Query operations are modelled as `AbstractExecutor` subclasses (see `src/frontend/core/executor/`) and dispatched by `ExecutorFactory`. New graph algorithms must follow this pattern.
- **Cypher query pipeline**: Cypher queries go through: lexer/parser (ANTLR4) → `ASTBuilder` → `SemanticAnalyzer` → `QueryPlanner` → `OperatorExecutor` → results. Generated ANTLR sources are expected at `/home/ubuntu/software/antlr/`.
- **Knowledge graphs**: `Pipeline` orchestrates LLM-based triple extraction from documents. `VLLMTupleStreamer` and `OllamaTupleStreamer` are pluggable backends.
- **RAG agent**: `Agent` receives queries, `Planner` generates a plan (decoded by `PlanDecoder`), and `Responder` uses retrieved context to answer.
- **Observability**: OpenTelemetry tracing and metrics are integrated via `OpenTelemetryUtil` and shipped via OTLP HTTP. OpenTelemetry is **disabled in DEBUG builds** to avoid test interference.

---

## Coding Standards

### Language and Standard
- **C++17** throughout (`set(CMAKE_CXX_STANDARD 17)` in `CMakeLists.txt`).
- Prefer STL containers (`std::map`, `std::unordered_map`, `std::vector`, `std::string`) over raw arrays.
- Use `std::shared_ptr` / `std::unique_ptr` for heap-allocated objects rather than raw owning pointers.

### Formatting
Formatting is enforced by `.clang-format` (based on Google style with local overrides):
- **Indent width**: 4 spaces (no tabs).
- **Column limit**: 120 characters.
- **Brace style**: `Attach` (same-line opening braces).
- **Short `if` statements**: never on a single line (`AllowShortIfStatementsOnASingleLine: false`).
- **Trailing comments**: 2 spaces before `//`.
- Access modifiers (`public:`, `private:`) are indented and their members indented further.

Always run `clang-format` before committing C++ files:
```sh
clang-format -i <file>
```

### File Headers
Every source and header file must begin with the Apache 2.0 license header:
```cpp
/**
Copyright <YEAR> JasmineGraph Team
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */
```

### File Footers
Every source and header file must end with an empty line.

### Include Guards
Use `#ifndef` guards (not `#pragma once`). The guard macro must match the pattern `JASMINEGRAPH_<CLASSNAME>_H`:
```cpp
#ifndef JASMINEGRAPH_MYCLASS_H
#define JASMINEGRAPH_MYCLASS_H
// ...
#endif  // JASMINEGRAPH_MYCLASS_H
```

### Naming Conventions
| Entity | Convention | Example |
|---|---|---|
| Classes | `PascalCase` | `MetisPartitioner`, `NodeManager` |
| Methods / functions | `camelCase` | `loadDataSet()`, `partitionWithGPMetis()` |
| Member variables | `camelCase`, no prefix | `edgeCount`, `graphID` |
| Constants / macros | `UPPER_SNAKE_CASE` | `PROFILE_DOCKER`, `ROOT_DIR` |
| File names | Match the primary class name | `MetisPartitioner.h`, `MetisPartitioner.cpp` |

### Logging
Use the `Logger` class from `src/util/logger/Logger.h`. Never use `std::cout` / `printf` in production code paths.
```cpp
#include "../../util/logger/Logger.h"

Logger logger;
logger.info("Graph loaded successfully");
logger.error("Failed to connect: " + errorMsg);
logger.warn("Partition count exceeds worker count");
logger.debug("Edge count: " + std::to_string(edgeCount));
```

### Thread Safety
- Worker loops and frontend sessions run on dedicated threads.
- Protect shared mutable state with `std::mutex`.
- Prefer `std::atomic<bool>` for loop-exit flags (see `JasmineGraphFrontEnd::strian_exit`).

### Error Handling
- Throw `JasmineGraphException` (from `src/exception/`) for unrecoverable errors.
- Log errors via `Logger::error()` before propagating or handling exceptions.
- Do not swallow exceptions silently.

---

## Build System

The project uses **CMake** (minimum 3.29.6) with Ninja as the preferred generator.

### Build Types
| Type | Use |
|---|---|
| `RelWithDebInfo` | Default — release optimizations with debug symbols (VTune-friendly) |
| `DEBUG` | Enables unit tests, code coverage, and disables OpenTelemetry |

### Key External Dependencies
| Library | Purpose |
|---|---|
| SQLite3 | Metadata and performance databases |
| rdkafka / cppkafka | Kafka streaming integration |
| xerces-c | XML/RDF parsing |
| flatbuffers | Serialized edge/attribute stores |
| jsoncpp | JSON parsing |
| libhdfs3 | HDFS connectivity |
| FAISS + OpenBLAS | Vector similarity search |
| ANTLR4 runtime | Cypher query parsing |
| yaml-cpp | Configuration parsing |
| OpenMP | Parallel query execution |
| libkubernetes | Kubernetes API client |
| OpenTelemetry C++ | Distributed tracing and metrics |
| CRoaring | Bitmap operations |
| METIS | Graph partitioning |

### Build Commands
```sh
# Configure (from project root)
cmake -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo -B build .

# Build
cmake --build build

# Build for unit tests (DEBUG)
cmake -G Ninja -DCMAKE_BUILD_TYPE=DEBUG -B build .
cmake --build build
```

### Adding New Source Files
Register both the `.h` and `.cpp` in `CMakeLists.txt` under the `HEADERS` and `SOURCES` variables respectively. Failure to do so will result in linker errors.

---

## Testing

### Unit Tests
Located in `tests/unit/`. Built only when `CMAKE_BUILD_TYPE=DEBUG`. Tests use **Google Test** (fetched via CMake FetchContent). Code coverage is measured with `gcovr` and reported as `coverage/coverage.xml`.

```sh
# Run unit tests in Docker (matches CI)
docker build -f ./tests/unit/Dockerfile -t jasminegraph:unit .
```

### Integration Tests
- **Docker**: `./test-docker.sh` — spins up a multi-container JasmineGraph cluster.
- **Kubernetes**: `./test-k8s.sh` — deploys to a local k3s cluster.
- A mock LLM server (`tests/mock_service/mock_ollama_server.py`) must be running on port `11450` before integration tests that use LLM features.

### CI Pipeline (GitHub Actions)
Three jobs in `.github/workflows/build.yml`:
1. `unit-tests` — Docker build + unit test run + Codecov upload.
2. `docker-integration-tests` — full Docker cluster integration tests.
3. `k8s-integration-tests` — k3s-based integration tests.

PRs can skip specific test suites using GitHub labels:
- `Skip unit tests`
- `Skip docker integration`
- `Skip k8s integration`

---

## When Generating New Features
1. **Start with the spects**: Check `/specs` directory to understand the feature specifications.
2. **Understand the architecture**: Check `src/` directory to understand the current implementation status.
3. **Understand the build system**: Check `CMakeLists.txt` to understand the current implementation status.
4. **Understand the testing**: Check `tests/` directory to understand the current implementation status.
5. **Write Tests First**: Before writing the implementation, write unit tests in `tests/unit/`.
6. **Add logging**: Add logging to the implementation using `Logger::info()`, `Logger::error()`, `Logger::warn()`, `Logger::debug()` for important operations and errors.
7. **Add documentation**: Add documentation to the implementation using Doxygen-style comments.
8. **Add build system support**: Add build system support to the implementation using `CMakeLists.txt`.
9. **Run tests**: Run unit tests in `tests/unit/` to verify the implementation.
10. **Run integration tests**: Run integration tests in `tests/` to verify the implementation.
11. **Run CI pipeline**: Run CI pipeline in `.github/workflows/build.yml` to verify the implementation.
12. **Generate PR**: Generate PR with the commit message and PR description.

## Adding New Features — Checklist

### New Graph Algorithm
1. Implement the algorithm in `src/query/algorithms/<category>/`.
2. Create an `Executor` subclass in `src/frontend/core/executor/impl/` that wraps the algorithm.
3. Register the executor in `ExecutorFactory`.
4. Add protocol constants to the appropriate `*Protocol.h`.
5. Handle the command in `JasmineGraphFrontEnd` (for master-side) and `JasmineGraphInstanceService` (for worker-side).
6. Register new `.h`/`.cpp` files in `CMakeLists.txt`.
7. Write unit tests in `tests/unit/`.

### New Frontend Command
1. Define the command string constant in `src/frontend/JasmineGraphFrontEndProtocol.h`.
2. Handle the command in `JasmineGraphFrontEnd.cpp` within the session dispatch loop.
3. If the command requires worker-side work, add a corresponding protocol message in `JasmineGraphInstanceProtocol.h` and handle it in `JasmineGraphInstanceService.cpp`.

### New Metadata Field
1. Add the column to the relevant SQLite schema in `ddl/`.
2. Update `SQLiteDBInterface` (or `PerformanceSQLiteDBInterface`) with appropriate query methods.
3. Update any serialization/deserialization logic that reads or writes the affected table.

---

## Code Review Checklist

- [ ] Follows C++17 standard
- [ ] Follows formatting rules
- [ ] Follows naming conventions
- [ ] Follows logging rules
- [ ] Follows thread safety rules
- [ ] Follows error handling rules
- [ ] Follows build system rules
- [ ] Follows testing rules
- [ ] Follows adding new features checklist
- [ ] Follows deployment reference

## Documentation Requirements

- **Inline Comments**: Add Doxygen-style comments to all functions, classes, and methods. Explain why something is done, not what is done.
- **Documentation Files**: Update the relevant documentation files in `docs/` directory.
- **Update README**: Update `README.md` if the feature requires any changes in the instructions.

## Deployment Reference

### Docker (single node / cluster)
```sh
docker build -t jasminegraph .
docker run ... jasminegraph --MODE 1 --MASTERIP <ip> --WORKERS 4 --WORKERIP <ip> --ENABLE_NMON false
```

### Kubernetes
```sh
./start-k8s.sh --META_DB_PATH "$(pwd)/metadb" \
    --PERFORMANCE_DB_PATH "$(pwd)/performancedb" \
    --DATA_PATH "$(pwd)/data" \
    --LOG_PATH "$(pwd)/logs" \
    --AGGREGATE_PATH "$(pwd)/aggregate" \
    --CONFIG_DIRECTORY_PATH "$(pwd)/config" \
    --NO_OF_WORKERS 2 \
    --MAX_COUNT 4
```

### Key Ports
| Port | Purpose |
|---|---|
| 7777 | Frontend (client-facing) TCP port |
| 7778 | Backend / inter-node communication port |

**Remember** : Code quality matters more than speed. Follow these patterns consistently for maintainable, testable, and professional codebase.
