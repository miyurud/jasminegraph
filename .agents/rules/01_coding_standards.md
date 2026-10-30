# JasmineGraph C++ Coding Standards
These rules must be strictly followed by all developers and AI agents when modifying the C++ codebase.

## 1. Memory Management

*   **Avoid raw owning pointers (`new`/`delete`)** for new code. Prefer smart pointers:
    *   `std::unique_ptr` for exclusive resource ownership.
    *   `std::shared_ptr` / `std::weak_ptr` for shared reference-counted ownership.
*   **Raw Pointers as Non-Owning Observers**: Raw pointers are acceptable when acting as non-owning observers, passing optional references, or interoperating with legacy/C APIs (e.g., `sqlite3*`, `faiss::Index*`, or METIS `idx_t*`).
    *   Always document non-ownership semantics in code comments (or use non-owning reference wrappers).
    *   **NEVER call `delete`** on non-owning raw pointers.
*   **Pass-by-Reference**: Pass read-only strings and complex objects by `const std::string&` / `const T&` to avoid unnecessary heap copies.

### Safe vs Unsafe Pointer Examples

```cpp
// ❌ UNSAFE: Raw owning pointer prone to memory leaks upon early return or exception
void processGraph(int graphId) {
    auto* store = new JasmineGraphHashMapLocalStore(graphId, 0);
    if (!store->loadGraph()) {
        return; // LEAK! store is never deleted
    }
    delete store;
}

// ✅ SAFE: Modern smart pointer with automatic lifetime management
void processGraph(int graphId) {
    auto store = std::make_unique<JasmineGraphHashMapLocalStore>(graphId, 0);
    if (!store->loadGraph()) {
        return; // Safe, automatically deallocated
    }
}

// ✅ ACCEPTABLE: Non-owning observer borrowed from an existing owner or C API
void inspectTopology(const NodeBlock* node) { // Non-owning observer
    if (!node) return;
    // Read operations only; never delete node
}
```

## 2. Distributed Safety & Safe Concurrency Patterns

*   **Synchronous Network Loops**: `JasmineGraphServer` and worker daemons communicate over synchronous TCP/IP sockets. **NEVER execute long-running compute loops or blocking disk I/O directly on the main network/socket listener thread**, as doing so blocks the entire cluster handshake.
*   **Offloading Work**: Always dispatch heavy computation, graph algorithms, or streaming ingestion to worker threads or a thread pool.
*   **Cluster State Persistence**: All distributed state changes (worker assignment, partition mappings, active graph status) must be persisted to the `SQLiteDBInterface` (MetaDB) under `Utils::sqliteMutex`.

### Safe Offloading Pattern

```cpp
// ❌ UNSAFE: Blocking computation directly inside the network command handler
static void query_command(int connFd) {
    runHeavyGraphTraversal(connFd); // Blocks all incoming socket commands for minutes!
}

// ✅ SAFE: Dispatching heavy computation asynchronously
static void query_command(int connFd) {
    std::thread workerThread([connFd]() {
        runHeavyGraphTraversal(connFd);
        close(connFd);
    });
    workerThread.detach(); // Or register with an active thread pool / DynamicThreadPool
}
```

## 3. Formatting & Linting

*   **Clang-Format**: All code must conform to the project `.clang-format` configuration before submission.
*   **Linters**: C++ code must pass `cpplint` checks enforced in CI (`--linelength=120`). Python scripts must adhere to `.pylintrc`.
*   **Naming Conventions**:
    *   Classes / Structs: `PascalCase` (e.g., `MetisPartitioner`, `NodeManager`).
    *   Variables & Methods: `camelCase` (e.g., `graphID`, `loadGraph`).
    *   Constants & Macros: `UPPER_SNAKE_CASE` (e.g., `BLOCK_SIZE`, `PROFILE_DOCKER`).

## 4. Legacy Code Modification & Refactor Path (RFC Process)

*   **Localized Modernization**: When modifying legacy files, apply modern C++ standards strictly to the lines of code you add or edit. Do not perform sweeping refactors of untouched surrounding code just to modernize it.
*   **Required Process for Large Refactors (RFC Path)**:
    If a legacy component has structural defects or requires extensive architectural modernization:
    1.  **Open an Issue / RFC**: Detail the motivation, scope, risk analysis, and proposed design.
    2.  **Verify Test Coverage**: Ensure comprehensive unit tests exist for the existing behavior *before* modifying legacy logic.
    3.  **Coordinate with Maintainers**: Gain sign-off from the repository maintainers (`@miyurud` / JasmineGraph team).
    4.  **Split into Small PRs**: Execute the refactoring in small, atomic, independently verifiable PRs (< 200 lines each).

## 5. Security, Secrets & Data Handling

*   **No Hardcoded Secrets**: Never commit credentials, passwords, API tokens, or private endpoint URLs into the repository.
*   **Environment & Configuration**: Read all sensitive or environment-specific values from system environment variables (e.g., `OLLAMA_HOST`, `VLLM_API_KEY`) or `conf/jasminegraph-server.properties`.
*   **Local Inference Default**: JasmineGraph processes graph data with locally deployed inference models (Ollama / vLLM) by default. Do not transmit private graph data, prompts, or proprietary dataset extracts to external public cloud APIs.
*   **Endpoint Review Gate**: Any changes introducing new external network dependencies, telemetry export targets, or remote endpoints require explicit human maintainer approval.
