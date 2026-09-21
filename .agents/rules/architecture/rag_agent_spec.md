# RAG Agent Specification

**Location**: `src/rag/agent/`
**Status**: Reverse-Engineered Spec (Brownfield)
**Date**: 2026-09-21

## 1. Overview
The `Agent` component is the brain of the Multi-Agentic GraphRAG system. It is responsible for intercepting natural language queries, planning graph traversals or semantic searches, and formulating a final human-readable response based on the graph's output.

## 2. Core Architecture

The workflow is strictly decoupled into a Planning Phase and a Responding Phase to maintain separation of concerns.

### 2.1 The Planner (`Planner.h`, `PlanTypes.h`)
* **Mechanism**: The `Planner` takes a raw natural language string and queries an LLM to generate a structured execution plan.
* **Output**: The output is an `nlohmann::json` object representing a `SemanticBeamSearchPlan` (or other execution types like Cypher).
* **Strong Typing**: The `PlanTypes.h` defines C++ structs like `SBSObjective` and `SBSPlan`. A `PlanDecoder` is responsible for safely parsing the LLM's JSON output into these strict C++ types before execution.

### 2.2 The Agent Coordinator (`Agent.h`)
* **Mechanism**: Acts as the orchestrator. It uses a Pimpl (Pointer to Implementation) idiom (`struct Impl; Impl* p;`) to hide the complex orchestration state. 
* **Operations**: Provides `generatePlan()` to kick off the planner, and `generateResponse()` once data is retrieved.

### 2.3 The Responder (`Responder.h`)
* **Mechanism**: Once the underlying query executor (e.g., `IntraPartitionParallelExecutor`) retrieves nodes/edges from the graph, the data is passed to the `Responder` as JSON. 
* **Output**: The `Responder` queries the LLM again, instructing it to synthesize the graph data into a coherent natural language answer.

## 3. Implicit Contracts & Constraints (Important for AI Agents)
* **Two-Phase Commit**: The Agent never executes a query directly. It *Plans*, waits for the engine to execute, and then *Responds*. Do not attempt to merge execution logic into the `Agent` class.
* **Prompt Engineering Dependency**: The `Planner` and `Responder` rely heavily on specific system prompts to force the LLM to output valid JSON conforming to `PlanTypes.h`. If an AI agent modifies `SBSObjective`, it MUST also update the LLM prompt inside `Planner.cpp` and the parsing logic in `PlanDecoder.cpp`.
* **Pimpl Idiom**: `Agent.h` uses the Pimpl idiom. Any state changes or new private variables must be added to `Agent::Impl` inside the `.cpp` file, not the header.
