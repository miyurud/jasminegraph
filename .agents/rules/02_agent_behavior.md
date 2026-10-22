---
trigger: always_on
---

# AI Agent Behavioral Constraints
These rules apply to any AI Coding Agent (Antigravity, Claude Code, etc.) operating in this workspace.

## 1. PR Size & Commits
*   **Small PRs**: Limit your code modifications to a maximum of 150-200 lines of code per task. If a user requests a massive feature, stop and ask them to break it down into smaller, verifiable chunks.
*   **Commits**: Use Conventional Commits format (e.g., `feat(rag): add planner module`, `fix(server): resolve socket deadlock`).

## 2. Verification
*   Do not mark a task as complete in C++ until you have successfully built the project inside the Docker container (e.g., by running `./test-docker.sh` or `docker build -t jasminegraph:test .`). Ensure there are no compilation warnings in the Docker build output. Do NOT rely on local IDE warnings as they may be false positives caused by host environment mismatches.
*   If modifying temporal or partitioning logic, you must run Valgrind checks for memory leaks.
