# Claude Code Configuration

Welcome to the JasmineGraph repository. This project uses a unified rule system for all AI coding agents to ensure PR consistency and architectural safety.

## Core Directives

1. **Core Standards**: Before writing or modifying C++ code, you MUST adhere to the standards in [.agents/rules/01_coding_standards.md](file:///d:/Projects/fyp/jasminegraph/.agents/rules/01_coding_standards.md) and behavioral constraints in [.agents/rules/02_agent_behavior.md](file:///d:/Projects/fyp/jasminegraph/.agents/rules/02_agent_behavior.md).
2. **Architecture Specifications**: Before modifying any subsystem (e.g., `temporalstore`, `partitioner`, `nativestore`, `centralstore`, `rag`), check the corresponding document in [.agents/rules/architecture/](file:///d:/Projects/fyp/jasminegraph/.agents/rules/architecture/) to prevent violating distributed runtime invariants.
3. **Commit Style**: Strictly use [Conventional Commits](https://www.conventionalcommits.org/) (e.g., `feat(server): ...`, `fix(query): ...`).
4. **Scope Limit**: Keep code modifications strictly under 150–200 lines per task. Do not perform sweeping refactors of legacy code without prior maintainer approval.
5. **Human Approval Gate**: All agent-authored changes require human review and approval by repository maintainers (`@miyurud`). Auto-merging is prohibited.
