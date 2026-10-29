# Claude Code Configuration

Welcome to the JasmineGraph repository. This project uses a unified rule system for all AI coding agents to ensure PR consistency and architectural safety.

## Core Directives

1. **Core Standards**: Before writing or modifying C++ code, you MUST adhere to the standards in [.agents/rules/01_coding_standards.md](.agents/rules/01_coding_standards.md) and behavioral constraints in [.agents/rules/02_agent_behavior.md](.agents/rules/02_agent_behavior.md).
2. **Architecture Specifications**: Before modifying any subsystem (e.g., `temporalstore`, `partitioner`, `nativestore`, `centralstore`, `rag`), check the corresponding document in [.agents/rules/architecture/](.agents/rules/architecture/) to prevent violating distributed runtime invariants.
3. **Commit Style**: Adhere to standard Git commit best practices ([The Seven Rules](https://cbea.ms/git-commit/)): concise imperative subject line (<= 50 chars, capitalized, no period), blank line before body wrapped at 72 chars explaining *what* and *why* vs *how*.
4. **Scope Limit**: Keep code modifications strictly under 150–200 lines per task. Do not perform sweeping refactors of legacy code without prior maintainer approval.
5. **Human Approval Gate**: All agent-authored changes require human review and approval by repository maintainers (`@miyurud`). Auto-merging is prohibited.
