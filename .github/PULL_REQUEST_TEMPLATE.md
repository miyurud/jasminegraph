## Description
<!-- Provide a brief, clear summary of the changes and motivation -->

## Subsystem
<!-- Select relevant subsystems: [ ] Server / Backend [ ] TemporalStore [ ] Partitioner [ ] NativeStore [ ] CentralStore [ ] Query Executor [ ] RAG / ML -->

## Agent Verification Checklist
<!-- All agents (and human contributors) must complete this checklist before requesting review -->
- [ ] **Docker Build**: Verified with `docker build -t jasminegraph:test .` (exit code 0, 0 compiler warnings).
- [ ] **Unit Tests**: Executed `docker build -f ./tests/unit/Dockerfile -t jasminegraph:unit .` and tests pass.
- [ ] **Valgrind Checks**: Run if modifying temporal, partitioner, or native store components (0 leaks).
- [ ] **Conventional Commits**: Commits follow `type(scope): description` (e.g., `feat(server): ...`).
- [ ] **Scope Limit**: Code modifications kept within 150–200 LOC per task (or exception approved).
- [ ] **Human Maintainer Review**: Awaiting review and approval from human maintainers (@miyurud). Auto-merging is disabled.
