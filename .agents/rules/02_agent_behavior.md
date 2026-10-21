---
trigger: always_on
---

# AI Agent Behavioral Constraints
These rules apply to any AI Coding Agent (Antigravity, Claude Code, GitHub Copilot, etc.) operating in this workspace.

## 1. PR Size, Commits & Scope Limits

*   **Small, Atomic PRs**: Limit code modifications to a maximum of **150–200 lines of code** per task. If a task requires changes across multiple subsystems, stop and prompt the user to break it down into smaller, independently verifiable milestones.
*   **Conventional Commits**: Every commit message must strictly follow the [Conventional Commits](https://www.conventionalcommits.org/) standard:
    *   `feat(rag): add planner module`
    *   `fix(server): resolve socket deadlock in remote worker dispatch`
    *   `test(partitioner): add unit test for sheep partitioner`
    *   `docs(architecture): update central store specification`

## 2. Build Verification & Exact Commands

Before declaring any C++ implementation or bugfix complete, agents must verify the build within the canonical Ubuntu Docker container. **Never rely on host or local IDE warnings**, as they often reflect platform mismatches (e.g., Windows/macOS vs. Ubuntu target container).

### Exact Verification Commands

1.  **Container Build (Compile & Warning Check)**:
    ```bash
    docker build -t jasminegraph:test .
    ```
    *Criterion*: Build must complete with exit code 0 and zero compilation warnings.

2.  **Unit Test Suite**:
    ```bash
    docker build -f ./tests/unit/Dockerfile -t jasminegraph:unit .
    docker run --rm jasminegraph:unit ./tests/unit/JasmineGraphUnitTest
    ```
    *Criterion*: All Google Test fixtures must pass.

3.  **Integration Test Suite (Docker Mode)**:
    ```bash
    chmod +x test-docker.sh && ./test-docker.sh
    ```

4.  **Style & Linter Validation**:
    ```bash
    cpplint --linelength=120 --filter=-build,-runtime,-readability/todo,-whitespace/todo,-readability/casting,-readability/braces,-readability/fn_size,-legal/copyright --recursive .
    pylint -d C0103,E0401,R0801,R0902,R0913,R0914,R0915,W0603,W0621,W0718 --recursive=y .
    ```

## 3. Valgrind Memory Leak Checks

Whenever modifying or implementing algorithms in **temporal graph storage** (`src/temporalstore/`), **partitioning** (`src/partitioner/`), or **native slotted storage** (`src/nativestore/`), agents must execute Valgrind checks inside the unit test container.

### Exact Valgrind Invocation

```bash
docker run --rm jasminegraph:unit valgrind \
    --leak-check=full \
    --show-leak-kinds=definite,possible \
    --track-origins=yes \
    --error-exitcode=1 \
    ./tests/unit/JasmineGraphUnitTest
```

*   **Scope**: Targeted tests for modified classes (e.g., `--gtest_filter="TemporalStoreTest.*"`).
*   **Threshold**: Zero "definitely lost" bytes and zero unaddressed memory access errors (`--error-exitcode=1` ensures failures block CI).

## 4. Governance, Human Approval Gate & Enforcement Policy

*   **Mandatory Human Gate**: AI agents **MUST NEVER** attempt to self-authorize or auto-merge pull requests. Every PR authored or co-authored by an agent requires mandatory review and approval by a human maintainer.
*   **Maintainer Contact**: The primary repository maintainer is **@miyurud** along with the JasmineGraph core research team.
*   **Exception Requests**: If a task genuinely requires exceeding the 200 LOC limit (e.g., vendor headers, third-party FlatBuffer definitions, dataset fixtures):
    1.  Create an issue titled `[Exception Request] <Reason>`.
    2.  Obtain explicit maintainer confirmation before opening the pull request.

## 5. Mandatory PR Checklist Template

Every pull request description submitted by or on behalf of an agent must include the following completed checklist:

```markdown
### Agent Verification Checklist
- [ ] **Docker Build**: Ran `docker build -t jasminegraph:test .` with 0 warnings.
- [ ] **Unit Tests**: Executed `JasmineGraphUnitTest` inside container (all tests pass).
- [ ] **Valgrind**: Ran leak check for temporal/partitioning changes (0 leaks).
- [ ] **Conventional Commits**: Commits follow `type(scope): description`.
- [ ] **LOC Limit**: Under 200 lines modified (or exception approved).
- [ ] **Human Review**: Awaiting maintainer approval (@miyurud).
```
