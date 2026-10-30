---
trigger: always_on
---

# AI Agent Behavioral Constraints
These rules apply to any AI Coding Agent (Antigravity, Claude Code, GitHub Copilot, etc.) operating in this workspace.

## 1. Scope Limits & Git Commit Standards

*   **Small, Atomic Scope**: Limit code modifications to a maximum of **150–200 lines of code** per task. If a task requires changes across multiple subsystems, stop and prompt the user to break it down into smaller, independently verifiable milestones.
*   **Git Commit Guidelines**: Commit messages must adhere to the standard Git best practices outlined in [How to Write a Git Commit Message](https://cbea.ms/git-commit/) (The Seven Rules):
    1.  **Separate subject from body with a blank line**.
    2.  **Limit the subject line to 50 characters** (hard limit: 72 characters).
    3.  **Capitalize the subject line** (e.g., `Add unit tests for sheep partitioner`).
    4.  **Do not end the subject line with a period**.
    5.  **Use the imperative mood in the subject line** (e.g., complete the sentence *"If applied, this commit will <your subject line>"* — use `Add`, `Fix`, `Refactor`, `Update`, not `Added`, `Fixing`, or `Fixes`).
    6.  **Wrap the body at 72 characters**.
    7.  **Use the body to explain what and why vs. how** (the diff shows *how*; the body explains context and *why* the change was made).

### Example Commit Format

```text
Summarize changes in around 50 characters or less in imperative mood

More detailed explanatory text, if necessary. Wrap it to about 72
characters. The blank line separating the summary from the body is
critical.

Explain the problem that this commit is solving. Focus on why you are
making this change as opposed to how (the code explains that). Are there
side effects or other consequences of this change? Explain them here.

- Bullet points are okay, preceded by a hyphen or asterisk
- Separate paragraphs and bullets with blank lines
```

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
