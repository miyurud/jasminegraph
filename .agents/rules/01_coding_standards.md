# JasmineGraph C++ Coding Standards
These rules must be strictly followed by all developers and AI agents when modifying the C++ codebase.

## 1. Memory Management
*   **NEVER use raw pointers** (`new`/`delete`) for new developments.
*   Always use `std::unique_ptr` for exclusive ownership and `std::shared_ptr` for shared ownership.
*   Pass by `const std::string&` instead of pass-by-value for strings.

## 2. Distributed Safety
*   The `JasmineGraphServer` uses blocking TCP/IP sockets. NEVER write long-running `while` or `for` loops on the main network thread.
*   All distributed state changes must be persisted to the `SQLiteDBInterface` (MetaDB).

## 3. Formatting
*   Code must be formatted using `clang-format` before submission.
*   Use `camelCase` for variables and `PascalCase` for classes.

## 4. Legacy Code Modification
*   When modifying legacy files, apply these modern standards strictly to the lines of code you are adding or modifying. Do not rewrite surrounding legacy code just to modernize it, unless explicitly asked.
