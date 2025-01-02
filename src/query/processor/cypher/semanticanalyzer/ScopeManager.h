/**
Copyright 2024 JasmineGraph Team
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

#ifndef SCOPE_MANAGER_H
#define SCOPE_MANAGER_H

#include <stack>
#include <optional>
#include <string>
#include "Scope.h"

class ScopeManager {
 public:
    ScopeManager();

    void enterScope();
    void exitScope();
    void addSymbol(const std::string& symbolName, const std::string& symbolType);
    void clearTable();
    std::string getType(const std::string& symbolName);
    std::optional<std::string> lookup(const std::string& symbolName) const;

 private:
    std::stack<Scope*> scopeStack;  // Stack of smart pointers
    Scope* currentScope = nullptr;  // Current scope managed by smart pointer
};

#endif  // SCOPE_MANAGER_H
