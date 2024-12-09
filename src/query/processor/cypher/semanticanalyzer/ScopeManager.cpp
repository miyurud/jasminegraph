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

#include "ScopeManager.h"

using namespace std;

ScopeManager::ScopeManager() {
    enterScope();// Initialize with a global scope
}

void ScopeManager::enterScope() {
    // Create a new scope and use shared_ptr to manage its memory
    auto *newScope = new Scope(currentScope);
    currentScope = newScope;
    scopeStack.push(newScope);
}

void ScopeManager::exitScope() {
    if (!scopeStack.empty()) {
        scopeStack.pop(); // Remove the current scope from the stack

        // Update currentScope to the new top of the stack or nullptr if empty
        currentScope = scopeStack.empty() ? nullptr : scopeStack.top();
    }
}

void ScopeManager::addSymbol(const string& symbolName, const string& symbolType) {
    if (currentScope) { // Ensure currentScope is valid
        currentScope->addSymbol(symbolName, symbolType);
    }
}

void ScopeManager::clearTable()
{
    if(currentScope)
    {
        currentScope->clearTable();
    }
}

string ScopeManager::getType(const std::string& symbolName)
{
    if(currentScope)
    {
        return currentScope->getType(symbolName);
    }
    return nullptr;
}

optional<string> ScopeManager::lookup(const string& symbolName) const {
    if (currentScope) { // Ensure currentScope is valid
        return currentScope->lookup(symbolName);
    }
    return nullopt;
}
