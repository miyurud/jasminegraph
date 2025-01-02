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

#include "Scope.h"
#include <iostream>
using namespace std;
Scope::Scope(Scope* parent) {
    parentScope = parent;
    symbolTable = new unordered_map<string, string>();
}

void Scope::addSymbol(const string& symbolName, const std::string& symbolType) {
    pair<string, string> x = pair<string, string>(symbolName, symbolType);
    symbolTable->insert(x);
}

void Scope::clearTable() {
    symbolTable->clear();
}

string Scope::getType(const string& symbolName) {
    return symbolTable->at(symbolName);
}

optional<string> Scope::lookup(const std::string& symbolName) const {
    if (symbolTable->find(symbolName) != symbolTable->end()) {
        return symbolTable->at(symbolName);
    } else if (parentScope) {
        return parentScope->lookup(symbolName);
    } else {
        return std::nullopt;
    }
}
