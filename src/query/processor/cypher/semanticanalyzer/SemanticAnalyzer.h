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

#include "../astbuilder/ASTNode.h"
#include "ScopeManager.h"  // Include scope manager header

#ifndef SEMANTICANALYZER_H
#define SEMANTICANALYZER_H

class SemanticAnalyzer {
 public:
    // Constructor
    SemanticAnalyzer();

    // Destructor
    ~SemanticAnalyzer();

    // Public method to analyze the AST
    bool analyze(ASTNode* root, bool canDefine = false, string type = "ANY");

    ScopeManager* getScopeManager();
    unordered_map<std::string, std::string> *temp;

 private:
    ScopeManager* scopeManager;  // Add a ScopeManager instance
    void clearTemp();
    // Private helper methods
    bool checkVariableDeclarations(ASTNode* node, string type);
    bool checkVariableUsage(ASTNode* node, string type);
    // Method to report errors
    void reportError(const std::string &message, ASTNode* node);
};

#endif  // SEMANTICANALYZER_H
