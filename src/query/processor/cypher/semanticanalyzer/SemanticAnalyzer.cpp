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

#include <iostream>
#include "SemanticAnalyzer.h"
#include "ScopeManager.h"
#include "../../../../util/logger/Logger.h"
#include "../util/Const.h"

using namespace std;

// Constructor
SemanticAnalyzer::SemanticAnalyzer() {
    // Initialize the scope manager with the global scope
    scopeManager = new ScopeManager();
    temp = new unordered_map<string, string>();  // Enter global scope
}

// Destructor
SemanticAnalyzer::~SemanticAnalyzer() {
    // Ensure to exit all scopes upon destruction
    scopeManager->exitScope();
}


// Main method to analyze the AST
bool SemanticAnalyzer::analyze(ASTNode* root, bool canDefine, string type) {
    if (root->nodeType == Const::VARIABLE && canDefine) {
        if (!checkVariableDeclarations(root, type)) {
            return false;
        }
    } else if (root->nodeType == Const::VARIABLE) {
        if (!checkVariableUsage(root, type)) {
            return false;
        }
    } else if (root->nodeType == Const::AS) {
        string ntype;
        if (root->elements[0]->nodeType == Const::LIST || root->elements[0]->nodeType == Const::LIST_COMPREHENSION) {
            ntype = Const::LIST;
        } else if (root->elements[0]->nodeType == Const::PROPERTIES_MAP) {
            ntype = Const::MAP;
        } else {
            ntype = Const::ANY;
        }
        if (!analyze(root->elements[0])) {
            return false;
        }
        if (root->elements[1]->nodeType != Const::VARIABLE
            || !analyze(root->elements[1], true, ntype)) {
            return false;
        }
    } else if (root->nodeType == Const::YIELD) {
        if (root->elements[0]->nodeType == "AS" &&
            !analyze(root->elements[0], false, Const::STRING)) {
            return false;
        }
        if (!analyze(root->elements[0], true, Const::STRING)) {
            return false;
        }
    } else if (root->nodeType == Const::EQUAL) {
        string ntype = root->nodeType == Const::PATTERN_ELEMENTS ? Const::PATH_PATTERN : Const::ANY;
        if (!analyze(root->elements[0], true, ntype)) {
            return false;
        }
        if (!analyze(root->elements[1])) {
            return false;
        }
    } else if (root->nodeType == Const::NODE_PATTERN && !root->elements.empty()) {
        if (root->elements[0]->nodeType == Const::VARIABLE &&
            !analyze(root->elements[0], true, Const::NODE)) {
            return false;
        }
        for (int i=1; i < root->elements.size(); i++) {
            if (!analyze(root->elements[i])) {
                return false;
            }
        }
    } else if (root->nodeType == Const::RELATIONSHIP_DETAILS && !root->elements.empty()) {
        if (root->elements[0]->nodeType == Const::VARIABLE &&
            !analyze(root->elements[0], true, Const::RELATIONSHIP)) {
            return false;
        }
        for (int i=1; i < root->elements.size(); i++) {
            if (!analyze(root->elements[i])) {
                return false;
            }
        }
    } else if (root->nodeType == Const::LIST_ITERATE) {
        if (!analyze(root->elements[0], true)) {
            return false;
        }
        if (!analyze(root->elements[1])) {
            return false;
        }
    } else if (root->nodeType == Const::NON_ARITHMETIC_OPERATOR && root->elements[0]->nodeType == Const::VARIABLE) {
        if (root->elements[1]->nodeType == Const::LIST_INDEX_RANGE &&
            !analyze(root->elements[0], false, Const::LIST)) {
            return false;
        } else if (root->elements[1]->nodeType == Const::LIST_INDEX &&
            root->elements[1]->elements[0]->nodeType == Const::DECIMAL &&
            !analyze(root->elements[0], false, "LIST")) {
            return false;
        } else if (root->elements[1]->nodeType == Const::PROPERTY_LOOKUP &&
            !analyze(root->elements[0], false, Const::LOOKUP)) {
            return false;
        }
    } else if (root->nodeType == Const::EXISTS) {
        scopeManager->enterScope();
        for (int i = 0; i < root->elements.size(); i++) {
            if (!analyze(root->elements[i])) {
                return false;
            }
        }
        scopeManager->exitScope();

    } else if (root->nodeType == Const::WITH) {
        for (int i = 0; i < root->elements.size(); i++) {
            if (!analyze(root->elements[i])) {
                return false;
            }
        }

        clearTemp();
        auto *node = root->elements[0]->elements[0];
        for (auto* child : node->elements) {
            if (child->nodeType == Const::AS) {
                string tempType;
                if (child->elements[0]->nodeType == Const::VARIABLE) {
                    tempType = scopeManager->getType(child->elements[0]->value);
                } else if (child->elements[0]->nodeType == Const::PROPERTIES_MAP) {
                    tempType = Const::MAP;
                } else if (child->elements[0]->nodeType == Const::LIST) {
                    tempType = Const::LIST;
                } else {
                    tempType = Const::ANY;
                }
                pair<string,string> x = pair<string,string>(child->elements[1]->value, tempType);
                temp->insert(x);
            } else if (child->nodeType == Const::VARIABLE) {
                string tempType = scopeManager->getType(child->value);
                pair<string,string> x = pair<string, string>(child->value, tempType);
                temp->insert(x);
            } else {
                reportError("use 'as' keyword to assign it to new variable" + node->value, node);
                return false;
            }
        }
        scopeManager->clearTable();
        for (auto tempNode = temp->begin(); tempNode != temp->end(); ++tempNode) {
            scopeManager->addSymbol(tempNode->first, tempNode->second);
        }

    } else {
        for (int i = 0; i < root->elements.size(); i++) {
            if (!analyze(root->elements[i])) {
                return false;
            }
        }
    }
    return true;
}

// Check for variable declarations
bool SemanticAnalyzer::checkVariableDeclarations(ASTNode* node, string type) {
    if (!scopeManager->lookup(node->value)) {
        // If variable is not in current scope, add it
        scopeManager->addSymbol(node->value, type);
    } else if (scopeManager->lookup(node->value) == type) {
        return true;
    } else {
        reportError("Varmiable already declared: " + node->value, node);
        return false;
    }
    return true;
}

// Check for function definitions
void SemanticAnalyzer::clearTemp() {
    temp->clear();
}

bool SemanticAnalyzer::checkVariableUsage(ASTNode* node, string type) {
    if (!(scopeManager->lookup(node->value))) {
        reportError("Variable is not defined in this scope: " + node->value, node);
        return false;
    } else if (scopeManager->lookup(node->value) && type == Const::ANY) {
        return true;
    } else if (scopeManager->lookup(node->value) && scopeManager->lookup(node->value) == type) {
        return true;
    } else if (type == "LOOKUP" && (scopeManager->lookup(node->value) == Const::NODE ||
                                  scopeManager->lookup(node->value) == Const::RELATIONSHIP ||
                                  scopeManager->lookup(node->value) == Const::MAP)) {
        return true;
    } else {
        reportError("Variable type mismatch: " + node->value, node);
        return false;
    }
}

// Report errors
void SemanticAnalyzer::reportError(const std::string &message, ASTNode* node) {
    Logger logger;
    logger.error(message);
}
