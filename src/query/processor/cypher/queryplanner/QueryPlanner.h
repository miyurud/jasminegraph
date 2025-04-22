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

// QueryPlanner.h
#ifndef QUERY_PLANNER_H
#define QUERY_PLANNER_H

#include "../astbuilder/ASTNode.h"
#include "Operators.h" // Include all operators
#include <algorithm>
class QueryPlanner {
public:
    QueryPlanner() = default;
    ~QueryPlanner() = default;

    Operator* createExecutionPlan(ASTNode* ast, Operator* op = nullptr, string var = "");
    bool isAllChildrenAreGivenType(string nodeType, ASTNode* root);
    bool isAvailable(string nodeType, ASTNode* subtree);
    vector<ASTNode*> getSubTreeListByNodeType(ASTNode* root, string nodeType);
    ASTNode* verifyTreeType(ASTNode* root, string nodeType);
    pair<vector<bool>, vector<ASTNode*>> getRelationshipDetails(ASTNode* node);
    pair<vector<bool>, vector<ASTNode*>> getNodeDetails(ASTNode* node);
    Operator* pathPatternHandler(ASTNode* pattern, Operator* opr);
    ASTNode* prepareWhereClause(string var1, string var2);
};

#endif // QUERY_PLANNER_H
