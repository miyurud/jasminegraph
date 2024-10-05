// QueryPlanner.h
#ifndef QUERY_PLANNER_H
#define QUERY_PLANNER_H

#include "../astbuilder/ASTNode.h"
#include "Operators.h" // Include all operators

class QueryPlanner {
public:
    QueryPlanner() = default;
    ~QueryPlanner() = default;

    Operator* createExecutionPlan(ASTNode* ast, Operator* op = nullptr, string var = "");
    bool isAllChildAreGivenType(string nodeType, ASTNode* root);
    bool isAvailable(string nodeType, ASTNode* subtree);
    vector<ASTNode*> getSubTreeListByNodeType(ASTNode* root, string nodeType);
    ASTNode* getSubtreeByType(ASTNode* root, string nodeType);
};

#endif // QUERY_PLANNER_H
