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
    pair<vector<bool>, vector<ASTNode*>> getRelationshipDetails(ASTNode* node);
    pair<vector<bool>, vector<ASTNode*>> getNodeDetails(ASTNode* node);
    Operator* pathPatternHandler(ASTNode* pattern);
    ASTNode* prepareWhereClause(std::string var1, std::string var2);
};

#endif // QUERY_PLANNER_H
