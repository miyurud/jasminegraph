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
    Operator* optimizePlan(Operator* root);
};

#endif // QUERY_PLANNER_H
