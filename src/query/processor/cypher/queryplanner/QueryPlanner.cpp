// QueryPlanner.cpp
#include "QueryPlanner.h"


Operator* QueryPlanner::createExecutionPlan(ASTNode* ast, Operator* op, string var) {
    Operator* oprtr = op;
    // Example: Create a simple execution plan based on the AST
    if(ast->nodeType == "UNION")
    {

    }else if(ast->nodeType == "ALL")
    {

    }else if(ast->nodeType == "SINGLE_QUERY")
    {
        for(int i = 0; i< ast->elements.size(); i++)
        {
            oprtr = createExecutionPlan(ast->elements[i],oprtr);
        }
    }else if(ast->nodeType == "MULTI_PART_QUERY")
    {
        for(int i = 0; i< ast->elements.size(); i++)
        {
            oprtr = createExecutionPlan(ast->elements[i],oprtr);
        }
    }else if(ast->nodeType == "MATCH")
    {
        oprtr = createExecutionPlan(ast->elements[0]->elements[0],oprtr);
    }else if(ast->nodeType == "OPTIONAL")
    {

    }else if(ast->nodeType == "UNWIND")
    {

    }else if(ast->nodeType == "AS")
    {

    }else if(ast->nodeType == "MERGE")
    {

    }else if(ast->nodeType == "ON_CREATE")
    {

    }else if(ast->nodeType == "ON_MATCH")
    {

    }else if(ast->nodeType == "CREATE")
    {

    }else if(ast->nodeType == "MULTIPLE_SET")
    {

    }else if(ast->nodeType == "SET")
    {

    }else if(ast->nodeType == "SET_+=")
    {

    }else if(ast->nodeType == "SET_=")
    {

    }else if(ast->nodeType == "SET")
    {

    }else if(ast->nodeType == "DELETE")
    {

    }else if(ast->nodeType == "DETACH")
    {

    }else if(ast->nodeType == "REMOVE_LIST")
    {

    }else if(ast->nodeType == "REMOVE")
    {

    }else if(ast->nodeType == "CALL")
    {

    }else if(ast->nodeType == "*")
    {

    }else if(ast->nodeType == "YIELD_ITEMS")
    {

    }else if(ast->nodeType == "YIELD")
    {

    }else if(ast->nodeType == "WITH")
    {

    }else if(ast->nodeType == "RETURN")
    {
        oprtr = createExecutionPlan(ast->elements[0],oprtr);
    }else if(ast->nodeType == "DISTINCT")
    {

    }else if(ast->nodeType == "RETURN_BODY")
    {
        if(ast->elements[0]->nodeType == "VARIABLE")
        {
            return new ProduceResults(ast->elements[0]->value, oprtr);
        }
    }else if(ast->nodeType == "ORDERED_BY")
    {

    }else if(ast->nodeType == "SKIP")
    {

    }else if(ast->nodeType == "LIMIT")
    {

    }else if(ast->nodeType == "ASC")
    {

    }else if(ast->nodeType == "DESC")
    {

    }else if(ast->nodeType == "WHERE")
    {

    }else if(ast->nodeType == "PATTERN")
    {
        oprtr = createExecutionPlan(ast->elements[0],oprtr);

    }else if(ast->nodeType == "PATTERN_ELEMENTS")
    {

    }else if(ast->nodeType == "NODE_PATTERN")
    {
        int size = ast->elements.size();
        bool isVar = ast->elements[0]->nodeType == "VARIABLE";
        string var_0 = isVar ? ast->elements[0]->value : "";

        if(ast->elements[0]->nodeType == "VARIABLE" && size == 1)
        {
            return new AllNodeScan(ast->elements[0]->value);
        }else if(ast->elements[0]->nodeType == "NODE_LABELS" && size == 1)
        {
            oprtr = createExecutionPlan(ast->elements[0], oprtr);
        }else if(ast->elements[0]->nodeType == "NODE_LABEL" && size == 1)
        {
            oprtr = createExecutionPlan(ast->elements[0], oprtr);
        }else if(ast->elements[0]->nodeType == "PROPERTY_MAP" && size == 1)
        {
            oprtr = createExecutionPlan(ast->elements[0], oprtr);
        }else if(ast->elements[1]->nodeType == "NODE_LABELS" && size == 2 && isVar)
        {
            oprtr = createExecutionPlan(ast->elements[1], oprtr, var_0);
        }else if(ast->elements[1]->nodeType == "NODE_LABEL" && size == 2 && isVar)
        {
            oprtr = createExecutionPlan(ast->elements[1], oprtr, var_0);

        }else if(ast->elements[1]->nodeType == "PROPERTY_MAP" && size == 1 && isVar)
        {
            oprtr = createExecutionPlan(ast->elements[1], oprtr, var_0);
        }

    }else if(ast->nodeType == "PATTERN_ELEMENT_CHAIN")
    {

    }else if(ast->nodeType == "RELATIONSHIP_PATTTERN")
    {

    }else if(ast->nodeType == "UNIDIRECTION_ARROW")
    {

    }else if(ast->nodeType == "RELATIONSHIP_DETAILS")
    {

    }else if(ast->nodeType == "RELATIONSHIP_TYPES")
    {

    }else if(ast->nodeType == "NODE_LABELS")
    {

        vector<string> labels;
        string var_0 = var != "" ? var : "var_0";
        for(int i = 0; i<ast->elements.size();i++)
        {
            labels.push_back(ast->elements[i]->elements[0]->value);
        }
        return new MultipleNodeScanByLabel(labels, var_0);

    }else if(ast->nodeType == "NODE_LABEL")
    {
        string var_0 = var != "" ? var : "var_0";
        return new NodeScanByLabel(ast->elements[0]->value, var_0);

    }else if(ast->nodeType == "RANGE")
    {

    }else if(ast->nodeType == "PROPERTY")
    {

    }else if(ast->nodeType == "OR")
    {

    }else if(ast->nodeType == "XOR")
    {

    }else if(ast->nodeType == "AND")
    {

    }else if(ast->nodeType == "NOT")
    {

    }else if(ast->nodeType == "COMPARISON")
    {

    }else if(ast->nodeType == ">")
    {

    }else if(ast->nodeType == "<>")
    {

    }else if(ast->nodeType == "==")
    {

    }else if(ast->nodeType == "<")
    {

    }else if(ast->nodeType == ">=")
    {

    }else if(ast->nodeType == "<=")
    {

    }else if(ast->nodeType == "PREDICATE_EXPRESSIONS")
    {

    }else if(ast->nodeType == "STRING_PREDICATES")
    {

    }else if(ast->nodeType == "LIST_PREDICATES")
    {

    }else if(ast->nodeType == "NULL_PREDICATES")
    {

    }else if(ast->nodeType == "STARTS_WITH")
    {

    }else if(ast->nodeType == "ENDS_WITH")
    {

    }else if(ast->nodeType == "CONTAINS")
    {

    }else if(ast->nodeType == "IN")
    {

    }else if(ast->nodeType == "IS_NOT_NULL")
    {

    }else if(ast->nodeType == "IS_NULL")
    {

    }else if(ast->nodeType == "ADD_OR_SUBSTRACT")
    {

    }else if(ast->nodeType == "+")
    {

    }else if(ast->nodeType == "-")
    {

    }else if(ast->nodeType == "MULTIPLY_DIVID_MODULO")
    {

    }else if(ast->nodeType == "*")
    {

    }else if(ast->nodeType == "/")
    {

    }else if(ast->nodeType == "POWER_OF")
    {

    }else if(ast->nodeType == "^")
    {

    }else if(ast->nodeType == "UNARY_+")
    {

    }else if(ast->nodeType == "UNARY_-")
    {

    }else if(ast->nodeType == "NON_ARITHMETIC_OPERATOR")
    {

    }else if(ast->nodeType == "LIST_INDEX_RANGE")
    {

    }else if(ast->nodeType == "LIST_INDEX")
    {

    }else if(ast->nodeType == "PROPERTY_LOOKUP")
    {

    }else if(ast->nodeType == "COUNT")
    {

    }else if(ast->nodeType == "CASE_PATTERN")
    {

    }else if(ast->nodeType == "CASE_EXPRESSION")
    {

    }else if(ast->nodeType == "ELSE_EXPRESSION")
    {

    }else if(ast->nodeType == "CASE")
    {

    }else if(ast->nodeType == "WHEN")
    {

    }else if(ast->nodeType == "THEN")
    {

    }else if(ast->nodeType == "LIST_COMPREHENSION")
    {

    }else if(ast->nodeType == "FILTER_RESULT")
    {

    }else if(ast->nodeType == "PATTERN_COMPREHENSION")
    {

    }else if(ast->nodeType == "=")
    {

    }else if(ast->nodeType == "FILTER_RESULT")
    {

    }else if(ast->nodeType == "ANY")
    {

    }else if(ast->nodeType == "NONE")
    {

    }else if(ast->nodeType == "SINGLE")
    {

    }else if(ast->nodeType == "FILTER_EXPRESSION")
    {

    }else if(ast->nodeType == "LIST_ITERATE")
    {

    }else if(ast->nodeType == "FUNCTION_BODY")
    {

    }else if(ast->nodeType == "ARGUMENTS")
    {

    }else if(ast->nodeType == "FUNCTION_NAME")
    {

    }else if(ast->nodeType == "FUNCTION")
    {

    }else if(ast->nodeType == "EXISTS")
    {

    }else if(ast->nodeType == "EXPLICIT_PROCEDURE")
    {

    }else if(ast->nodeType == "IMPLICIT_PROCEDURE")
    {

    }else if(ast->nodeType == "PROCEDURE_RESULT")
    {

    }else if(ast->nodeType == "PROCEDURE")
    {

    }else if(ast->nodeType == "NAMESPACE")
    {

    }else if(ast->nodeType == "VARIABLE")
    {

    }else if(ast->nodeType == "NULL")
    {

    }else if(ast->nodeType == "STRING")
    {

    }else if(ast->nodeType == "BOOLEAN")
    {

    }else if(ast->nodeType == "DECIMAL")
    {

    }else if(ast->nodeType == "HEX")
    {

    }else if(ast->nodeType == "OCTAL")
    {

    }else if(ast->nodeType == "EXP_DECIMAL")
    {

    }else if(ast->nodeType == "REGULAR_DECIMAL")
    {

    }else if(ast->nodeType == "LIST")
    {

    }else if(ast->nodeType == "PROPERIES_MAP")
    {

    }else if(ast->nodeType == "PROPERTY")
    {

    }else if(ast->nodeType == "PARAMETER")
    {

    }else if(ast->nodeType == "SYMBOLIC_WORD")
    {

    }else if(ast->nodeType == "RESERVED_WORD")
    {

    }else if(ast->nodeType == "LEFT_ARRROW")
    {

    }else if(ast->nodeType == "RIGHT_ARROW")
    {

    }

    return oprtr;
}

Operator* QueryPlanner::optimizePlan(Operator* root) {
    // Placeholder for optimization logic
    std::cout << "Optimizing the execution plan..." << std::endl;
    return root; // Return the original plan for now
}
