// QueryPlanner.cpp
#include "QueryPlanner.h"
#include "../util/Const.h"

Operator* QueryPlanner::optimizePlan(Operator* root) {
    // Placeholder for optimization logic
    std::cout << "Optimizing the execution plan..." << std::endl;
    return root; // Return the original plan for now
}
Operator* QueryPlanner::createExecutionPlan(ASTNode* ast, Operator* op, string var) {
    cout<<ast->nodeType<<endl;
    Operator* oprtr = op;
    // Example: Create a simple execution plan based on the AST
    if(ast->nodeType == Const::UNION)
    {

    }else if(ast->nodeType == Const::ALL)
    {

    }else if(ast->nodeType == Const::SINGLE_QUERY)
    {
        for(int i = 0; i< ast->elements.size(); i++)
        {
            oprtr = createExecutionPlan(ast->elements[i],oprtr);
        }
    }else if(ast->nodeType == Const::MULTI_PART_QUERY)
    {
        for(int i = 0; i< ast->elements.size(); i++)
        {
            oprtr = createExecutionPlan(ast->elements[i],oprtr);
        }
    }else if(ast->nodeType == Const::MATCH)
    {
        oprtr = createExecutionPlan(ast->elements[0]->elements[0],oprtr);
    }else if(ast->nodeType == Const::OPTIONAL)
    {

    }else if(ast->nodeType == Const::UNWIND)
    {

    }else if(ast->nodeType == Const::AS)
    {

    }else if(ast->nodeType == Const::MERGE)
    {

    }else if(ast->nodeType == Const::ON_CREATE)
    {

    }else if(ast->nodeType == Const::ON_MATCH)
    {

    }else if(ast->nodeType == Const::CREATE)
    {

    }else if(ast->nodeType == Const::MULTIPLE_SET)
    {

    }else if(ast->nodeType == Const::SET)
    {

    }else if(ast->nodeType == Const::SET_PLUS_EQAL)
    {

    }else if(ast->nodeType == Const::SET_EUAL)
    {

    }else if(ast->nodeType == Const::DELETE)
    {

    }else if(ast->nodeType == Const::DETACH)
    {

    }else if(ast->nodeType == Const::REMOVE_LIST)
    {

    }else if(ast->nodeType == Const::REMOVE)
    {

    }else if(ast->nodeType == Const::CALL)
    {

    }else if(ast->nodeType == Const::STAR)
    {

    }else if(ast->nodeType == Const::YIELD_ITEMS)
    {

    }else if(ast->nodeType == Const::YIELD)
    {

    }else if(ast->nodeType == Const::WITH)
    {

    }else if(ast->nodeType == Const::RETURN)
    {
        oprtr = createExecutionPlan(ast->elements[0],oprtr);
    }else if(ast->nodeType == Const::DISTINCT)
    {

    }else if(ast->nodeType == Const::RETURN_BODY)
    {
        if(ast->elements[0]->nodeType == Const::VARIABLE)
        {
            return new ProduceResults(ast->elements[0]->value, oprtr);
        }
    }else if(ast->nodeType == Const::ORDERED_BY)
    {

    }else if(ast->nodeType == Const::SKIP)
    {

    }else if(ast->nodeType == Const::LIMIT)
    {

    }else if(ast->nodeType == Const::ASC)
    {

    }else if(ast->nodeType == Const::DESC)
    {

    }else if(ast->nodeType == Const::WHERE)
    {

    }else if(ast->nodeType == Const::PATTERN)
    {
        oprtr = createExecutionPlan(ast->elements[0],oprtr);

    }else if(ast->nodeType == Const::PATTERN_ELEMENTS)
    {

    }else if(ast->nodeType == Const::NODE_PATTERN)
    {
        int size = ast->elements.size();
        bool isVar = ast->elements[0]->nodeType == Const::VARIABLE;
        string var_0 = isVar ? ast->elements[0]->value : "";

        if(ast->elements[0]->nodeType == Const::VARIABLE && size == 1)
        {
            return new AllNodeScan(ast->elements[0]->value);
        }else if(ast->elements[0]->nodeType == Const::NODE_LABELS && size == 1)
        {
            oprtr = createExecutionPlan(ast->elements[0], oprtr);
        }else if(ast->elements[0]->nodeType == Const::NODE_LABEL && size == 1)
        {
            oprtr = createExecutionPlan(ast->elements[0], oprtr);
        }else if(ast->elements[0]->nodeType == Const::PROPERTIES_MAP && size == 1)
        {
            oprtr = createExecutionPlan(ast->elements[0], oprtr);
        }else if(ast->elements[1]->nodeType == Const::NODE_LABELS && size == 2 && isVar)
        {
            oprtr = createExecutionPlan(ast->elements[1], oprtr, var_0);
        }else if(ast->elements[1]->nodeType == Const::NODE_LABEL && size == 2 && isVar)
        {
            oprtr = createExecutionPlan(ast->elements[1], oprtr, var_0);

        }else if(ast->elements[1]->nodeType == Const::PROPERTIES_MAP && size == 1 && isVar)
        {
            oprtr = createExecutionPlan(ast->elements[1], oprtr, var_0);
        }

    }else if(ast->nodeType == Const::PATTERN_ELEMENT_CHAIN)
    {

    }else if(ast->nodeType == Const::RELATIONSHIP_PATTTERN)
    {

    }else if(ast->nodeType == Const::UNIDIRECTION_ARROW)
    {

    }else if(ast->nodeType == Const::RELATIONSHIP_DETAILS)
    {

    }else if(ast->nodeType == Const::RELATIONSHIP_TYPES)
    {

    }else if(ast->nodeType == Const::NODE_LABELS)
    {

        vector<string> labels;
        string var_0 = var != "" ? var : "var_0";
        for(int i = 0; i<ast->elements.size();i++)
        {
            labels.push_back(ast->elements[i]->elements[0]->value);
        }
        return new MultipleNodeScanByLabel(labels, var_0);

    }else if(ast->nodeType == Const::NODE_LABEL)
    {
        string var_0 = var != "" ? var : "var_0";
        return new NodeScanByLabel(ast->elements[0]->value, var_0);

    }else if(ast->nodeType == Const::RANGE)
    {

    }else if(ast->nodeType == Const::PROPERTY)
    {

    }else if(ast->nodeType == Const::OR)
    {

    }else if(ast->nodeType == Const::XOR)
    {

    }else if(ast->nodeType == Const::AND)
    {

    }else if(ast->nodeType == Const::NOT)
    {

    }else if(ast->nodeType == Const::COMPARISON)
    {

    }else if(ast->nodeType == Const::GREATER_THAN)
    {

    }else if(ast->nodeType == Const::GREATER_THAN_LOWER_THAN)
    {

    }else if(ast->nodeType == Const::DOUBLE_EQUAL)
    {

    }else if(ast->nodeType == Const::LOWER_THAN)
    {

    }else if(ast->nodeType == Const::GREATER_THAN_OR_EQUAL)
    {

    }else if(ast->nodeType == Const::LOWER_THAN_OR_EQUAL)
    {

    }else if(ast->nodeType == Const::PREDICATE_EXPRESSIONS)
    {

    }else if(ast->nodeType == Const::STRING_PREDICATES)
    {

    }else if(ast->nodeType == Const::LIST_PREDICATES)
    {

    }else if(ast->nodeType == Const::NULL_PREDICATES)
    {

    }else if(ast->nodeType == Const::STARTS_WITH)
    {

    }else if(ast->nodeType == Const::ENDS_WITH)
    {

    }else if(ast->nodeType == Const::CONTAINS)
    {

    }else if(ast->nodeType == Const::IN)
    {

    }else if(ast->nodeType == Const::IS_NOT_NULL)
    {

    }else if(ast->nodeType == Const::IS_NULL)
    {

    }else if(ast->nodeType == Const::ADD_OR_SUBSTRACT)
    {

    }else if(ast->nodeType == Const::PLUS)
    {

    }else if(ast->nodeType == Const::MINUS)
    {

    }else if(ast->nodeType == Const::MULTIPLY_DIVID_MODULO)
    {

    }else if(ast->nodeType == Const::STAR)
    {

    }else if(ast->nodeType == Const::DIVIDE)
    {

    }else if(ast->nodeType == Const::POWER_OF)
    {

    }else if(ast->nodeType == Const::POWER)
    {

    }else if(ast->nodeType == Const::UNARY_PLUS)
    {

    }else if(ast->nodeType == Const::UNARY_MINUS)
    {

    }else if(ast->nodeType == Const::NON_ARITHMETIC_OPERATOR)
    {

    }else if(ast->nodeType == Const::LIST_INDEX_RANGE)
    {

    }else if(ast->nodeType == Const::LIST_INDEX)
    {

    }else if(ast->nodeType == Const::PROPERTY_LOOKUP)
    {

    }else if(ast->nodeType == Const::COUNT)
    {

    }else if(ast->nodeType == Const::CASE_PATTERN)
    {

    }else if(ast->nodeType == Const::CASE_EXPRESSION)
    {

    }else if(ast->nodeType == Const::ELSE_EXPRESSION)
    {

    }else if(ast->nodeType == Const::CASE)
    {

    }else if(ast->nodeType == Const::WHEN)
    {

    }else if(ast->nodeType == Const::THEN)
    {

    }else if(ast->nodeType == Const::LIST_COMPREHENSION)
    {

    }else if(ast->nodeType == Const::FILTER_RESULT)
    {

    }else if(ast->nodeType == Const::PATTERN_COMPREHENSION)
    {

    }else if(ast->nodeType == Const::EQUAL)
    {

    }else if(ast->nodeType == Const::FILTER_RESULT)
    {

    }else if(ast->nodeType == Const::ANY)
    {

    }else if(ast->nodeType == Const::NONE)
    {

    }else if(ast->nodeType == Const::SINGLE)
    {

    }else if(ast->nodeType == Const::FILTER_EXPRESSION)
    {

    }else if(ast->nodeType == Const::LIST_ITERATE)
    {

    }else if(ast->nodeType == Const::FUNCTION_BODY)
    {

    }else if(ast->nodeType == Const::ARGUMENTS)
    {

    }else if(ast->nodeType == Const::FUNCTION_NAME)
    {

    }else if(ast->nodeType == Const::FUNCTION)
    {

    }else if(ast->nodeType == Const::EXISTS)
    {

    }else if(ast->nodeType == Const::EXPLICIT_PROCEDURE)
    {

    }else if(ast->nodeType == Const::IMPLICIT_PROCEDURE)
    {

    }else if(ast->nodeType == Const::PROCEDURE_RESULT)
    {

    }else if(ast->nodeType == Const::PROCEDURE)
    {

    }else if(ast->nodeType == Const::NAMESPACE)
    {

    }else if(ast->nodeType == Const::VARIABLE)
    {

    }else if(ast->nodeType == Const::NULL_STRING)
    {

    }else if(ast->nodeType == Const::STRING)
    {

    }else if(ast->nodeType == Const::BOOLEAN)
    {

    }else if(ast->nodeType == Const::DECIMAL)
    {

    }else if(ast->nodeType == Const::HEX)
    {

    }else if(ast->nodeType == Const::OCTAL)
    {

    }else if(ast->nodeType == Const::EXP_DECIMAL)
    {

    }else if(ast->nodeType == Const::REGULAR_DECIMAL)
    {

    }else if(ast->nodeType == Const::LIST)
    {

    }else if(ast->nodeType == Const::PROPERTIES_MAP)
    {

    }else if(ast->nodeType == Const::PROPERTY)
    {

    }else if(ast->nodeType == Const::PARAMETER)
    {

    }else if(ast->nodeType == Const::SYMBOLIC_WORD)
    {

    }else if(ast->nodeType == Const::RESERVED_WORD)
    {

    }else if(ast->nodeType ==  Const::LEFT_ARRROW)
    {

    }else if(ast->nodeType == Const::RIGHT_ARROW)
    {

    }

    return oprtr;
}

bool QueryPlanner::isAvailable(string nodeType, ASTNode* subtree)
{
    if(subtree->nodeType == nodeType)
    {
        return true;
    }else
    {
        for(auto* element: subtree->elements)
        {
            bool isExist = isAvailable(nodeType,element);
            if(isExist)
            {
                return true;
            }
        }
        return false;
    }
}


