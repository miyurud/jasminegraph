// QueryPlanner.cpp
#include "QueryPlanner.h"
#include "../util/Const.h"

Operator* QueryPlanner::createExecutionPlan(ASTNode* ast, Operator* op, string var) {

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
        for(int i = 0; i< ast->elements.size(); i++)
        {
            oprtr = createExecutionPlan(ast->elements[i],oprtr);
        }
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
        vector<ASTNode*> var;
        if(isAllChildAreGivenType(Const::VARIABLE, ast))
        {
            return new ProduceResults(op, var);
        }

        vector<ASTNode*> nonArith = getSubTreeListByNodeType(ast,Const::NON_ARITHMETIC_OPERATOR);
        vector<ASTNode*> property;
        Operator* temp_opt = nullptr;

        if(!nonArith.empty())
        {
            for(auto* node: nonArith)
            {
                if(isAvailable(Const::PROPERTY_LOOKUP, node))
                {
                    property.push_back(node);
                }
            }
            if(!property.empty())
            {
                temp_opt = new CacheProperty(oprtr,property);
            }
        }

        if(temp_opt!=nullptr)
        {
            temp_opt = new Projection(temp_opt, ast->elements);
        }else
        {
            temp_opt = new Projection(oprtr, ast->elements);
        }

        return new ProduceResults(temp_opt, vector<ASTNode*>(ast->elements));



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
        return new Filter(op, ast);
    }else if(ast->nodeType == Const::PATTERN)
    {
        oprtr = createExecutionPlan(ast->elements[0],oprtr);

    }else if(ast->nodeType == Const::PATTERN_ELEMENTS)
    {
        vector<ASTNode*> patternElements = getSubTreeListByNodeType(ast, Const::PATTERN_ELEMENT_CHAIN);
        int strat;
        for(int i=patternElements.size()-1;i>=0;i--)
        {
            auto* e = patternElements[i];
            if(!e->elements[0]->elements[1]->elements.empty()
                && isAvailable(Const::RELATIONSHIP_TYPE,e->elements[0]->elements[1])
                && e->elements[0]->elements[0]->nodeType == Const::UNIDIRECTION_ARROW)
            {
                oprtr = new UndirectedRelationshipTypeScan("Person");
            }
        }

    }else if(ast->nodeType == Const::NODE_PATTERN)
    {
        if(ast->elements.empty())
        {
            return new AllNodeScan();
        }

        if(isAvailable(Const::PROPERTIES_MAP,ast))
        {
            if(isAvailable(Const::NODE_LABEL,ast) && isAvailable(Const::VARIABLE, ast))
            {
                oprtr = new NodeScanByLabel(ast->elements[1]->value,ast->elements[0]->value);
                return new Filter(oprtr, ast->elements[2]);
            }else if(!isAvailable(Const::NODE_LABEL,ast) && !isAvailable(Const::NODE_LABELS,ast) && isAvailable(Const::VARIABLE, ast))
            {
                oprtr = new AllNodeScan(ast->elements[0]->value);
                return new Filter(oprtr, ast->elements[1]);
            }else if(isAvailable(Const::NODE_LABEL,ast) && !isAvailable(Const::VARIABLE, ast))
            {
                oprtr = new NodeScanByLabel(ast->elements[0]->value);
                return new Filter(oprtr, ast->elements[1]);
            }else if(isAvailable(Const::NODE_LABELS,ast) && isAvailable(Const::VARIABLE, ast))
            {
                oprtr = createExecutionPlan(ast->elements[1],oprtr);
                return new Filter(oprtr, ast->elements[2]);
            }else if(isAvailable(Const::NODE_LABELS,ast) && !isAvailable(Const::VARIABLE, ast))
            {
                oprtr = createExecutionPlan(ast->elements[0],oprtr);
                return new Filter(oprtr, ast->elements[1]);
            }
        }else
        {
            if( !isAvailable(Const::NODE_LABEL,ast) && !isAvailable(Const::NODE_LABELS,ast) && isAvailable(Const::VARIABLE, ast))
            {
                return new AllNodeScan(ast->elements[0]->value);

            }else if(isAvailable(Const::VARIABLE, ast) && ast->elements[1]->nodeType == Const::NODE_LABEL)
            {
                return new NodeScanByLabel(ast->elements[1]->value,ast->elements[0]->value);

            }else if(!isAvailable(Const::VARIABLE, ast) && ast->elements[0]->nodeType == Const::NODE_LABEL)
            {
                return new NodeScanByLabel(ast->elements[0]->value);

            }else if(isAvailable(Const::NODE_LABELS,ast) && isAvailable(Const::VARIABLE, ast))
            {
                return createExecutionPlan(ast->elements[1],oprtr);

            }else if(isAvailable(Const::NODE_LABELS,ast) && !isAvailable(Const::VARIABLE, ast))
            {
                return createExecutionPlan(ast->elements[0],oprtr);
            }
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
        return new Filter(op,ast);
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

bool QueryPlanner::isAllChildAreGivenType(string nodeType, ASTNode* root)
{
    for(int i=0;i<root->elements.size(); i++)
    {
        if(root->elements[i]->nodeType != nodeType )
        {
            return false;
        }
    }
    return true;
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

vector<ASTNode*> QueryPlanner::getSubTreeListByNodeType(ASTNode* root, string nodeType)
{
    vector<ASTNode*> treeList;
    vector<ASTNode*> temp;
    for(auto* element : root->elements)
    {
        if(getSubtreeByType(element,nodeType))
        {
            treeList.push_back(element);
        }else if(!element->elements.empty())
        {
            temp = getSubTreeListByNodeType(element,nodeType);
            for (auto* e:temp) {
                treeList.push_back(e);
            }
            temp.clear();
        }
    }
    return treeList;
}

ASTNode* QueryPlanner::getSubtreeByType(ASTNode* root, string nodeType)
{
    if(root->nodeType == nodeType)
    {
        return root;
    }else
    {
        return nullptr;
    }
}


