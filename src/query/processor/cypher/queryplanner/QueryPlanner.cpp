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

// QueryPlanner.cpp
#include "QueryPlanner.h"
#include "../util/Const.h"
#include "../astbuilder/ASTLeafValue.h"
#include "../astbuilder/ASTInternalNode.h"
#include "../astbuilder/ASTNode.h"

Operator* QueryPlanner::createExecutionPlan(ASTNode* ast, Operator* op, string var) {
    Operator* currentOperator = op;
    // Example: Create a simple execution plan based on the AST
    if (ast->nodeType == Const::UNION) {
        // TODO(thamindumk): Implement UNION
    } else if (ast->nodeType == Const::ALL) {
        // TODO(thamindumk): Implement ALL
    } else if (ast->nodeType == Const::SINGLE_QUERY) {
        for (int i = 0; i < ast->elements.size(); i++) {
            currentOperator = createExecutionPlan(ast->elements[i], currentOperator);
        }
    } else if (ast->nodeType == Const::MULTI_PART_QUERY) {
        int i = 0;
        currentOperator = createExecutionPlan(ast->elements[i++], currentOperator);
        while (i < ast->elements.size()) {
            Apply* apply = new Apply(currentOperator);
            if (i != ast->elements.size() - 1) {
                apply->addOperator(createExecutionPlan(ast->elements[i++]));
            } else {
                apply->addOperator(createExecutionPlan(ast->elements[i++]));
                currentOperator = apply;
            }
        }
    } else if (ast->nodeType == Const::MATCH) {
        if (isAvailable(Const::FUNCTION_BODY, ast)) {
            auto where = getSubTreeListByNodeType(ast, Const::WHERE);
            auto comparisons = getSubTreeListByNodeType(ast, Const::COMPARISON);
            for (ASTNode const* a : comparisons) {
                if (a->elements[0]->nodeType == Const::FUNCTION_BODY
                    && a->elements[0]->elements[0]->elements[1]->value == "id"
                    && a->elements[1]->nodeType == "=="
                    && !isAvailable(Const::OR, where[0])
                    && !isAvailable(Const::XOR, where[0])) {
                    string id = a->elements[1]->elements[0]->value;
                    string variable = a->elements[0]->elements[1]->elements[0]->value;
                    currentOperator = new NodeByIdSeek(id, variable);
                }
            }
        }

        for (int i = 0; i< ast->elements.size(); i++) {
            currentOperator = createExecutionPlan(ast->elements[i], currentOperator);
        }

    } else if (ast->nodeType == Const::OPTIONAL) {
        // TODO(thamindumk): Implement OPTIONAL
    } else if (ast->nodeType == Const::UNWIND) {
        // TODO(thamindumk): Implement UNWIND
    } else if (ast->nodeType == Const::AS) {
        // TODO(thamindumk): Implement AS
    } else if (ast->nodeType == Const::MERGE) {
        // TODO(thamindumk): Implement MERGE
    } else if (ast->nodeType == Const::ON_CREATE) {
        // TODO(thamindumk): Implement ON_CREATE
    } else if (ast->nodeType == Const::ON_MATCH) {
        // TODO(thamindumk): Implement ON_MATCH
    } else if (ast->nodeType == Const::CREATE) {
        // TODO(thamindumk): Implement CREATE
    } else if (ast->nodeType == Const::MULTIPLE_SET) {
        // TODO(thamindumk): Implement MULTIPLE_SET
    } else if (ast->nodeType == Const::SET) {
        // TODO(thamindumk): Implement SET
    } else if (ast->nodeType == Const::SET_PLUS_EQAL) {
        // TODO(thamindumk): Implement SET_PLUS_EQAL
    } else if (ast->nodeType == Const::SET_EUAL) {
        // TODO(thamindumk): Implement SET_EUAL
    } else if (ast->nodeType == Const::DELETE) {
        // TODO(thamindumk): Implement DELETE
    } else if (ast->nodeType == Const::DETACH) {
        // TODO(thamindumk): Implement DETACH
    } else if (ast->nodeType == Const::REMOVE_LIST) {
        // TODO(thamindumk): Implement REMOVE_LIST
    } else if (ast->nodeType == Const::REMOVE) {
        // TODO(thamindumk): Implement REMOVE
    } else if (ast->nodeType == Const::CALL) {
        // TODO(thamindumk): Implement CALL
    } else if (ast->nodeType == Const::STAR) {
        // TODO(thamindumk): Implement STAR
    } else if (ast->nodeType == Const::YIELD_ITEMS) {
        // TODO(thamindumk): Implement YIELD_ITEMS
    } else if (ast->nodeType == Const::YIELD) {
        // TODO(thamindumk): Implement YIELD
    } else if (ast->nodeType == Const::WITH) {
        currentOperator = createExecutionPlan(ast->elements[0], currentOperator);
    } else if (ast->nodeType == Const::RETURN) {
        currentOperator = createExecutionPlan(ast->elements[0], currentOperator);
    } else if (ast->nodeType == Const::DISTINCT) {
        // TODO(thamindumk): Implement DISTINCT
    } else if (ast->nodeType == Const::RETURN_BODY) {
        vector<ASTNode*> var;
        if (isAllChildrenAreGivenType(Const::VARIABLE, ast)) {
            var = ast->elements;
            return new ProduceResults(currentOperator, var);
        }

        vector<ASTNode*> nonArith = getSubTreeListByNodeType(ast, Const::NON_ARITHMETIC_OPERATOR);
        vector<ASTNode*> property;
        Operator* temp_opt = nullptr;

        if (!nonArith.empty()) {
            for (auto* node : nonArith) {
                if (isAvailable(Const::PROPERTY_LOOKUP, node)) {
                    property.push_back(node);
                }
            }
            if (!property.empty()) {
                temp_opt = new CacheProperty(currentOperator, property);
            }
        }

        if (temp_opt != nullptr) {
            temp_opt = new Projection(temp_opt, ast->elements);
        } else {
            temp_opt = new Projection(currentOperator, ast->elements);
        }
        return new ProduceResults(temp_opt, vector<ASTNode*>(ast->elements));
    } else if (ast->nodeType == Const::ORDERED_BY) {
        // TODO(thamindumk): Implement ORDERED_BY
    } else if (ast->nodeType == Const::SKIP) {
        // TODO(thamindumk): Implement SKIP
    } else if (ast->nodeType == Const::LIMIT) {
        // TODO(thamindumk): Implement LIMIT
    } else if (ast->nodeType == Const::ASC) {
        // TODO(thamindumk): Implement ASC
    } else if (ast->nodeType == Const::DESC) {
        // TODO(thamindumk): Implement DESC
    } else if (ast->nodeType == Const::WHERE) {
        auto filterCase = pair<string, ASTNode*>("null", ast);
        vector<pair<string, ASTNode*>> vec = {filterCase};
        return new Filter(op, vec);
    } else if (ast->nodeType == Const::PATTERN) {
        return createExecutionPlan(ast->elements[0], currentOperator);
    } else if (ast->nodeType == Const::PATTERN_ELEMENTS) {
        return pathPatternHandler(ast, currentOperator);
    } else if (ast->nodeType == Const::NODE_PATTERN) {
        if (ast->elements.empty()) {
            return new AllNodeScan();
        }

        if (isAvailable(Const::PROPERTIES_MAP, ast)) {
            if (isAvailable(Const::NODE_LABEL, ast) && isAvailable(Const::VARIABLE, ast)) {
                if (!currentOperator) {
                    currentOperator = new NodeScanByLabel(ast->elements[1]->value, ast->elements[0]->value);
                }
                auto filterCase = pair<string, ASTNode*>(ast->elements[0]->value, ast->elements[2]);
                vector<pair<string, ASTNode*>> vec = {filterCase};
                return new Filter(currentOperator, vec);
            } else if (!isAvailable(Const::NODE_LABEL, ast) &&
                        !isAvailable(Const::NODE_LABELS, ast) &&
                        isAvailable(Const::VARIABLE, ast)) {
                if (!currentOperator) {
                    currentOperator = new AllNodeScan(ast->elements[0]->value);
                }
                auto filterCase = pair<string, ASTNode*>(ast->elements[0]->value, ast->elements[1]);
                vector<pair<string, ASTNode*>> vec = {filterCase};
                return new Filter(currentOperator, vec);
            } else if (isAvailable(Const::NODE_LABEL, ast) &&
                        !isAvailable(Const::VARIABLE, ast)) {
                if (!currentOperator) {
                    currentOperator = new NodeScanByLabel(ast->elements[0]->value);
                }
                auto filterCase = pair<string, ASTNode*>("node_0", ast->elements[1]);
                vector<pair<string, ASTNode*>> vec = {filterCase};
                return new Filter(currentOperator, vec);
            } else if (isAvailable(Const::NODE_LABELS, ast) &&
                        isAvailable(Const::VARIABLE, ast)) {
                if (!currentOperator) {
                    currentOperator = createExecutionPlan(ast->elements[1], currentOperator);
                }
                auto filterCase = pair<string, ASTNode*>(ast->elements[0]->value, ast->elements[2]);
                vector<pair<string, ASTNode*>> vec = {filterCase};
                return new Filter(currentOperator, vec);
            } else if (isAvailable(Const::NODE_LABELS, ast) &&
                        !isAvailable(Const::VARIABLE, ast)) {
                currentOperator = createExecutionPlan(ast->elements[0], currentOperator);
                auto filterCase = pair<string, ASTNode*>("node_0", ast->elements[1]);
                vector<pair<string, ASTNode*>> vec = {filterCase};
                return new Filter(currentOperator, vec);
            }
        } else {
            if (!isAvailable(Const::NODE_LABEL, ast) &&
                !isAvailable(Const::NODE_LABELS, ast) &&
                isAvailable(Const::VARIABLE, ast)) {
                if (currentOperator) {
                    return currentOperator;
                }
                return new AllNodeScan(ast->elements[0]->value);
            } else if (isAvailable(Const::VARIABLE, ast) &&
                ast->elements[1]->nodeType == Const::NODE_LABEL) {
                if (currentOperator) {
                    return currentOperator;
                }
                return new NodeScanByLabel(ast->elements[1]->value, ast->elements[0]->value);
            } else if (!isAvailable(Const::VARIABLE, ast) &&
                ast->elements[0]->nodeType == Const::NODE_LABEL) {
                if (currentOperator) {
                    return currentOperator;
                }
                return new NodeScanByLabel(ast->elements[0]->value);
            } else if (isAvailable(Const::NODE_LABELS, ast) &&
                    isAvailable(Const::VARIABLE, ast)) {
                return createExecutionPlan(ast->elements[1], currentOperator);
            } else if (isAvailable(Const::NODE_LABELS, ast) &&
                    !isAvailable(Const::VARIABLE, ast)) {
                return createExecutionPlan(ast->elements[0], currentOperator);
            }
        }

    } else if (ast->nodeType == Const::PATTERN_ELEMENT_CHAIN) {
        // TODO(thamindumk): Implement PATTERN_ELEMENT_CHAIN
    } else if (ast->nodeType == Const::RELATIONSHIP_PATTTERN) {
        // TODO(thamindumk): Implement RELATIONSHIP_PATTTERN
    } else if (ast->nodeType == Const::UNIDIRECTION_ARROW) {
        // TODO(thamindumk): Implement UNIDIRECTION_ARROW
    } else if (ast->nodeType == Const::RELATIONSHIP_DETAILS) {
        // TODO(thamindumk): Implement RELATIONSHIP_DETAILS
    } else if (ast->nodeType == Const::RELATIONSHIP_TYPES) {
        // TODO(thamindumk): Implement RELATIONSHIP_TYPES
    } else if (ast->nodeType == Const::NODE_LABELS) {
        if (currentOperator) {
            vector<pair<string, ASTNode*>> vec;
            for (int i = 0; i < ast->elements.size(); i++) {
                vec.push_back(pair<string, ASTNode*>(ast->elements[i]->elements[0]->value, ast->elements[i]));
            }
            return new Filter(currentOperator, vec);
        }
        vector<string> labels;
        string var_0 = var != "" ? var : "var_0";
        for (int i = 0; i < ast->elements.size(); i++) {
            labels.push_back(ast->elements[i]->elements[0]->value);
        }
        return new MultipleNodeScanByLabel(labels, var_0);

    } else if (ast->nodeType == Const::NODE_LABEL) {
        string var_0 = var != "" ? var : "var_0";
        return new NodeScanByLabel(ast->elements[0]->value, var_0);
    } else if (ast->nodeType == Const::RANGE) {
        // TODO(thamindumk): Implement RANGE
    } else if (ast->nodeType == Const::PROPERTY) {
        // TODO(thamindumk): Implement PROPERTY
    } else if (ast->nodeType == Const::OR) {
        // TODO(thamindumk): Implement OR
    } else if (ast->nodeType == Const::XOR) {
        // TODO(thamindumk): Implement XOR
    } else if (ast->nodeType == Const::AND) {
        // TODO(thamindumk): Implement AND
    } else if (ast->nodeType == Const::NOT) {
        // TODO(thamindumk): Implement NOT
    } else if (ast->nodeType == Const::COMPARISON) {
        // TODO(thamindumk): Implement COMPARISON
    } else if (ast->nodeType == Const::GREATER_THAN) {
        // TODO(thamindumk): Implement GREATER_THAN
    } else if (ast->nodeType == Const::GREATER_THAN_LOWER_THAN) {
        // TODO(thamindumk): Implement GREATER_THAN_LOWER_THAN
    } else if (ast->nodeType == Const::DOUBLE_EQUAL) {
        // TODO(thamindumk): Implement DOUBLE_EQUAL
    } else if (ast->nodeType == Const::LOWER_THAN) {
        // TODO(thamindumk): Implement LOWER_THAN
    } else if (ast->nodeType == Const::GREATER_THAN_OR_EQUAL) {
        // TODO(thamindumk): Implement GREATER_THAN_OR_EQUAL
    } else if (ast->nodeType == Const::LOWER_THAN_OR_EQUAL) {
        // TODO(thamindumk): Implement LOWER_THAN_OR_EQUAL
    } else if (ast->nodeType == Const::PREDICATE_EXPRESSIONS) {
        // TODO(thamindumk): Implement PREDICATE_EXPRESSIONS
    } else if (ast->nodeType == Const::STRING_PREDICATES) {
        // TODO(thamindumk): Implement STRING_PREDICATES
    } else if (ast->nodeType == Const::LIST_PREDICATES) {
        // TODO(thamindumk): Implement LIST_PREDICATES
    } else if (ast->nodeType == Const::NULL_PREDICATES) {
        // TODO(thamindumk): Implement NULL_PREDICATES
    } else if (ast->nodeType == Const::STARTS_WITH) {
        // TODO(thamindumk): Implement STARTS_WITH
    } else if (ast->nodeType == Const::ENDS_WITH) {
        // TODO(thamindumk): Implement ENDS_WITH
    } else if (ast->nodeType == Const::CONTAINS) {
        // TODO(thamindumk): Implement CONTAINS
    } else if (ast->nodeType == Const::IN) {
        // TODO(thamindumk): Implement IN
    } else if (ast->nodeType == Const::IS_NOT_NULL) {
        // TODO(thamindumk): Implement IS_NOT_NULL
    } else if (ast->nodeType == Const::IS_NULL) {
        // TODO(thamindumk): Implement IS_NULL
    } else if (ast->nodeType == Const::ADD_OR_SUBSTRACT) {
        // TODO(thamindumk): Implement ADD_OR_SUBSTRACT
    } else if (ast->nodeType == Const::PLUS) {
        // TODO(thamindumk): Implement PLUS
    } else if (ast->nodeType == Const::MINUS) {
        // TODO(thamindumk): Implement MINUS
    } else if (ast->nodeType == Const::MULTIPLY_DIVID_MODULO) {
        // TODO(thamindumk): Implement MULTIPLY_DIVID_MODULO
    } else if (ast->nodeType == Const::STAR) {
        // TODO(thamindumk): Implement STAR
    } else if (ast->nodeType == Const::DIVIDE) {
        // TODO(thamindumk): Implement DIVIDE
    } else if (ast->nodeType == Const::POWER_OF) {
        // TODO(thamindumk): Implement POWER_OF
    } else if (ast->nodeType == Const::POWER) {
        // TODO(thamindumk): Implement POWER
    } else if (ast->nodeType == Const::UNARY_PLUS) {
        // TODO(thamindumk): Implement UNARY_PLUS
    } else if (ast->nodeType == Const::UNARY_MINUS) {
        // TODO(thamindumk): Implement UNARY_MINUS
    } else if (ast->nodeType == Const::NON_ARITHMETIC_OPERATOR) {
        // TODO(thamindumk): Implement NON_ARITHMETIC_OPERATOR
    } else if (ast->nodeType == Const::LIST_INDEX_RANGE) {
        // TODO(thamindumk): Implement LIST_INDEX_RANGE
    } else if (ast->nodeType == Const::LIST_INDEX) {
        // TODO(thamindumk): Implement LIST_INDEX
    } else if (ast->nodeType == Const::PROPERTY_LOOKUP) {
        // TODO(thamindumk): Implement PROPERTY_LOOKUP
    } else if (ast->nodeType == Const::COUNT) {
        // TODO(thamindumk): Implement COUNT
    } else if (ast->nodeType == Const::CASE_PATTERN) {
        // TODO(thamindumk): Implement CASE_PATTERN
    } else if (ast->nodeType == Const::CASE_EXPRESSION) {
        // TODO(thamindumk): Implement CASE_EXPRESSION
    } else if (ast->nodeType == Const::ELSE_EXPRESSION) {
        // TODO(thamindumk): Implement ELSE_EXPRESSION
    } else if (ast->nodeType == Const::CASE) {
        // TODO(thamindumk): Implement CASE
    } else if (ast->nodeType == Const::WHEN) {
        // TODO(thamindumk): Implement WHEN
    } else if (ast->nodeType == Const::THEN) {
        // TODO(thamindumk): Implement THEN
    } else if (ast->nodeType == Const::LIST_COMPREHENSION) {
        // TODO(thamindumk): Implement LIST_COMPREHENSION
    } else if (ast->nodeType == Const::FILTER_RESULT) {
        // TODO(thamindumk): Implement FILTER_RESULT
    } else if (ast->nodeType == Const::PATTERN_COMPREHENSION) {
        // TODO(thamindumk): Implement PATTERN_COMPREHENSION
    } else if (ast->nodeType == Const::EQUAL) {
        // TODO(thamindumk): Implement EQUAL
    } else if (ast->nodeType == Const::FILTER_RESULT) {
        // TODO(thamindumk): Implement FILTER_RESULT
    } else if (ast->nodeType == Const::ANY) {
        // TODO(thamindumk): Implement ANY
    } else if (ast->nodeType == Const::NONE) {
        // TODO(thamindumk): Implement NONE
    } else if (ast->nodeType == Const::SINGLE) {
        // TODO(thamindumk): Implement SINGLE
    } else if (ast->nodeType == Const::FILTER_EXPRESSION) {
        // TODO(thamindumk): Implement FILTER_EXPRESSION
    } else if (ast->nodeType == Const::LIST_ITERATE) {
        // TODO(thamindumk): Implement LIST_ITERATE
    } else if (ast->nodeType == Const::FUNCTION_BODY) {
        // TODO(thamindumk): Implement FUNCTION_BODY
    } else if (ast->nodeType == Const::ARGUMENTS) {
        // TODO(thamindumk): Implement ARGUMENTS
    } else if (ast->nodeType == Const::FUNCTION_NAME) {
        // TODO(thamindumk): Implement FUNCTION_NAME
    } else if (ast->nodeType == Const::FUNCTION) {
        // TODO(thamindumk): Implement FUNCTION
    } else if (ast->nodeType == Const::EXISTS) {
        // TODO(thamindumk): Implement EXISTS
    } else if (ast->nodeType == Const::EXPLICIT_PROCEDURE) {
        // TODO(thamindumk): Implement EXPLICIT_PROCEDURE
    } else if (ast->nodeType == Const::IMPLICIT_PROCEDURE) {
        // TODO(thamindumk): Implement IMPLICIT_PROCEDURE
    } else if (ast->nodeType == Const::PROCEDURE_RESULT) {
        // TODO(thamindumk): Implement PROCEDURE_RESULT
    } else if (ast->nodeType == Const::PROCEDURE) {
        // TODO(thamindumk): Implement PROCEDURE
    } else if (ast->nodeType == Const::NAMESPACE) {
        // TODO(thamindumk): Implement NAMESPACE
    } else if (ast->nodeType == Const::VARIABLE) {
        // TODO(thamindumk): Implement VARIABLE
    } else if (ast->nodeType == Const::NULL_STRING) {
        // TODO(thamindumk): Implement NULL_STRING
    } else if (ast->nodeType == Const::STRING) {
        // TODO(thamindumk): Implement STRING
    } else if (ast->nodeType == Const::BOOLEAN) {
        // TODO(thamindumk): Implement BOOLEAN
    } else if (ast->nodeType == Const::DECIMAL) {
        // TODO(thamindumk): Implement DECIMAL
    } else if (ast->nodeType == Const::HEX) {
        // TODO(thamindumk): Implement HEX
    } else if (ast->nodeType == Const::OCTAL) {
        // TODO(thamindumk): Implement OCTAL
    } else if (ast->nodeType == Const::EXP_DECIMAL) {
        // TODO(thamindumk): Implement EXP_DECIMAL
    } else if (ast->nodeType == Const::REGULAR_DECIMAL) {
        // TODO(thamindumk): Implement REGULAR_DECIMAL
    } else if (ast->nodeType == Const::LIST) {
        // TODO(thamindumk): Implement LIST
    } else if (ast->nodeType == Const::PROPERTIES_MAP) {
        // TODO(thamindumk): Implement PROPERTIES_MAP
    } else if (ast->nodeType == Const::PROPERTY) {
        // TODO(thamindumk): Implement PROPERTY
    } else if (ast->nodeType == Const::PARAMETER) {
        // TODO(thamindumk): Implement PARAMETER
    } else if (ast->nodeType == Const::SYMBOLIC_WORD) {
        // TODO(thamindumk): Implement SYMBOLIC_WORD
    } else if (ast->nodeType == Const::RESERVED_WORD) {
        // TODO(thamindumk): Implement RESERVED_WORD
    } else if (ast->nodeType ==  Const::LEFT_ARRROW) {
        // TODO(thamindumk): Implement LEFT_ARRROW
    } else if (ast->nodeType == Const::RIGHT_ARROW) {
        // TODO(thamindumk): Implement RIGHT_ARROW
    }
    return currentOperator;
}

bool QueryPlanner::isAllChildrenAreGivenType(string nodeType, ASTNode* root ) {
    for (int i = 0; i < root->elements.size(); i++) {
        if (root->elements[i]->nodeType != nodeType) {
            return false;
        }
    }
    return true;
}

bool QueryPlanner::isAvailable(string nodeType, ASTNode* subtree ) {
    if (subtree->nodeType == nodeType) {
        return true;
    } else {
        for (auto* element : subtree->elements) {
            bool isExist = isAvailable(nodeType, element);
            if (isExist) {
                return true;
            }
        }
        return false;
    }
}

vector<ASTNode*> QueryPlanner::getSubTreeListByNodeType(ASTNode* root, string nodeType ) {
    vector<ASTNode*> treeList;
    vector<ASTNode*> temp;
    for (auto* element : root->elements) {
        if (verifyTreeType(element, nodeType)) {
            treeList.push_back(element);
        } else if (!element->elements.empty()) {
            temp = getSubTreeListByNodeType(element, nodeType);
            for (auto* e : temp) {
                treeList.push_back(e);
            }
            temp.clear();
        }
    }
    return treeList;
}

ASTNode* QueryPlanner::verifyTreeType(ASTNode* root, string nodeType ) {
    if (root->nodeType == nodeType) {
        return root;
    } else {
        return nullptr;
    }
}


pair<vector<bool>, vector<ASTNode *>> QueryPlanner::getRelationshipDetails(ASTNode *node) {
    vector<bool> availability = {false, false, false};
    vector<ASTNode*> nodes;
    for (int i = 0; i < Const::THREE; i++) {
        if (i < node->elements.size()) {
            auto* e = node->elements[i];
            if (e->nodeType == Const::VARIABLE) {
                availability[0] = true;
                nodes.push_back(e);
            } else if (e->nodeType == Const::RELATIONSHIP_TYPE) {
                availability[1] = true;
                nodes.push_back(e);
            } else if (e->nodeType == Const::RELATIONSHIP_TYPES) {
                availability[1] = true;
                auto* types = e;
                types->elements.clear();
                for (auto* type : e->elements) {
                    types->elements.push_back(type->elements[0]);
                }
                nodes.push_back(types);
            } else if (e->nodeType == Const::PROPERTIES_MAP) {
                availability[2] = true;
                nodes.push_back(e);
            }
        } else {
            nodes.push_back(nullptr);
        }
    }
    auto outputPair = pair<vector<bool>, vector<ASTNode *>>(availability, nodes);
    return outputPair;
}

pair<vector<bool>, vector<ASTNode *>> QueryPlanner::getNodeDetails(ASTNode *node) {
    vector<bool> availability = {false, false, false};
    vector<ASTNode*> nodes;
    for (int i = 0; i < Const::THREE; i++) {
        if (i < node->elements.size()) {
            auto* e = node->elements[i];
            if (e->nodeType == Const::VARIABLE) {
                availability[0] = true;
                nodes.push_back(e);
            } else if (e->nodeType == Const::NODE_LABEL || e->nodeType == Const::NODE_LABELS) {
                availability[1] = true;
                nodes.push_back(e);
            } else if (e->nodeType == Const::PROPERTIES_MAP) {
                availability[2] = true;
                nodes.push_back(e);
            }
        } else {
            nodes.push_back(nullptr);
        }
    }
    auto outputPair = pair<vector<bool>, vector<ASTNode *>>(availability, nodes);
    return outputPair;
}

ASTNode *QueryPlanner::prepareWhereClause(std::string var1, std::string var2) {
    auto* whereClause = new ASTInternalNode(Const::WHERE);
    auto* comp = new ASTInternalNode(Const::COMPARISON);
    auto* result1 = new ASTLeafValue(Const::VARIABLE, var1);
    auto* result2 = new ASTLeafValue(Const::VARIABLE, var2);
    auto* comp_op = new ASTInternalNode(Const::GREATER_THAN_LOWER_THAN);
    comp_op->addElements(result2);
    comp->addElements(result1);
    comp->addElements(comp_op);
    whereClause->addElements(comp);
    return whereClause;
}

Operator* QueryPlanner::pathPatternHandler(ASTNode *pattern, Operator* inputOperator) {
    auto* startNode  = pattern->elements[0];
    vector<ASTNode*> patternElements = getSubTreeListByNodeType(pattern, Const::PATTERN_ELEMENT_CHAIN);
    bool isRelTypeExist = false;
    bool isNodeLabelExist = false;
    bool isDirectionExist = false;
    int index;
    int labelIndex = -1;
    int directionIndex = -1;

    if (inputOperator) {
        string variable = static_cast<NodeByIdSeek*>(inputOperator)->getVariable();
        for (int i = patternElements.size()-1; i >= 0; i--) {
            auto* e = patternElements[i];
            if (e->elements[1]->elements.size() && variable == e->elements[1]->elements[0]->value) {
                index = i+1;
            }
        }
        if (pattern->elements[0]->elements[0]->value == variable) {
            index = 0;
        }

        if (index == 0) {
            vector<pair<string, ASTNode*>> filterCases;
            string startVar = variable;
            string prevRel = "null";
            for (int right = index; right < patternElements.size(); right++) {
                filterCases.clear();
                auto analyzedRel = getRelationshipDetails(patternElements[right]->elements[0]->elements[1]);
                auto analyzedNode = getNodeDetails(patternElements[right]->elements[1]);

                string newStartVar = startVar;
                string newDestvar = analyzedNode.first[0]? analyzedNode.second[0]->value :
                        "node_var_"+ to_string(right);
                string newRelVar = analyzedRel.first[0] ? analyzedRel.second[0]->value :
                        "edge_var_"+to_string(right);
                string newRelType = analyzedRel.first[1] ? analyzedRel.second[1]->elements[0]->value :
                        "null";

                if (patternElements[right]->elements[0]->elements[0]->nodeType == Const::UNIDIRECTION_ARROW) {
                    inputOperator = new ExpandAll(inputOperator, newStartVar, newDestvar,
                                                  newRelVar, newRelType);
                } else {
                    auto direction = patternElements[right]->elements[0]->elements[0]->nodeType == Const::LEFT_ARRROW ?
                            "left" : "right";
                    inputOperator = new ExpandAll(inputOperator, newStartVar, newDestvar,
                                                  newRelVar, newRelType, direction);
                }

                if (prevRel != "null") {
                    auto* whereClause = prepareWhereClause(newRelVar, prevRel);
                    filterCases.push_back(pair<string, ASTNode*>("null", whereClause));
                }

                prevRel = newRelVar;
                startVar = newDestvar;

                if (analyzedRel.first[2]) {
                    filterCases.push_back(pair<string, ASTNode*>(newRelVar, analyzedRel.second[2]));
                }
                if (analyzedNode.first[1]) {
                    filterCases.push_back(pair<string, ASTNode*>(newDestvar, analyzedNode.second[1]));
                }
                if (analyzedNode.first[2]) {
                    filterCases.push_back(pair<string, ASTNode*>(newDestvar, analyzedNode.second[2]));
                }
                if (!filterCases.empty()) {
                    inputOperator = new Filter(inputOperator, filterCases);
                }
            }
            return inputOperator;
        } else if (index == patternElements.size()) {
            vector<pair<string, ASTNode*>> filterCases;
            string startVar = variable;
            string prevRel = "null";
            for (int left = index - 1; left >= 0; left--) {
                filterCases.clear();
                auto analyzedRel = getRelationshipDetails(patternElements[left]->elements[0]->elements[1]);
                pair<vector<bool>, vector<ASTNode*>> analyzedNode;
                if (left>0) {
                    analyzedNode = getNodeDetails(patternElements[left-1]->elements[1]);
                } else {
                    analyzedNode = getNodeDetails(startNode);
                }

                string newStartVar = startVar;
                string newDestvar = analyzedNode.first[0]? analyzedNode.second[0]->value :
                        "node_var_"+ to_string(left);
                string newRelVar = analyzedRel.first[0] ? analyzedRel.second[0]->value : "edge_var_"+to_string(left);
                string newRelType = analyzedRel.first[1] ? analyzedRel.second[1]->elements[0]->value : "null";
                if (patternElements[left]->elements[0]->elements[0]->nodeType == Const::UNIDIRECTION_ARROW) {
                    inputOperator = new ExpandAll(inputOperator, newStartVar, newDestvar,
                                                  newRelVar, newRelType);

                } else {
                    auto direction = patternElements[left]->elements[0]->elements[0]->nodeType ==
                            Const::LEFT_ARRROW ? "right" : "left";
                    inputOperator = new ExpandAll(inputOperator, newStartVar, newDestvar,
                                                  newRelVar, newRelType, direction);
                }

                if (prevRel != "null") {
                    auto *whereClause = prepareWhereClause(newRelVar, prevRel);
                    filterCases.push_back(pair<string, ASTNode *>("null", whereClause));
                }

                prevRel = newRelVar;
                startVar = newDestvar;

                if (analyzedRel.first[2]) {
                    filterCases.push_back(pair<string, ASTNode*>(newRelVar, analyzedRel.second[2]));
                }
                if (analyzedNode.first[1]) {
                    filterCases.push_back(pair<string, ASTNode*>(newDestvar, analyzedNode.second[1]));
                }
                if (analyzedNode.first[2]) {
                    filterCases.push_back(pair<string, ASTNode*>(newDestvar, analyzedNode.second[2]));
                }
                if (!filterCases.empty()) {
                    inputOperator = new Filter(inputOperator, filterCases);
                }
            }
            return inputOperator;
        } else {
            auto leftRel = getRelationshipDetails(patternElements[index-1]->elements[0]->elements[1]);
            auto rightRel = getRelationshipDetails(patternElements[index]->elements[0]->elements[1]);
            if (count(leftRel.first.begin(), leftRel.first.end(), true) >
                count(rightRel.first.begin(), rightRel.first.end(), true)) {
                vector<pair<string, ASTNode*>> filterCases;
                string startVar = variable;
                string prevRel = "null";
                string rel;
                for (int left = index - 1; left >= 0; left--) {
                    filterCases.clear();
                    auto analyzedRel = getRelationshipDetails(patternElements[left]->elements[0]->elements[1]);
                    pair<vector<bool>, vector<ASTNode*>> analyzedNode;
                    if (left>0) {
                        analyzedNode = getNodeDetails(patternElements[left-1]->elements[1]);
                    } else {
                        analyzedNode = getNodeDetails(startNode);
                    }

                    string newStartVar = startVar;
                    string newDestvar = analyzedNode.first[0]? analyzedNode.second[0]->value :
                            "node_var_"+ to_string(left);
                    string newRelVar = analyzedRel.first[0] ? analyzedRel.second[0]->value :
                            "edge_var_"+to_string(left);
                    string newRelType = analyzedRel.first[1] ? analyzedRel.second[1]->elements[0]->value : "null";
                    if (patternElements[left]->elements[0]->elements[0]->nodeType == Const::UNIDIRECTION_ARROW) {
                        inputOperator = new ExpandAll(inputOperator, newStartVar, newDestvar,
                                                      newRelVar, newRelType);

                    } else {
                        auto direction = patternElements[left]->elements[0]->elements[0]->nodeType ==
                                Const::LEFT_ARRROW ? "right" : "left";
                        inputOperator = new ExpandAll(inputOperator, newStartVar, newDestvar,
                                                      newRelVar, newRelType, direction);
                    }

                    if (prevRel != "null") {
                        auto *whereClause = prepareWhereClause(newRelVar, prevRel);
                        filterCases.push_back(pair<string, ASTNode *>("null", whereClause));
                    } else {
                        rel = newRelVar;
                    }

                    prevRel = newRelVar;
                    startVar = newDestvar;

                    if (analyzedRel.first[2]) {
                        filterCases.push_back(pair<string, ASTNode*>(newRelVar, analyzedRel.second[2]));
                    }
                    if (analyzedNode.first[1]) {
                        filterCases.push_back(pair<string, ASTNode*>(newDestvar, analyzedNode.second[1]));
                    }
                    if (analyzedNode.first[2]) {
                        filterCases.push_back(pair<string, ASTNode*>(newDestvar, analyzedNode.second[2]));
                    }
                    if (!filterCases.empty()) {
                        inputOperator = new Filter(inputOperator, filterCases);
                    }
                }

                prevRel = rel;
                startVar = variable;
                for (int right = index; right<patternElements.size(); right++) {
                    filterCases.clear();
                    auto analyzedRel = getRelationshipDetails(patternElements[right]->elements[0]->elements[1]);
                    auto analyzedNode = getNodeDetails(patternElements[right]->elements[1]);

                    string newStartVar = startVar;
                    string newDestvar = analyzedNode.first[0]? analyzedNode.second[0]->value :
                            "node_var_"+ to_string(right);
                    string newRelVar = analyzedRel.first[0] ? analyzedRel.second[0]->value :
                            "edge_var_"+to_string(right);
                    string newRelType = analyzedRel.first[1] ? analyzedRel.second[1]->elements[0]->value : "null";

                    if (patternElements[right]->elements[0]->elements[0]->nodeType == Const::UNIDIRECTION_ARROW) {
                        inputOperator = new ExpandAll(inputOperator, newStartVar, newDestvar,
                                                      newRelVar, newRelType);

                    } else {
                        auto direction = patternElements[right]->elements[0]->elements[0]->nodeType ==
                                Const::LEFT_ARRROW ? "left" : "right";
                        inputOperator = new ExpandAll(inputOperator, newStartVar, newDestvar,
                                                      newRelVar, newRelType, direction);
                    }

                    if (prevRel != "null") {
                        auto* whereClause = prepareWhereClause(newRelVar, prevRel);
                        filterCases.push_back(pair<string, ASTNode*>("null", whereClause));
                    }

                    prevRel = newRelVar;
                    startVar = newDestvar;

                    if (analyzedRel.first[2]) {
                        filterCases.push_back(pair<string, ASTNode*>(newRelVar, analyzedRel.second[2]));
                    }
                    if (analyzedNode.first[1]) {
                        filterCases.push_back(pair<string, ASTNode*>(newDestvar, analyzedNode.second[1]));
                    }
                    if (analyzedNode.first[2]) {
                        filterCases.push_back(pair<string, ASTNode*>(newDestvar, analyzedNode.second[2]));
                    }
                    if (!filterCases.empty()) {
                        inputOperator = new Filter(inputOperator, filterCases);
                    }
                }
            } else {
                vector<pair<string, ASTNode*>> filterCases;
                string startVar = variable;
                string prevRel = "null";
                string rel;
                startVar = variable;
                for (int right = index; right<patternElements.size(); right++) {
                    filterCases.clear();
                    auto analyzedRel = getRelationshipDetails(patternElements[right]->elements[0]->elements[1]);
                    auto analyzedNode = getNodeDetails(patternElements[right]->elements[1]);

                    string newStartVar = startVar;
                    string newDestvar = analyzedNode.first[0]? analyzedNode.second[0]->value :
                            "node_var_"+ to_string(right);
                    string newRelVar = analyzedRel.first[0] ? analyzedRel.second[0]->value :
                            "edge_var_"+to_string(right);
                    string newRelType = analyzedRel.first[1] ? analyzedRel.second[1]->elements[0]->value : "null";

                    if (patternElements[right]->elements[0]->elements[0]->nodeType == Const::UNIDIRECTION_ARROW) {
                        inputOperator = new ExpandAll(inputOperator, newStartVar, newDestvar,
                                                      newRelVar, newRelType);

                    } else {
                        auto direction = patternElements[right]->elements[0]->elements[0]->nodeType ==
                                Const::LEFT_ARRROW ? "left" : "right";
                        inputOperator = new ExpandAll(inputOperator, newStartVar, newDestvar,
                                                      newRelVar, newRelType, direction);
                    }

                    if (prevRel != "null") {
                        auto* whereClause = prepareWhereClause(newRelVar, prevRel);
                        filterCases.push_back(pair<string, ASTNode*>("null", whereClause));
                    } else {
                        rel = newRelVar;
                    }

                    prevRel = newRelVar;
                    startVar = newDestvar;

                    if (analyzedRel.first[2]) {
                        filterCases.push_back(pair<string, ASTNode*>(newRelVar, analyzedRel.second[2]));
                    }
                    if (analyzedNode.first[1]) {
                        filterCases.push_back(pair<string, ASTNode*>(newDestvar, analyzedNode.second[1]));
                    }
                    if (analyzedNode.first[2]) {
                        filterCases.push_back(pair<string, ASTNode*>(newDestvar, analyzedNode.second[2]));
                    }
                    if (!filterCases.empty()) {
                        inputOperator = new Filter(inputOperator, filterCases);
                    }
                }

                prevRel = rel;
                startVar = variable;
                for (int left = index - 1; left >= 0; left--) {
                    filterCases.clear();
                    auto analyzedRel = getRelationshipDetails(patternElements[left]->elements[0]->elements[1]);
                    pair<vector<bool>,vector<ASTNode*>> analyzedNode;
                    if (left>0) {
                        analyzedNode = getNodeDetails(patternElements[left-1]->elements[1]);
                    } else {
                        analyzedNode = getNodeDetails(startNode);
                    }

                    string newStartVar = startVar;
                    string newDestvar = analyzedNode.first[0]? analyzedNode.second[0]->value :
                            "node_var_"+ to_string(left);
                    string newRelVar = analyzedRel.first[0] ? analyzedRel.second[0]->value :
                            "edge_var_"+to_string(left);
                    string newRelType = analyzedRel.first[1] ? analyzedRel.second[1]->elements[0]->value : "null";
                    if (patternElements[left]->elements[0]->elements[0]->nodeType == Const::UNIDIRECTION_ARROW) {
                        inputOperator = new ExpandAll(inputOperator, newStartVar, newDestvar,
                                                      newRelVar, newRelType);

                    } else {
                        auto direction = patternElements[left]->elements[0]->elements[0]->nodeType ==
                                Const::LEFT_ARRROW ? "right" : "left";
                        inputOperator = new ExpandAll(inputOperator, newStartVar, newDestvar,
                                                      newRelVar, newRelType, direction);
                    }

                    if (prevRel != "null") {
                        auto *whereClause = prepareWhereClause(newRelVar, prevRel);
                        filterCases.push_back(pair<string, ASTNode *>("null", whereClause));
                    }

                    prevRel = newRelVar;
                    startVar = newDestvar;

                    if (analyzedRel.first[2]) {
                        filterCases.push_back(pair<string, ASTNode*>(newRelVar, analyzedRel.second[2]));
                    }
                    if (analyzedNode.first[1]) {
                        filterCases.push_back(pair<string, ASTNode*>(newDestvar, analyzedNode.second[1]));
                    }
                    if (analyzedNode.first[2]) {
                        filterCases.push_back(pair<string, ASTNode*>(newDestvar, analyzedNode.second[2]));
                    }
                    if (!filterCases.empty()) {
                        inputOperator = new Filter(inputOperator, filterCases);
                    }
                }
            }
            return inputOperator;
        }
    }

    for (int i = patternElements.size()-1; i >=0 ; i--) {
        auto* e = patternElements[i];
        auto analyzedDetails = getRelationshipDetails(e->elements[0]->elements[1]);
        auto  nodeDetail = getNodeDetails(e->elements[1]);

        if (analyzedDetails.first[1]) {
            isRelTypeExist = true;
            index = i;
            break;
        }

        if (nodeDetail.first[1] && !isNodeLabelExist) {
            isNodeLabelExist = true;
            labelIndex = i;
        }

        if (e->elements[0]->elements[0]->nodeType != Const::UNIDIRECTION_ARROW) {
            isDirectionExist = true;
            directionIndex = i;
        }
    }
    if (!isNodeLabelExist) {
        isNodeLabelExist = getNodeDetails(startNode).first[1];
    }

    if (isRelTypeExist) {
        auto* e = patternElements[index];
        auto analyzedDetails = getRelationshipDetails(e->elements[0]->elements[1]);
        ASTNode* sourceNodePattern = nullptr;
        ASTNode* destinationNodePattern = nullptr;
        if (index > 0) {
            sourceNodePattern = patternElements[index-1]->elements[1];
        } else {
            sourceNodePattern = startNode;
        }
        destinationNodePattern = patternElements[index]->elements[1];

        auto analyzedSource = getNodeDetails(sourceNodePattern);
        auto analyzedDest = getNodeDetails(destinationNodePattern);

        vector<pair<string, ASTNode*>> filterCases;
        string relVar = analyzedDetails.first[0] ? analyzedDetails.second[0]->value : "edge_var_"+to_string(index);
        string startVar = analyzedSource.first[0]? analyzedSource.second[0]->value : "node_var_"+ to_string(index);
        string destVar = analyzedDest.first[0]? analyzedDest.second[0]->value : "node_var_"+ to_string(index+1);

        if (e->elements[0]->elements[0]->nodeType == Const::UNIDIRECTION_ARROW) {
            if (analyzedDetails.first[0]) {
                inputOperator = new UndirectedRelationshipTypeScan(analyzedDetails.second[1]->elements[0]->value,
                                                                   relVar, startVar, destVar);

            } else {
                inputOperator = new UndirectedRelationshipTypeScan(analyzedDetails.second[0]->elements[0]->value,
                                                                   relVar, startVar, destVar);
            }

        } else {
            auto direction = e->elements[0]->elements[0]->nodeType == Const::LEFT_ARRROW ? "left" : "right";
            if (analyzedDetails.first[0]) {
                inputOperator = new DirectedRelationshipTypeScan(direction,
                                                                 analyzedDetails.second[1]->elements[0]->value,
                                                                 relVar, startVar, destVar);
            } else {
                inputOperator = new DirectedRelationshipTypeScan(direction,
                                                                 analyzedDetails.second[0]->elements[0]->value,
                                                                 relVar, startVar, destVar);
            }
        }

        if (analyzedDetails.first[2]) {
            filterCases.push_back(pair<string, ASTNode*>(relVar, analyzedDetails.second[2]));
        }
        if (analyzedSource.first[1]) {
            filterCases.push_back(pair<string, ASTNode*>(startVar, analyzedSource.second[1]));
        }
        if (analyzedSource.first[2]) {
            filterCases.push_back(pair<string, ASTNode*>(startVar, analyzedSource.second[2]));
        }
        if (analyzedDest.first[1]) {
            filterCases.push_back(pair<string, ASTNode*>(destVar, analyzedDest.second[1]));
        }
        if (analyzedDest.first[2]) {
            filterCases.push_back(pair<string, ASTNode*>(destVar, analyzedDest.second[2]));
        }
        if (!filterCases.empty()) {
            inputOperator = new Filter(inputOperator, filterCases);
        }

        auto prevRel = relVar;
        if (sourceNodePattern->elements.size() < destinationNodePattern->elements.size()) {
            for (int right = index+1; right < patternElements.size(); right++) {
                filterCases.clear();
                auto analyzedRel = getRelationshipDetails(patternElements[right]->elements[0]->elements[1]);
                auto analyzedNode = getNodeDetails(patternElements[right]->elements[1]);

                string newStartVar = destVar;
                string newDestvar = analyzedNode.first[0]? analyzedNode.second[0]->value :
                        "node_var_"+ to_string(right);
                string newRelVar = analyzedRel.first[0] ? analyzedRel.second[0]->value :
                        "edge_var_"+to_string(right);
                string newRelType = analyzedRel.first[1] ? analyzedRel.second[1]->elements[0]->value : "null";

                if (patternElements[right]->elements[0]->elements[0]->nodeType == Const::UNIDIRECTION_ARROW) {
                    inputOperator = new ExpandAll(inputOperator, newStartVar, newDestvar,
                                                  newRelVar, newRelType);

                } else {
                    auto direction = patternElements[right]->elements[0]->elements[0]->nodeType ==
                            Const::LEFT_ARRROW ? "left" : "right";
                    inputOperator = new ExpandAll(inputOperator, newStartVar, newDestvar,
                                                  newRelVar, newRelType, direction);
                }


                auto* whereClause = prepareWhereClause(newRelVar, prevRel);
                filterCases.push_back(pair<string, ASTNode*>("null", whereClause));

                prevRel = newRelVar;
                destVar = newDestvar;

                if (analyzedRel.first[2]) {
                    filterCases.push_back(pair<string, ASTNode*>(newRelVar, analyzedRel.second[2]));
                }
                if (analyzedNode.first[1]) {
                    filterCases.push_back(pair<string, ASTNode*>(newDestvar, analyzedNode.second[1]));
                }
                if (analyzedNode.first[2]) {
                    filterCases.push_back(pair<string, ASTNode*>(newDestvar, analyzedNode.second[2]));
                }
                if (!filterCases.empty()) {
                    inputOperator = new Filter(inputOperator, filterCases);
                }
            }

            prevRel = relVar;
            for (int left = index - 1; left >= 0; left--) {
                filterCases.clear();
                auto analyzedRel = getRelationshipDetails(patternElements[left]->elements[0]->elements[1]);
                pair<vector<bool>,vector<ASTNode*>> analyzedNode;
                if (left > 0) {
                    analyzedNode = getNodeDetails(patternElements[left-1]->elements[1]);
                } else {
                    analyzedNode = getNodeDetails(startNode);
                }

                string newStartVar = startVar;
                string newDestvar = analyzedNode.first[0]? analyzedNode.second[0]->value :
                        "node_var_"+ to_string(left);
                string newRelVar = analyzedRel.first[0] ? analyzedRel.second[0]->value :
                        "edge_var_"+to_string(left);
                string newRelType = analyzedRel.first[1] ? analyzedRel.second[1]->elements[0]->value : "null";
                if (patternElements[left]->elements[0]->elements[0]->nodeType == Const::UNIDIRECTION_ARROW) {
                    inputOperator = new ExpandAll(inputOperator, newStartVar, newDestvar,
                                                  newRelVar, newRelType);

                } else {
                    auto direction = patternElements[left]->elements[0]->elements[0]->nodeType ==
                            Const::LEFT_ARRROW ? "right" : "left";
                    inputOperator = new ExpandAll(inputOperator, newStartVar, newDestvar,
                                                  newRelVar, newRelType, direction);
                }

                auto* whereClause = prepareWhereClause(newRelVar,prevRel);
                filterCases.push_back(pair<string, ASTNode*>("null", whereClause));


                prevRel = newRelVar;
                startVar = newDestvar;

                if (analyzedRel.first[2]) {
                    filterCases.push_back(pair<string, ASTNode*>(newRelVar, analyzedRel.second[2]));
                }
                if (analyzedNode.first[1]) {
                    filterCases.push_back(pair<string, ASTNode*>(newDestvar, analyzedNode.second[1]));
                }
                if (analyzedNode.first[2]) {
                    filterCases.push_back(pair<string, ASTNode*>(newDestvar, analyzedNode.second[2]));
                }
                if (!filterCases.empty()) {
                    inputOperator = new Filter(inputOperator, filterCases);
                }
            }
        } else {
            prevRel = relVar;
            for (int left = index - 1; left >= 0; left--) {
                filterCases.clear();
                auto analyzedRel = getRelationshipDetails(patternElements[left]->elements[0]->elements[1]);
                pair<vector<bool>,vector<ASTNode*>> analyzedNode;
                if (left > 0) {
                    analyzedNode = getNodeDetails(patternElements[left-1]->elements[1]);
                } else {
                    analyzedNode = getNodeDetails(startNode);
                }

                string newStartVar = startVar;
                string newDestvar = analyzedNode.first[0]? analyzedNode.second[0]->value :
                        "node_var_"+ to_string(left);
                string newRelVar = analyzedRel.first[0] ? analyzedRel.second[0]->value :
                        "edge_var_"+to_string(left);
                string newRelType = analyzedRel.first[1] ? analyzedRel.second[1]->elements[0]->value : "null";
                if (patternElements[left]->elements[0]->elements[0]->nodeType == Const::UNIDIRECTION_ARROW) {
                    inputOperator = new ExpandAll(inputOperator, newStartVar, newDestvar,
                                                  newRelVar, newRelType);

                } else {
                    auto direction = patternElements[left]->elements[0]->elements[0]->nodeType ==
                            Const::LEFT_ARRROW ? "right" : "left";
                    inputOperator = new ExpandAll(inputOperator, newStartVar, newDestvar,
                                                  newRelVar, newRelType, direction);
                }


                auto* whereClause = prepareWhereClause(newRelVar,prevRel);
                filterCases.push_back(pair<string, ASTNode*>("null", whereClause));


                prevRel = newRelVar;
                startVar = newDestvar;
                if (analyzedRel.first[2]) {
                    filterCases.push_back(pair<string, ASTNode*>(newRelVar, analyzedRel.second[2]));
                }
                if (analyzedNode.first[1]) {
                    filterCases.push_back(pair<string, ASTNode*>(newDestvar, analyzedNode.second[1]));
                }
                if (analyzedNode.first[2]) {
                    filterCases.push_back(pair<string, ASTNode*>(newDestvar, analyzedNode.second[2]));
                }
                if (!filterCases.empty()) {
                    inputOperator = new Filter(inputOperator, filterCases);
                }
            }

            prevRel = relVar;
            for (int right = index+1; right < patternElements.size(); right++) {
                filterCases.clear();
                auto analyzedRel = getRelationshipDetails(patternElements[right]->elements[0]->elements[1]);
                auto analyzedNode = getNodeDetails(patternElements[right]->elements[1]);

                string newStartVar = destVar;
                string newDestvar = analyzedNode.first[0]? analyzedNode.second[0]->value :
                        "node_var_"+ to_string(right);
                string newRelVar = analyzedRel.first[0] ? analyzedRel.second[0]->value :
                        "edge_var_"+to_string(right);
                string newRelType = analyzedRel.first[1] ? analyzedRel.second[1]->elements[0]->value : "null";

                if (patternElements[right]->elements[0]->elements[0]->nodeType == Const::UNIDIRECTION_ARROW) {
                    inputOperator = new ExpandAll(inputOperator, newStartVar, newDestvar,
                                                  newRelVar, newRelType);

                } else {
                    auto direction = patternElements[right]->elements[0]->elements[0]->nodeType ==
                            Const::LEFT_ARRROW ? "left" : "right";
                    inputOperator = new ExpandAll(inputOperator, newStartVar, newDestvar,
                                                  newRelVar, newRelType, direction);
                }


                auto* whereClause = prepareWhereClause(newRelVar,prevRel);
                filterCases.push_back(pair<string, ASTNode*>("null", whereClause));


                prevRel = newRelVar;
                destVar = newDestvar;

                if (analyzedRel.first[2]) {
                    filterCases.push_back(pair<string, ASTNode*>(newRelVar, analyzedRel.second[2]));
                }
                if (analyzedNode.first[1]) {
                    filterCases.push_back(pair<string, ASTNode*>(newDestvar, analyzedNode.second[1]));
                }
                if (analyzedNode.first[2]) {
                    filterCases.push_back(pair<string, ASTNode*>(newDestvar, analyzedNode.second[2]));
                }
                if (!filterCases.empty()) {
                    inputOperator = new Filter(inputOperator, filterCases);
                }
            }
        }
    } else if (isNodeLabelExist) {
        auto* e = patternElements[labelIndex];
        ASTNode* sourceNodePattern = nullptr;
        ASTNode* destinationNodePattern = nullptr;
        if (labelIndex >= 0) {
            sourceNodePattern = patternElements[labelIndex]->elements[1];
        } else {
            sourceNodePattern = startNode;
        }
        auto analyzedSource = getNodeDetails(sourceNodePattern);

        vector<pair<string, ASTNode*>> filterCases;
        string startVar = analyzedSource.first[0]? analyzedSource.second[0]->value : "node_var_"+ to_string(index+1);

        inputOperator =  new NodeScanByLabel(analyzedSource.second[1]->elements[0]->value, startVar);

        if (analyzedSource.first[2]) {
            filterCases.push_back(pair<string, ASTNode*>(startVar, analyzedSource.second[2]));
        }

        if (!filterCases.empty()) {
            inputOperator = new Filter(inputOperator, filterCases);
        }

        string prevRel;

        for (int right = labelIndex+1; right < patternElements.size(); right++) {
            filterCases.clear();
            auto analyzedRel = getRelationshipDetails(patternElements[right]->elements[0]->elements[1]);
            auto analyzedNode = getNodeDetails(patternElements[right]->elements[1]);

            string newStartVar = startVar;
            string newDestvar = analyzedNode.first[0]? analyzedNode.second[0]->value : "node_var_"+ to_string(right);
            string newRelVar = analyzedRel.first[0] ? analyzedRel.second[0]->value : "edge_var_"+to_string(right);
            string newRelType = analyzedRel.first[1] ? analyzedRel.second[1]->elements[0]->value : "null";

            if (patternElements[right]->elements[0]->elements[0]->nodeType == Const::UNIDIRECTION_ARROW) {
                inputOperator = new ExpandAll(inputOperator, newStartVar, newDestvar,
                                              newRelVar, newRelType);
            } else {
                auto direction = patternElements[right]->elements[0]->elements[0]->nodeType ==
                        Const::LEFT_ARRROW ? "left" : "right";
                inputOperator = new ExpandAll(inputOperator, newStartVar, newDestvar,
                                              newRelVar, newRelType, direction);
            }

            if (right > labelIndex+1) {
                auto* whereClause = prepareWhereClause(newRelVar, prevRel);
                filterCases.push_back(pair<string, ASTNode*>("null", whereClause));
                prevRel = newRelVar;
                startVar = newDestvar;
            } else {
                prevRel = newRelVar;
                startVar = newDestvar;
            }

            if (analyzedRel.first[2]) {
                filterCases.push_back(pair<string, ASTNode*>(newRelVar, analyzedRel.second[2]));
            }
            if (analyzedNode.first[1]) {
                filterCases.push_back(pair<string, ASTNode*>(newDestvar, analyzedNode.second[1]));
            }
            if (analyzedNode.first[2]) {
                filterCases.push_back(pair<string, ASTNode*>(newDestvar, analyzedNode.second[2]));
            }
            if (!filterCases.empty()) {
                inputOperator = new Filter(inputOperator, filterCases);
            }
        }

        startVar = analyzedSource.first[0]? analyzedSource.second[0]->value : "node_var_"+ to_string(labelIndex);
        if (labelIndex < patternElements.size()-1) {
            prevRel = getRelationshipDetails(patternElements[labelIndex+1]->elements[0]->elements[1]).first[0] ?
                      getRelationshipDetails(
                              patternElements[labelIndex+1]->elements[0]->elements[1]).second[0]->value :
                              "edge_var_" + to_string(labelIndex+1);
        }
        for (int left = labelIndex; left >= 0; left--) {
            filterCases.clear();
            pair<vector<bool>, vector<ASTNode *>> analyzedNode;
            auto analyzedRel = getRelationshipDetails(patternElements[left]->elements[0]->elements[1]);
            if (left == 0) {
                analyzedNode = getNodeDetails(startNode);
            } else {
                analyzedNode = getNodeDetails(patternElements[left-1]->elements[1]);
            }

            string newStartVar = startVar;
            string newDestvar = analyzedNode.first[0]? analyzedNode.second[0]->value : "node_var_"+ to_string(left);
            string newRelVar = analyzedRel.first[0] ? analyzedRel.second[0]->value : "edge_var_"+to_string(left);
            string newRelType = analyzedRel.first[1] ? analyzedRel.second[1]->elements[0]->value : "null";

            if (patternElements[left]->elements[0]->elements[0]->nodeType == Const::UNIDIRECTION_ARROW) {
                inputOperator = new ExpandAll(inputOperator, newStartVar, newDestvar,
                                              newRelVar, newRelType);

            } else {
                auto direction = patternElements[left]->elements[0]->elements[0]->nodeType ==
                        Const::LEFT_ARRROW ? "right" : "left";
                inputOperator = new ExpandAll(inputOperator, newStartVar, newDestvar,
                                              newRelVar, newRelType, direction);
            }

            auto* whereClause = prepareWhereClause(newRelVar, prevRel);
            filterCases.push_back(pair<string, ASTNode*>("null", whereClause));
            prevRel = newRelVar;
            startVar = newDestvar;


            if (analyzedRel.first[2]) {
                filterCases.push_back(pair<string, ASTNode*>(newRelVar, analyzedRel.second[2]));
            }
            if (analyzedNode.first[1]) {
                filterCases.push_back(pair<string, ASTNode*>(newDestvar, analyzedNode.second[1]));
            }
            if (analyzedNode.first[2]) {
                filterCases.push_back(pair<string, ASTNode*>(newDestvar, analyzedNode.second[2]));
            }
            if (!filterCases.empty()) {
                inputOperator = new Filter(inputOperator, filterCases);
            }
        }
    } else if (isDirectionExist) {
        auto* e = patternElements[directionIndex];
        auto analyzedDetails = getRelationshipDetails(e->elements[0]->elements[1]);
        ASTNode* sourceNodePattern = nullptr;
        ASTNode* destinationNodePattern = nullptr;
        if (directionIndex > 0) {
            sourceNodePattern = patternElements[directionIndex-1]->elements[1];
            destinationNodePattern = patternElements[directionIndex]->elements[1];
        } else {
            sourceNodePattern = startNode;
            destinationNodePattern = patternElements[directionIndex]->elements[1];
        }

        auto analyzedSource = getNodeDetails(sourceNodePattern);
        auto analyzedDest = getNodeDetails(destinationNodePattern);

        vector<pair<string, ASTNode*>> filterCases;
        string relVar = analyzedDetails.first[0] ? analyzedDetails.second[0]->value :
                "edge_var_"+to_string(directionIndex);
        string startVar = analyzedSource.first[0]? analyzedSource.second[0]->value :
                "node_var_"+ to_string(directionIndex);
        string destVar = analyzedDest.first[0]? analyzedDest.second[0]->value :
                "node_var_"+ to_string(directionIndex+1);


        auto direction = e->elements[0]->elements[0]->nodeType == Const::LEFT_ARRROW ? "left" : "right";
        inputOperator = new DirectedAllRelationshipScan(direction, startVar, destVar, relVar);

        string prevRel = relVar;
        for (int left = directionIndex - 1; left >= 0; left--) {
            filterCases.clear();
            auto analyzedRel = getRelationshipDetails(patternElements[left]->elements[0]->elements[1]);
            pair<vector<bool>,vector<ASTNode*>> analyzedNode;
            if (left > 0) {
                analyzedNode = getNodeDetails(patternElements[left-1]->elements[1]);
            } else {
                analyzedNode = getNodeDetails(startNode);
            }

            string newStartVar = startVar;
            string newDestvar = analyzedNode.first[0]? analyzedNode.second[0]->value : "node_var_"+ to_string(left);
            string newRelVar = analyzedRel.first[0] ? analyzedRel.second[0]->value : "edge_var_"+to_string(left);
            string newRelType = analyzedRel.first[1] ? analyzedRel.second[1]->elements[0]->value : "null";
            if (patternElements[left]->elements[0]->elements[0]->nodeType == Const::UNIDIRECTION_ARROW) {
                inputOperator = new ExpandAll(inputOperator, newStartVar, newDestvar,
                                              newRelVar, newRelType);

            } else {
                auto direction = patternElements[left]->elements[0]->elements[0]->nodeType ==
                        Const::LEFT_ARRROW ? "right" : "left";
                inputOperator = new ExpandAll(inputOperator, newStartVar, newDestvar,
                                              newRelVar, newRelType, direction);
            }


            auto* whereClause = prepareWhereClause(newRelVar,prevRel);
            filterCases.push_back(pair<string, ASTNode*>("null", whereClause));
            inputOperator = new Filter(inputOperator, filterCases);
            prevRel = newRelVar;
            startVar = newDestvar;
        }

        prevRel = relVar;
        for (int right = directionIndex+1; right < patternElements.size(); right++) {
            filterCases.clear();
            auto analyzedRel = getRelationshipDetails(patternElements[right]->elements[0]->elements[1]);
            auto analyzedNode = getNodeDetails(patternElements[right]->elements[1]);

            string newStartVar = destVar;
            string newDestvar = analyzedNode.first[0]? analyzedNode.second[0]->value : "node_var_"+ to_string(right);
            string newRelVar = analyzedRel.first[0] ? analyzedRel.second[0]->value : "edge_var_"+to_string(right);
            string newRelType = analyzedRel.first[1] ? analyzedRel.second[1]->elements[0]->value : "null";

            if (patternElements[right]->elements[0]->elements[0]->nodeType == Const::UNIDIRECTION_ARROW) {
                inputOperator = new ExpandAll(inputOperator, newStartVar, newDestvar,
                                              newRelVar, newRelType);
            } else {
                auto direction = patternElements[right]->elements[0]->elements[0]->nodeType ==
                        Const::LEFT_ARRROW ? "left" : "right";
                inputOperator = new ExpandAll(inputOperator, newStartVar, newDestvar,
                                              newRelVar, newRelType, direction);
            }

            auto* whereClause = prepareWhereClause(newRelVar,prevRel);
            filterCases.push_back(pair<string, ASTNode*>("null", whereClause));
            inputOperator = new Filter(inputOperator, filterCases);

            prevRel = newRelVar;
            destVar = newDestvar;
        }
    } else {
        int endIndex = patternElements.size()-1;
        auto* e = patternElements[endIndex];
        auto analyzedDetails = getRelationshipDetails(e->elements[0]->elements[1]);
        ASTNode* sourceNodePattern = nullptr;
        ASTNode* destinationNodePattern = nullptr;

        if (endIndex > 0) {
            sourceNodePattern = patternElements[endIndex-1]->elements[1];
            destinationNodePattern = patternElements[endIndex]->elements[1];
        } else {
            sourceNodePattern = startNode;
            destinationNodePattern = patternElements[endIndex]->elements[1];
        }

        auto analyzedSource = getNodeDetails(sourceNodePattern);
        auto analyzedDest = getNodeDetails(destinationNodePattern);

        vector<pair<string, ASTNode*>> filterCases;
        string relVar = analyzedDetails.first[0] ? analyzedDetails.second[0]->value :
                "edge_var_"+to_string(endIndex);
        string startVar = analyzedSource.first[0]? analyzedSource.second[0]->value :
                "node_var_"+ to_string(endIndex);
        string destVar = analyzedDest.first[0]? analyzedDest.second[0]->value : "node_var_"+ to_string(endIndex+1);

        inputOperator = new UndirectedAllRelationshipScan(startVar, destVar, relVar);
        string prevRel = relVar;
        for (int left = endIndex-1;left >= 0;left--) {
            filterCases.clear();
            auto analyzedRel = getRelationshipDetails(patternElements[left]->elements[0]->elements[1]);
            pair<vector<bool>,vector<ASTNode*>> analyzedNode;
            if (left > 0) {
                analyzedNode = getNodeDetails(patternElements[left-1]->elements[1]);
            } else {
                analyzedNode = getNodeDetails(startNode);
            }

            string newStartVar = startVar;
            string newDestvar = analyzedNode.first[0]? analyzedNode.second[0]->value : "node_var_"+ to_string(left);
            string newRelVar = analyzedRel.first[0] ? analyzedRel.second[0]->value : "edge_var_"+to_string(left);
            string newRelType = analyzedRel.first[1] ? analyzedRel.second[1]->elements[0]->value : "null";
            inputOperator = new ExpandAll(inputOperator, newStartVar, newDestvar,
                                          newRelVar, newRelType);

            auto* whereClause = prepareWhereClause(newRelVar,prevRel);
            filterCases.push_back(pair<string, ASTNode*>("null", whereClause));
            inputOperator = new Filter(inputOperator, filterCases);
            prevRel = newRelVar;
            startVar = newDestvar;
        }
    }
    return inputOperator;
}
