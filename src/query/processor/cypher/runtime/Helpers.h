//
// Created by kumarawansha on 2/8/25.
//

#ifndef JASMINEGRAPH_HELPERS_H
#define JASMINEGRAPH_HELPERS_H
#include <string>
#include <algorithm>
#include <variant>
#include <iostream>
#include <vector>
#include "./../util/Const.h"
#include "antlr4-runtime.h"
#include "/home/ubuntu/software/antlr/CypherLexer.h"
#include "/home/ubuntu/software/antlr/CypherParser.h"
#include "../astbuilder/ASTBuilder.h"
#include "../astbuilder/ASTNode.h"
#include "../semanticanalyzer/SemanticAnalyzer.h"
#include "../queryplanner/Operators.h"
#include "../queryplanner/QueryPlanner.h"
using namespace std;
#include <nlohmann/json.hpp>
using json = nlohmann::json;
using ValueType = std::variant<std::string, int, bool>;

class FilterHelper {
 public:
    FilterHelper (string condition);
    bool evaluate(string data);
 private:
    string condition;
    static bool evaluateCondition(string condition, string data);
    static bool evaluateComparison(string condition, string raw);
    static bool evaluatePredicateExpression(string condition, string raw);
    static bool evaluateLogical(string condition, string data);
    static bool evaluateNodes(string left, string right);
    static ValueType evaluateFunction(string function, string data, string type);
    static ValueType evaluatePropertyLookup(string property, string data, string type);
    static bool typeCheck(string left, string right);
    static ValueType evaluateOtherTypes(string data);
};

class ExpandAllHelper {
 public:
    static string generateSubQueryPlan(string query);
    static string generateSubQuery(string startVar, string destVar, string relVar, string id, string relType = "");
};

class AverageAggregationHelper {
 public:
    AverageAggregationHelper(string variable, string property): variable(variable), property(property){};
    void insertData(string data);
    string getFinalResult();
 private:
    string variable;
    string property;
    int numberOfData = 0;
    float  localAverage = 0.0f;
};



#endif //JASMINEGRAPH_HELPERS_H
