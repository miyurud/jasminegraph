/**
Copyright 2025 JasmineGraph Team
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
#include "../../../../nativestore/NodeManager.h"
#include "../../../../nativestore/DataPublisher.h"
#include "../../../../nativestore/MetaPropertyEdgeLink.h"
#include "../../../../nativestore/RelationBlock.h"
#include "../../../../partitioner/stream/Partitioner.h"
#include "../../../../util/Utils.h"

using namespace std;
#include <nlohmann/json.hpp>
using json = nlohmann::json;
using ValueType = std::variant<std::string, int, bool>;

class FilterHelper {
 public:
    FilterHelper(string condition);
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

class CreateHelper {
 public:
    CreateHelper(vector<json> elements, std::string partitionAlgo, GraphConfig gc, string masterIP);
    void insertFromData(string data, SharedBuffer &buffer);
    void insertWithoutData(SharedBuffer &buffer);

 private:
    GraphConfig gc;
    vector<json> elements;
    Partitioner* graphPartitioner;
    string masterIP;
};

#endif //JASMINEGRAPH_HELPERS_H
