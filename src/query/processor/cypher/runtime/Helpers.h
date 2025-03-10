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
    static bool evaluateLogical(string condition, string data);
    static bool evaluateNodes(string left, string right);
    static ValueType evaluateFunction(string function, string data, string type);
    static ValueType evaluatePropertyLookup(string property, string data, string type);
    static bool typeCheck(string left, string right);
    static ValueType evaluateOtherTypes(string data);
};



#endif //JASMINEGRAPH_HELPERS_H
