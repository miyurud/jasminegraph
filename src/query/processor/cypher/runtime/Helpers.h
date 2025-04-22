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
