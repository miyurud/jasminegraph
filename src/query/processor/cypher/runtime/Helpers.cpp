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

#include "Helpers.h"


FilterHelper::FilterHelper(string condition) : condition(condition) {};

bool FilterHelper::evaluate(std::string data) {
    return evaluateCondition(condition, data);
}

bool FilterHelper::evaluateCondition(std::string condition, std::string data) {
    json predicate = json::parse(condition);
    std::string type = predicate["type"];

    if (type == "COMPARISON") {
        return evaluateComparison(condition, data);
    } else if (type == "AND" || type == "OR" || type == "XOR" || type == "NOT") {
        return evaluateLogical(condition, data);
    }

    return false;
}

bool FilterHelper::evaluateComparison(std::string condition, std::string raw){
    json predicate = json::parse(condition);
    json data = json::parse(raw);
    if (!typeCheck(predicate["left"]["type"], predicate["right"]["type"])) {
        return false;
    }

    ValueType leftValue;
    ValueType rightValue;

    if (predicate["left"]["type"] == "VARIABLE" && predicate["right"]["type"] == "VARIABLE") {
        string left = predicate["left"]["value"];
        string right = predicate["right"]["value"];
        return evaluateNodes(data[left].dump(),
                             data[right].dump());
    }

    if (predicate["left"]["type"] == "PROPERTY_LOOKUP") {
        std::string variable = predicate["left"]["variable"];
        leftValue = evaluatePropertyLookup(predicate["left"].dump(),
                                           data[variable].dump(), predicate["right"]["type"]);
    } else if (predicate["left"]["type"] == Const::FUNCTION) {
        leftValue = evaluateFunction(predicate["left"].dump(),
                                     data.dump(), predicate["right"]["type"]);
    } else {
        // only evaluating string, decimal, boolean, null for now
        leftValue = evaluateOtherTypes(predicate["left"].dump());

    }

    if (predicate["right"]["type"] == "PROPERTY_LOOKUP") {
        std::string variable = predicate["right"]["variable"];
        rightValue = evaluatePropertyLookup(predicate["right"].dump(),
                                            data[variable].dump(), predicate["left"]["type"]);

    } else if (predicate["right"]["type"] == Const::FUNCTION) {
        rightValue = evaluateFunction(predicate["right"].dump(),
                                      data.dump(), predicate["left"]["type"]);
    } else {
        // only evaluating string, decimal, boolean, null for now
        rightValue = evaluateOtherTypes(predicate["right"].dump());

    }

    string op = predicate["operator"];

    return std::visit([&op](auto&& lhs, auto&& rhs) -> bool {
        using LType = std::decay_t<decltype(lhs)>;
        using RType = std::decay_t<decltype(rhs)>;

        if constexpr (std::is_same_v<LType, RType>) {
            if (op == "==") return lhs == rhs;
            if (op == "<>") return lhs != rhs;
            if constexpr (std::is_arithmetic_v<LType>) {
                if (op == "<") return lhs < rhs;
                if (op == ">") return lhs > rhs;
                if (op == "<=") return lhs <= rhs;
                if (op == ">=") return lhs >= rhs;
            }
        }

        return false; // Default if types are incompatible
    }, leftValue, rightValue);
    return false;
}

bool FilterHelper::evaluateLogical(std::string condition, std::string data) {
    json predicate = json::parse(condition);
    std::string type = predicate["type"];
    vector<json> comparisons = predicate["comparisons"];

    if (type == "AND") {
        for (auto comp : comparisons) {
            if (!evaluateCondition(comp.dump(), data)) {
                return false;
            }
        }
        return true;
    } else if (type == "OR") {
        for (auto comp : comparisons) {
            if (evaluateCondition(comp.dump(), data)) {
                return true;
            }
        }
        return false;
    } else if (type == "XOR") {
        if (comparisons.size() < 2) {
            return false;
        }
        bool result = evaluateCondition(comparisons[0].dump(),
                                        data) ^ evaluateCondition(comparisons[1].dump(), data);

        for (size_t i = 2; i < comparisons.size(); ++i) {
            result = result ^ evaluateCondition(comparisons[i].dump(), data);
        }
        return result;
    } else if (type == "NOT") {
        return evaluateCondition(comparisons[0].dump(), data);
    }

    return false;
}

bool FilterHelper::evaluateNodes(std::string left, std::string right) {
    json leftNode = json::parse(left);
    json rightNode = json::parse(right);
    if (leftNode["id"] != rightNode["id"]) {
        return true;
    }
    return false;
}

bool FilterHelper::typeCheck(std::string left, std::string right) {
    if (left == Const::PROPERTY_LOOKUP
        || right == Const::PROPERTY_LOOKUP
        || left == Const::FUNCTION
        || right == Const::FUNCTION) {
        return true;
    } else if (left == right) {
        return true;
    } else {
        return false;
    }
}

ValueType FilterHelper::evaluatePropertyLookup(std::string property, std::string data, string type) {
    json prop = json::parse(property);
    json raw = json::parse(data);
    vector<string> properties = prop["property"];
    string value;
    // should be implemented to lookup nested properties after persisting that kind of properties
    // for now only one level of properties are supported (size of properties vector should be 1)
    for (auto p: properties) {
        if (!raw.contains(p)) {
            value = "null";
        } else {
            value = raw[p];
        }
    }

    try {
        if (type == "STRING") {
            return value;
        } else if (type == "DECIMAL") {
            size_t pos;
            int num = stoi(value, &pos);
            if (pos != value.size()) throw invalid_argument("Invalid number format");
            return num;
        } else if (type == "BOOLEAN") {
            string lowerValue;
            std::transform(value.begin(), value.end(), back_inserter(lowerValue), ::tolower);
            if (lowerValue == "true") return true;
            if (lowerValue == "false") return false;
            throw invalid_argument("Invalid boolean format");
        } else if (type == "NULL") {
            return "null";
        } else if ("PROPERTY_LOOKUP") {
            return value;
        }
    } catch (const exception& e) {
        return "null";
    }
    return "null";
}

ValueType FilterHelper::evaluateFunction(std::string function, std::string data, std::string type) {
    json func = json::parse(function);
    json raw = json::parse(data);
    vector<string> args = func["arguments"];
    string arg = args[0];
    string value;

    if (func["functionName"] == "id") {
        value = raw[arg]["id"];
    }

    try {
        if (type == "STRING") {
            return value;
        } else if (type == "DECIMAL") {
            size_t pos;
            int num = stoi(value, &pos);
            if (pos != value.size()) throw invalid_argument("Invalid number format");
            return num;
        } else if (type == "BOOLEAN") {
            string lowerValue;
            std::transform(value.begin(), value.end(), back_inserter(lowerValue), ::tolower);
            if (lowerValue == "true") return true;
            if (lowerValue == "false") return false;
            throw invalid_argument("Invalid boolean format");
        } else if (type == "NULL") {
            return "null";
        } else if ("PROPERTY_LOOKUP") {
            return value;
        }
    } catch (const exception& e) {
        return "null";
    }
}

ValueType FilterHelper::evaluateOtherTypes(std::string data) {
    json val = json::parse(data);
    if (val["type"] == "STRING") {
        string str = val["value"];
        if (str.size() >= 2 && str.front() == '\'' && str.back() == '\'') {
            return str.substr(1, str.size() - 2); // Remove first and last character ' '
        }
        return val["value"];
    } else if (val["type"] == "DECIMAL") {
        return stoi(val["value"].get<std::string>());
    } else if (val["type"] == "BOOLEAN") {
        return val["value"] == "TRUE";
    } else if (val["type"] == "NULL") {
        return "";
    }
    return "";
}
