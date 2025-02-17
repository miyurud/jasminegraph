//
// Created by kumarawansha on 2/8/25.
//

#include "Helpers.h"


FilterHelper::FilterHelper(string condition) : condition(condition) {};

//              "condition": {
//                  "left": {
//                      "Type": "PROPERTY_LOOKUP",
//                      "property": [
//                          "name"
//                      ],
//                      "variable": "n"
//                  },
//                  "operator": "==",
//                  "right": {
//                      "Type": "STRING",
//                      "value": "'John'"
//                  },
//                  "type": "COMPARISON"
//              }


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

bool FilterHelper::evaluateComparison(std::string condition, std::string raw) {
    json predicate = json::parse(condition);
    json data = json::parse(raw);

    std::string property = predicate["left"]["property"][0];

    std::string variable = predicate["left"]["variable"];
    std::string op = predicate["operator"];
    std::string rightValue = predicate["right"]["value"];
    rightValue = rightValue.substr(1, rightValue.size() - 2);

    if (!data.contains(variable) || !data[variable].contains(property)) {
        return false;
    }

    std::string leftValue = data[variable][property];
    cout<<leftValue<<" --- "<<rightValue<<endl;
    if (op == "==") {
        return leftValue == rightValue;
    }
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
            cout<<comp.dump()<<endl;
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