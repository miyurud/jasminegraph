//
// Created by kumarawansha on 2/8/25.
//

#ifndef JASMINEGRAPH_HELPERS_H
#define JASMINEGRAPH_HELPERS_H
#include <string>
#include <iostream>
#include <vector>
using namespace std;
#include <nlohmann/json.hpp>
using json = nlohmann::json;

class FilterHelper {
 public:
    FilterHelper (string condition);
    bool evaluate(string data);
 private:
    string condition;
    static bool evaluateCondition(string condition, string data);
    static bool evaluateComparison(string condition, string raw);
    static bool evaluateLogical(string condition, string data);
};



#endif //JASMINEGRAPH_HELPERS_H
