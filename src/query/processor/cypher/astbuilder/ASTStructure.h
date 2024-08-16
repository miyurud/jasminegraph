//
// Created by thamindu on 7/24/24.
//
#include <iostream>
#include <vector>
#include <string>
#include <sstream>
#ifndef AST_STRUCTURE_H
#define AST_STRUCTURE_H

using namespace std;

class ASTNode {
    public:
        string nodeType;
        vector<ASTNode*> elements;
        string value;
        virtual ~ASTNode() = default;

    string print(int depth = 0, string prefix = "", bool isLast = true) const {
        stringstream ss;

        ss << prefix;
        ss << (isLast ? "└───" : "├──");
        ss << nodeType << ": " << value << endl;

        string result = ss.str();
        prefix += isLast ? "    " : "│   ";

        for (size_t i = 0; i < elements.size(); ++i) {
            result += elements[i]->print(depth + 1, prefix, i == elements.size() - 1);
        }

        return result;
    }

};




#endif //AST_STRUCTURE_H
