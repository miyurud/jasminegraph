#include "ASTNode.h"
#include <iostream>
#include <string>
#include <sstream>


using namespace std;

string ASTNode::print(int depth, string prefix, bool isLast) const
{
    stringstream ss;

    ss << prefix;
    ss << (isLast ? "└───" : "├──");
    ss << nodeType << ": " << value << "\n";

    string result = ss.str();
    prefix += (isLast ? "    " : "│   ");

    for (size_t i = 0; i < elements.size(); ++i) {
        result += elements[i]->print(depth + 1, prefix, i == elements.size() - 1);
    }

    return result;
}
