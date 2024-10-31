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
