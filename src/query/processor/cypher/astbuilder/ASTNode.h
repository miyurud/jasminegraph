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

#include <vector>
#include <string>


#ifndef AST_NODE_H
#define AST_NODE_H

using namespace std;

class ASTNode {
    public:
        string nodeType;
        vector<ASTNode*> elements;
        string value;
        virtual ~ASTNode() = default;

        string print(int depth = 0, string prefix = "", bool isLast = true) const;

};




#endif
