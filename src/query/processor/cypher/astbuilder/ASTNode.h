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
