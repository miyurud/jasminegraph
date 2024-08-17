#include "ASTNode.h"

#ifndef ASTINTERNALNODE_H
#define ASTINTERNALNODE_H

using namespace std;

class ASTInternalNode : public ASTNode{

    public:
        ASTInternalNode(string nodeType)
        {
            this->nodeType = nodeType;
        }

        void addElements(ASTNode* element) ;
};

#endif //ASTINTERNALNODE_H
