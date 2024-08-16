//
// Created by thamindu on 7/25/24.
//
#include "ASTStructure.h"

#ifndef ASTINTERNALNODE_H
#define ASTINTERNALNODE_H

using namespace std;

class ASTInternalNode : public ASTNode{

    public:

        ASTInternalNode(string nodeType)
        {
            this->nodeType = nodeType;
        }

        void addElements(ASTNode* element) {
            elements.push_back(element);
        }


};



#endif //ASTINTERNALNODE_H
