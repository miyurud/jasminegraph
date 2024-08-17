#include "ASTNode.h"
#ifndef ASTLEAFVALUE_H
#define ASTLEAFVALUE_H

class ASTLeafValue : public ASTNode {
public:

    ASTLeafValue(const std::string& name, const std::string& value)
    {
        this->nodeType = name;
        this->value = value;
    };

};

#endif //ASTLEAFVALUE_H
