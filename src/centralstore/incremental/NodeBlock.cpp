#include "NodeBlock.h"

#include <iostream>
#include <sstream>
#include <vector>

bool NodeBlock::isInUse() { return this->usage == '\1'; }

std::string NodeBlock::getLabel() { return std::string(this->label); }

void NodeBlock::save() {
    char _label[PropertyLink::MAX_VALUE_SIZE] = {0};
    std::strcpy(_label, id.c_str());

    bool brk = this->id == "1106112";

    bool isSmallLabel = id.length() <= sizeof(label);
    if (isSmallLabel) {
        std::strcpy(this->label, this->id.c_str());
    }
    NodeBlock::nodesDB->seekp(this->addr);
    NodeBlock::nodesDB->put(this->usage);
    NodeBlock::nodesDB->write(reinterpret_cast<char*>(&(this->edgeRef)), sizeof(this->edgeRef));
    NodeBlock::nodesDB->write(reinterpret_cast<char*>(&(this->propRef)), sizeof(this->propRef));
    NodeBlock::nodesDB->write(this->label, sizeof(this->label));
    NodeBlock::nodesDB->flush();  // Sync the file with in-memory stream
    if (!isSmallLabel) {
        this->addProperty("label", _label);
    }
}

void NodeBlock::addProperty(std::string name, char* value) {
    bool brk = this->id == "1106112";
    bool isEmpty = this->properties.isEmpty();
    this->propRef = properties.insert(name, value);
    if (isEmpty) {  // If it was an empty prop link before inserting, Then update the property reference of this node block
        NodeBlock::nodesDB->seekp(this->addr + 1 + sizeof(this->edgeRef));
        NodeBlock::nodesDB->write(reinterpret_cast<char*>(&(this->propRef)), sizeof(this->propRef));
        NodeBlock::nodesDB->flush();
    }
}

const unsigned long NodeBlock::BLOCK_SIZE = 15;
std::fstream* NodeBlock::nodesDB = NULL;