/**
Copyright 2020 JasminGraph Team
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**/

#include "NodeBlock.h"

#include <iostream>
#include <sstream>
#include <vector>

#include "RelationBlock.h"

bool NodeBlock::isInUse() { return this->usage == '\1'; }

std::string NodeBlock::getLabel() { return std::string(this->label); }

void NodeBlock::save() {
    char _label[PropertyLink::MAX_VALUE_SIZE] = {0};
    std::strcpy(_label, id.c_str());

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
    bool isEmpty = this->properties.isEmpty();
    this->propRef = properties.insert(name, value);
    if (isEmpty) {  // If it was an empty prop link before inserting, Then update the property reference of this node
                    // block
        NodeBlock::nodesDB->seekp(this->addr + 1 + sizeof(this->edgeRef));
        NodeBlock::nodesDB->write(reinterpret_cast<char*>(&(this->propRef)), sizeof(this->propRef));
        NodeBlock::nodesDB->flush();
    }
}

bool NodeBlock::updateRelation(RelationBlock* relation, bool relocateHead) {
    unsigned int edgeReferenceAddress = relation->addr;
    unsigned int idd = this->addr;
    if (this->edgeRef == 0) {
        int edgeRefOffset = sizeof(this->usage);
        NodeBlock::nodesDB->seekp(this->addr + edgeRefOffset);
        if (!NodeBlock::nodesDB->write(reinterpret_cast<char*>(&(edgeReferenceAddress)), sizeof(unsigned int))) {
            std::cout << "ERROR: Error while updating edge reference address of " << edgeReferenceAddress
                      << " for node " << this->addr << std::endl;
        }
        NodeBlock::nodesDB->flush();  // Sync the file with in-memory stream
        this->edgeRef = edgeReferenceAddress;
    } else if (relocateHead) {
        std::cout << "Info: Relocating edge list header" << std::endl;
    } else {
        RelationBlock* currentRelation = RelationBlock::get(this->edgeRef);
        while (currentRelation != NULL) {
            if (currentRelation->source.address == this->addr) {
                if (currentRelation->source.nextRelationId == 0) {
                    return currentRelation->updateSourceNextRelAddr(edgeReferenceAddress);
                } else {
                    currentRelation = RelationBlock::get(currentRelation->source.nextRelationId);
                }
            } else if (!this->isDirected && currentRelation->destination.address == this->addr) {
                if (currentRelation->destination.nextRelationId == 0) {
                    return currentRelation->updateDestinationNextRelAddr(edgeReferenceAddress);
                } else {
                    currentRelation = RelationBlock::get(currentRelation->destination.nextRelationId);
                }
            } else {
                std::cout << "WARNING: Invalid relation block!!" << std::endl;
            }
        }
        return false;
    }
}

const unsigned long NodeBlock::BLOCK_SIZE = 15;
std::fstream* NodeBlock::nodesDB = NULL;