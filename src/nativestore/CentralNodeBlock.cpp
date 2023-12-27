//
// Created by sandaruwan on 5/3/23.
//

#include "CentralNodeBlock.h"
#include <vector>

#include "../util/logger/Logger.h"
#include "CentralRelationBlock.h"

Logger central_node_block_logger;
pthread_mutex_t lockSaveCentralNode;
pthread_mutex_t lockAddCentralNodeProperty;


CentralNodeBlock::CentralNodeBlock(std::string id, unsigned int nodeId, unsigned int address, unsigned int propRef, unsigned int edgeRef,unsigned int centralEdgeRef,
                     unsigned char edgeRefPID, char _label[], bool usage)
        : id(id),nodeId(nodeId), addr(address), propRef(propRef), edgeRef(edgeRef),centralEdgeRef(centralEdgeRef), edgeRefPID(edgeRefPID), usage(usage) {
    strcpy(label, _label);
};

bool CentralNodeBlock::isInUse() { return this->usage == '\1'; }

std::string CentralNodeBlock::getLabel() { return std::string(this->label); }

void CentralNodeBlock::save() {
    pthread_mutex_lock(&lockSaveCentralNode);
    char _label[CentralPropertyLink::MAX_VALUE_SIZE] = {0};
    std::strcpy(_label, id.c_str());

    bool isSmallLabel = id.length() <= sizeof(label)*2;
//    if (isSmallLabel) {
//        std::strcpy(this->label, this->id.c_str());
//    }
    CentralNodeBlock::centralNodesDB->seekp(this->addr);
    CentralNodeBlock::centralNodesDB->put(this->usage); // 1
    CentralNodeBlock::centralNodesDB->write(reinterpret_cast<char*>(&(this->nodeId)), sizeof(this->nodeId)); // 4
    CentralNodeBlock::centralNodesDB->write(reinterpret_cast<char*>(&(this->edgeRef)), sizeof(this->edgeRef)); // 4
    CentralNodeBlock::centralNodesDB->write(reinterpret_cast<char*>(&(this->centralEdgeRef)), sizeof(this->centralEdgeRef)); // 4
    CentralNodeBlock::centralNodesDB->put(this->edgeRefPID); // 1
    CentralNodeBlock::centralNodesDB->write(reinterpret_cast<char*>(&(this->propRef)), sizeof(this->propRef)); // 4
    CentralNodeBlock::centralNodesDB->write(this->label, sizeof(this->label)); // 6
    CentralNodeBlock::centralNodesDB->flush();  // Sync the file with in-memory stream
    pthread_mutex_unlock(&lockSaveCentralNode);

    if (!isSmallLabel) {
        this->addProperty("label", _label);
    }
}

void CentralNodeBlock::addProperty(std::string name, char* value) {

    if (this->propRef == 0) {
        CentralPropertyLink* newLink = CentralPropertyLink::create(name, value);
        pthread_mutex_lock(&lockAddCentralNodeProperty);
        if (newLink) {
            this->propRef = newLink->blockAddress;
            // If it was an empty prop link before inserting, Then update the property reference of this node
            // block
            central_node_block_logger.info("propRef = " + std::to_string(this->propRef));
            CentralNodeBlock::centralNodesDB->seekp(this->addr + sizeof(this->nodeId) + sizeof(this->edgeRef) + sizeof(this->centralEdgeRef) + sizeof(this->edgeRefPID));
            CentralNodeBlock::centralNodesDB->write(reinterpret_cast<char*>(&(this->propRef)), sizeof(this->propRef));
            CentralNodeBlock::centralNodesDB->flush();
        } else {
            throw "Error occurred while adding a new property link to " + std::to_string(this->addr) + " node block";
        }
        pthread_mutex_unlock(&lockAddCentralNodeProperty);
    } else {
        this->propRef = this->getPropertyHead()->insert(name, value);
    }
}

bool CentralNodeBlock::updateRelation(CentralRelationBlock* newRelation, bool relocateHead) {
    unsigned int edgeReferenceAddress = newRelation->addr;
    unsigned int thisAddress = this->addr;
    CentralRelationBlock* currentHead = this->getRelationHead();
    if (relocateHead) {  // Insert new relation link to the head of the link list
        if (currentHead) {
            if (thisAddress == currentHead->source.address) {
                currentHead->setPreviousSource(newRelation->addr);
            } else if (thisAddress == currentHead->destination.address) {
                currentHead->setPreviousDestination(newRelation->addr);
            } else {
                throw std::to_string(thisAddress) +
                      " relation head does not contain current node in its source or destination";
            }
            if (thisAddress == newRelation->source.address) {
                newRelation->setNextSource(currentHead->addr);
            } else if (thisAddress == newRelation->destination.address) {
                newRelation->setNextDestination(currentHead->addr);
            } else {
                throw std::to_string(thisAddress) +
                      " new relation does not contain current node in its source or destination";
            }
        }
        return this->setRelationHead(*newRelation);
    } else {
        CentralRelationBlock* currentRelation = currentHead;
        while (currentRelation != NULL) {
            if (currentRelation->source.address == this->addr) {
                if (currentRelation->source.nextRelationId == 0) {
                    return currentRelation->setNextSource(edgeReferenceAddress);
                } else {
                    currentRelation = currentRelation->nextSource();
                }
            } else if (!this->isDirected && currentRelation->destination.address == this->addr) {
                if (currentRelation->destination.nextRelationId == 0) {
                    return currentRelation->setNextDestination(edgeReferenceAddress);
                } else {
                    currentRelation = currentRelation->nextDestination();
                }
            } else {
                central_node_block_logger.warn("Invalid relation block" + std::to_string(currentRelation->addr));
            }
        }
        return false;
    }
    return false;
}



CentralRelationBlock* CentralNodeBlock::getRelationHead() {
    CentralRelationBlock* relationsHead = NULL;
    if (this->edgeRef != 0) {
        relationsHead = CentralRelationBlock::get(this->edgeRef);
    }
    return relationsHead;
};



bool CentralNodeBlock::setRelationHead(CentralRelationBlock newRelation) {
    unsigned int edgeReferenceAddress = newRelation.addr;
    int edgeReferenceOffset = sizeof(this->usage) + sizeof(this->nodeId);
    CentralNodeBlock::centralNodesDB->seekp(this->addr + edgeReferenceOffset);
    if (!CentralNodeBlock::centralNodesDB->write(reinterpret_cast<char*>(&(edgeReferenceAddress)), sizeof(unsigned int))) {
        central_node_block_logger.error("ERROR: Error while updating edge reference address of " +
                                std::to_string(edgeReferenceAddress) + " for node " + std::to_string(this->addr));
        return false;
    }
    CentralNodeBlock::centralNodesDB->flush();  // Sync the file with in-memory stream
    this->edgeRef = edgeReferenceAddress;
    return true;
}



/**
 * Return a pointer to matching relation block with the given node if found, Else return NULL
 * **/
CentralRelationBlock* CentralNodeBlock::searchRelation(CentralNodeBlock withNode) {
    CentralRelationBlock* found = NULL;
    CentralRelationBlock* currentRelation = this->getRelationHead();
    while (currentRelation) {
        if (currentRelation->source.address == this->addr) {
            if (currentRelation->destination.address == withNode.addr) {
                found = currentRelation;
                break;
            } else {
                currentRelation = currentRelation->nextSource();
            }
        } else if (!this->isDirected && (currentRelation->destination.address == this->addr)) {
            if (currentRelation->source.address == withNode.addr) {
                found = currentRelation;
                break;
            } else {
                currentRelation = currentRelation->nextDestination();
            }
        } else {
            throw "Exception: Unrelated relation block for " + std::to_string(this->addr) +
                  " found in relation block " + std::to_string(currentRelation->addr);
        }
    }

    return found;
}




std::list<CentralNodeBlock> CentralNodeBlock::getEdges() {
    std::list<CentralNodeBlock> edges;
    CentralRelationBlock* currentRelation = this->getRelationHead();
    while (currentRelation != NULL) {
        CentralNodeBlock* node = NULL;
        if (currentRelation->source.address == this->addr) {
            node = CentralNodeBlock::get(currentRelation->destination.address);
            currentRelation = currentRelation->nextSource();
        } else if (currentRelation->destination.address == this->addr) {
            node = CentralNodeBlock::get(currentRelation->source.address);
            currentRelation = currentRelation->nextDestination();
        } else {
            throw "Error: Unrecognized relation for " + std::to_string(this->addr) + " in relation block " +
                  std::to_string(currentRelation->addr);
        }
        if (!node) {
            throw "Error creating node in the relation";
        }
        edges.push_back(*node);
    }
    return edges;
}

std::map<std::string, char*> CentralNodeBlock::getAllProperties() {
    std::map<std::string, char*> allProperties;
    CentralPropertyLink* current = this->getPropertyHead();
    while (current) {
        allProperties.insert({current->name, current->value});
        CentralPropertyLink* temp = current->next();
        delete current;  // To prevent memory leaks
        current = temp;
    }
    delete current;
    return allProperties;
}

CentralNodeBlock* CentralNodeBlock::get(unsigned int blockAddress) {
    CentralNodeBlock* CentralNodeBlockPointer = NULL;
    CentralNodeBlock::centralNodesDB->seekg(blockAddress);
    unsigned int nodeId;
    unsigned int edgeRef;
    unsigned int centralEdgeRef;
    unsigned char edgeRefPID;
    unsigned int propRef;
    char usageBlock;
    char label[CentralNodeBlock::LABEL_SIZE];
    std::string id;

    if (!CentralNodeBlock::centralNodesDB->get(usageBlock)) {
        central_node_block_logger.error("Error while reading usage data from block " + std::to_string(blockAddress));
    }
    if (!CentralNodeBlock::centralNodesDB->read(reinterpret_cast<char*>(&nodeId), sizeof(unsigned int))) {
        central_node_block_logger.error("Error while reading nodeId  data from block " + std::to_string(blockAddress));
    }
    if (!CentralNodeBlock::centralNodesDB->read(reinterpret_cast<char*>(&edgeRef), sizeof(unsigned int))) {
        central_node_block_logger.error("Error while reading edge reference data from block " + std::to_string(blockAddress));
    }
    if (!CentralNodeBlock::centralNodesDB->read(reinterpret_cast<char*>(&centralEdgeRef), sizeof(unsigned int))) {
        central_node_block_logger.error("Error while reading central edge reference data from block " + std::to_string(blockAddress));
    }

    if (!CentralNodeBlock::centralNodesDB->read(reinterpret_cast<char*>(&edgeRefPID), sizeof(unsigned char))) {
        central_node_block_logger.error("Error while reading edge reference partition ID data from block " +
                                std::to_string(blockAddress));
    }

    if (!CentralNodeBlock::centralNodesDB->read(reinterpret_cast<char*>(&propRef), sizeof(unsigned int))) {
        central_node_block_logger.error("Error while reading prop reference data from block " + std::to_string(blockAddress));
    }

    if (!CentralNodeBlock::centralNodesDB->read(&label[0], CentralNodeBlock::LABEL_SIZE)) {
        central_node_block_logger.error("Error while reading label data from block " + std::to_string(blockAddress));
    }
    bool usage = usageBlock == '\1';
    central_node_block_logger.debug("Label = " + std::string(label));
    central_node_block_logger.debug("Label = " + std::string(label));
    central_node_block_logger.debug("Length of label = " + std::to_string(strlen(label)));
    central_node_block_logger.debug("edgeRef = " + std::to_string(edgeRef));
    if (strlen(label) != 0) {
        id = std::string(label);
    }
    CentralNodeBlockPointer = new CentralNodeBlock(id, nodeId,blockAddress, propRef, edgeRef,centralEdgeRef, edgeRefPID, label, usage);
    if (CentralNodeBlockPointer->id.length() == 0) {  // if label not found in node block look in the properties
        std::map<std::string, char*> props = CentralNodeBlockPointer->getAllProperties();
        if (props["label"]) {
            CentralNodeBlockPointer->id = props["label"];
        } else {
            throw "Could not find node ID/Label for node with block address = " +
                  std::to_string(CentralNodeBlockPointer->addr);
        }
    }
    central_node_block_logger.debug("Edge ref = " + std::to_string(CentralNodeBlockPointer->edgeRef));
    if (CentralNodeBlockPointer->edgeRef % CentralRelationBlock::BLOCK_SIZE != 0) {
        throw "Exception: Invalid edge reference address = " + CentralNodeBlockPointer->edgeRef;
    }
    return CentralNodeBlockPointer;
}

CentralPropertyLink* CentralNodeBlock::getPropertyHead() { return CentralPropertyLink::get(this->propRef); }
std::fstream* CentralNodeBlock::centralNodesDB = NULL;
