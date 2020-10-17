#include "RelationBlock.h"

#include <iostream>
#include <sstream>
#include <vector>

#include "NodeManager.h"

RelationBlock* RelationBlock::add(NodeBlock source, NodeBlock destination) {
    int RECORD_SIZE = sizeof(unsigned int);

    NodeRelation sourceData;
    NodeRelation destinationData;

    sourceData.address = source.addr;
    destinationData.address = destination.addr;

    unsigned int relationPropAddr = 0;

    long relationBlockAddress = RelationBlock::nextRelationIndex * RelationBlock::BLOCK_SIZE;
    RelationBlock::relationsDB->seekg(relationBlockAddress);

    RelationBlock::relationsDB->write(reinterpret_cast<char*>(&sourceData.address), RECORD_SIZE);
    if (!RelationBlock::relationsDB->write(reinterpret_cast<char*>(&destinationData.address), RECORD_SIZE)) {
        std::cout << "ERROR: Error while writing relation destAddr " << destinationData.address << " into relation block address "
                  << relationBlockAddress << std::endl;
        return NULL;
    }

    if (!RelationBlock::relationsDB->write(reinterpret_cast<char*>(&sourceData.nextRelationId), RECORD_SIZE)) {
        std::cout << "ERROR: Error while writing relation destAddr " << sourceData.nextRelationId
                  << " into relation block address " << relationBlockAddress << std::endl;
        return NULL;
    }

    if (!RelationBlock::relationsDB->write(reinterpret_cast<char*>(&sourceData.nextPid), RECORD_SIZE)) {
        std::cout << "ERROR: Error while writing relation destAddr " << sourceData.nextPid << " into relation block address "
                  << relationBlockAddress << std::endl;
        return NULL;
    }

    if (!RelationBlock::relationsDB->write(reinterpret_cast<char*>(&sourceData.preRelationId), RECORD_SIZE)) {
        std::cout << "ERROR: Error while writing relation destAddr " << sourceData.preRelationId
                  << " into relation block address " << relationBlockAddress << std::endl;
        return NULL;
    }

    if (!RelationBlock::relationsDB->write(reinterpret_cast<char*>(&destinationData.nextRelationId), RECORD_SIZE)) {
        std::cout << "ERROR: Error while writing relation destAddr " << destinationData.nextRelationId << " into relation block address "
                  << relationBlockAddress << std::endl;
        return NULL;
    }

    if (!RelationBlock::relationsDB->write(reinterpret_cast<char*>(&destinationData.nextPid), RECORD_SIZE)) {
        std::cout << "ERROR: Error while writing relation destAddr " << destinationData.nextPid << " into relation block address "
                  << relationBlockAddress << std::endl;
        return NULL;
    }

    if (!RelationBlock::relationsDB->write(reinterpret_cast<char*>(&destinationData.preRelationId), RECORD_SIZE)) {
        std::cout << "ERROR: Error while writing relation destAddr " << destinationData.preRelationId << " into relation block address "
                  << relationBlockAddress << std::endl;
        return NULL;
    }

    if (!RelationBlock::relationsDB->write(reinterpret_cast<char*>(&destinationData.prePid), RECORD_SIZE)) {
        std::cout << "ERROR: Error while writing relation destAddr " << destinationData.prePid << " into relation block address "
                  << relationBlockAddress << std::endl;
        return NULL;
    }

    if (!RelationBlock::relationsDB->write(reinterpret_cast<char*>(&relationPropAddr), RECORD_SIZE)) {
        std::cout << "ERROR: Error while writing relation destAddr " << relationPropAddr
                  << " into relation block address " << relationBlockAddress << std::endl;
        return NULL;
    }

    RelationBlock::nextRelationIndex += 1;
    RelationBlock::relationsDB->flush();
    source.updateEdgeRef(relationBlockAddress);
    destination.updateEdgeRef(relationBlockAddress);
    return new RelationBlock(sourceData, destinationData, relationPropAddr);
}

bool RelationBlock::isInUse() { return this->usage == '\1'; }
unsigned int RelationBlock::nextRelationIndex = 1;  // Starting with 1 because of the 0 and '\0' differentiation issue

const unsigned long RelationBlock::BLOCK_SIZE = 11 * 8;
std::string RelationBlock::DB_PATH = "streamStore/relations.db";
std::fstream* RelationBlock::relationsDB = NULL;