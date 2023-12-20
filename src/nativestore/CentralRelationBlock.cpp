/**
Copyright 2023 JasminGraph Team
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

#include "CentralRelationBlock.h"
#include <sstream>
#include <vector>

#include "../util/logger/Logger.h"
#include "NodeManager.h"

Logger central_relation_block_logger;
pthread_mutex_t lockAddCentralProperty;

CentralRelationBlock* CentralRelationBlock::add(CentralNodeBlock source, CentralNodeBlock destination) {
    int RECORD_SIZE = sizeof(unsigned int);

    NodeRelation sourceData;
    NodeRelation destinationData;

    sourceData.address = source.addr;
    destinationData.address = destination.addr;

//    unsigned int relationPropAddr = this;

    long CentralRelationBlockAddress = CentralRelationBlock::nextRelationIndex * CentralRelationBlock::BLOCK_SIZE;  // Block size is 4 * 13

    CentralRelationBlock::centralRelationsDB->seekg(CentralRelationBlockAddress);
    if (!CentralRelationBlock::centralRelationsDB->write(reinterpret_cast<char*>(&source.nodeId), RECORD_SIZE)) {
        central_relation_block_logger.error("ERROR: Error while writing  sourceAddr " +
                                    std::to_string(source.nodeId) + " into relation block address " +
                                    std::to_string(CentralRelationBlockAddress));
        return NULL;
    }
    if (!CentralRelationBlock::centralRelationsDB->write(reinterpret_cast<char*>(&destination.nodeId), RECORD_SIZE)) {
        central_relation_block_logger.error("ERROR: Error while writing  destinationAddr " +
                                    std::to_string(destination.nodeId) + " into relation block address " +
                                    std::to_string(CentralRelationBlockAddress));
        return NULL;
    }
    if (!CentralRelationBlock::centralRelationsDB->write(reinterpret_cast<char*>(&sourceData.address), RECORD_SIZE)) {
        central_relation_block_logger.error("ERROR: Error while writing relation destAddr " +
                                    std::to_string(sourceData.address) + " into relation block address " +
                                    std::to_string(CentralRelationBlockAddress));
        return NULL;
    }

    if (!CentralRelationBlock::centralRelationsDB->write(reinterpret_cast<char*>(&destinationData.address), RECORD_SIZE)) {
        central_relation_block_logger.error("ERROR: Error while writing relation destAddr " +
                                    std::to_string(destinationData.address) + " into relation block address " +
                                    std::to_string(CentralRelationBlockAddress));
        return NULL;
    }

    if (!CentralRelationBlock::centralRelationsDB->write(reinterpret_cast<char*>(&sourceData.nextRelationId), RECORD_SIZE)) {
        central_relation_block_logger.error("ERROR: Error while writing source next relation address " +
                                    std::to_string(sourceData.nextRelationId) + " into relation block address " +
                                    std::to_string(CentralRelationBlockAddress));
        return NULL;
    }

    if (!CentralRelationBlock::centralRelationsDB->write(reinterpret_cast<char*>(&sourceData.nextPid), RECORD_SIZE)) {
        central_relation_block_logger.error("ERROR: Error while writing source next relation partition ID " +
                                    std::to_string(sourceData.nextPid) + " into relation block address " +
                                    std::to_string(CentralRelationBlockAddress));
        return NULL;
    }

    if (!CentralRelationBlock::centralRelationsDB->write(reinterpret_cast<char*>(&sourceData.preRelationId), RECORD_SIZE)) {
        central_relation_block_logger.error("ERROR: Error while writing source previous relation address " +
                                    std::to_string(sourceData.preRelationId) + " into relation block address " +
                                    std::to_string(CentralRelationBlockAddress));
        return NULL;
    }

    if (!CentralRelationBlock::centralRelationsDB->write(reinterpret_cast<char*>(&sourceData.prePid), RECORD_SIZE)) {
        central_relation_block_logger.error("ERROR: Error while writing source previous relation partition ID " +
                                    std::to_string(sourceData.prePid) + " into relation block address " +
                                    std::to_string(CentralRelationBlockAddress));
        return NULL;
    }

    if (!CentralRelationBlock::centralRelationsDB->write(reinterpret_cast<char*>(&destinationData.nextRelationId), RECORD_SIZE)) {
        central_relation_block_logger.error("ERROR: Error while writing destination next relation address " +
                                    std::to_string(destinationData.nextRelationId) + " into relation block address " +
                                    std::to_string(CentralRelationBlockAddress));
        return NULL;
    }

    if (!CentralRelationBlock::centralRelationsDB->write(reinterpret_cast<char*>(&destinationData.nextPid), RECORD_SIZE)) {
        central_relation_block_logger.error("ERROR: Error while writing destination next partition id " +
                                    std::to_string(destinationData.nextPid) + " into relation block address " +
                                    std::to_string(CentralRelationBlockAddress));
        return NULL;
    }

    if (!CentralRelationBlock::centralRelationsDB->write(reinterpret_cast<char*>(&destinationData.preRelationId), RECORD_SIZE)) {
        central_relation_block_logger.error("ERROR: Error while writing destination previous relation address " +
                                    std::to_string(destinationData.preRelationId) + " into relation block address " +
                                    std::to_string(CentralRelationBlockAddress));
        return NULL;
    }

    if (!CentralRelationBlock::centralRelationsDB->write(reinterpret_cast<char*>(&destinationData.prePid), RECORD_SIZE)) {
        central_relation_block_logger.error("ERROR: Error while writing destination previous relation partition id " +
                                    std::to_string(destinationData.prePid) + " into relation block address " +
                                    std::to_string(CentralRelationBlockAddress));
        return NULL;
    }

    if (!CentralRelationBlock::centralRelationsDB->write(reinterpret_cast<char*>(&(this->propertyAddress)), RECORD_SIZE)) {
        central_relation_block_logger.error("ERROR: Error while writing relation property address " +
                                    std::to_string(this->propertyAddress) + " into relation block address " +
                                    std::to_string(CentralRelationBlockAddress));
        return NULL;
    }

    CentralRelationBlock::nextRelationIndex += 1;
    CentralRelationBlock::centralRelationsDB->flush();
    return new CentralRelationBlock(CentralRelationBlockAddress, sourceData, destinationData, this->propertyAddress);
}


CentralRelationBlock* CentralRelationBlock::get(unsigned int address) {
    int RECORD_SIZE = sizeof(unsigned int);
    if (address == 0) {
        return NULL;
    } else if (address % CentralRelationBlock::BLOCK_SIZE != 0) {
        throw "Exception: Invalid relation block address !!\n received address = " + address;
    }

    CentralRelationBlock::centralRelationsDB->seekg(address + RECORD_SIZE*2);
    NodeRelation source;
    NodeRelation destination;
    unsigned int propertyReference;

    CentralRelationBlock::centralRelationsDB->read(reinterpret_cast<char*>(&source.address),
                                     RECORD_SIZE);  // < ------ relation data offset ID = 0
    if (!CentralRelationBlock::centralRelationsDB->read(reinterpret_cast<char*>(&destination.address),
                                          RECORD_SIZE)) {  // < ------ relation data offset ID = 1
        central_relation_block_logger.error(
                "Error while reading relation source node address offset ID = 1 from "
                "relation block address " +
                std::to_string(address));
        return NULL;
    }

    if (!CentralRelationBlock::centralRelationsDB->read(reinterpret_cast<char*>(&source.nextRelationId),
                                          RECORD_SIZE)) {  // < ------ relation data offset ID = 2
        central_relation_block_logger.error(
                "Error while reading relation source next relation address offset ID = 2 from "
                "relation block address " +
                std::to_string(address));
        return NULL;
    }

    if (!CentralRelationBlock::centralRelationsDB->read(reinterpret_cast<char*>(&source.nextPid),
                                          RECORD_SIZE)) {  // < ------ relation data offset ID = 3
        central_relation_block_logger.error(
                "Error while reading relation source next relation partition id offset ID = 3 from "
                "relation block address " +
                std::to_string(address));
        return NULL;
    }

    if (!CentralRelationBlock::centralRelationsDB->read(reinterpret_cast<char*>(&source.preRelationId),
                                          RECORD_SIZE)) {  // < ------ relation data offset ID = 4
        central_relation_block_logger.error(
                "Error while reading relation source previous relation address offset ID = 4 from "
                "relation block address " +
                std::to_string(address));
        return NULL;
    }

    if (!CentralRelationBlock::centralRelationsDB->read(reinterpret_cast<char*>(&source.prePid),
                                          RECORD_SIZE)) {  // < ------ relation data offset ID = 5
        central_relation_block_logger.error(
                "Error while reading relation source previous relation partition id offset ID = 5 from "
                "relation block address " +
                std::to_string(address));
        return NULL;
    }

    if (!CentralRelationBlock::centralRelationsDB->read(reinterpret_cast<char*>(&destination.nextRelationId),
                                          RECORD_SIZE)) {  // < ------ relation data offset ID = 6
        central_relation_block_logger.error(
                "Error while reading relation destination next relation address offset ID = 6 from "
                "relation block address " +
                std::to_string(address));
        return NULL;
    }

    if (!CentralRelationBlock::centralRelationsDB->read(reinterpret_cast<char*>(&destination.nextPid),
                                          RECORD_SIZE)) {  // < ------ relation data offset ID = 7
        central_relation_block_logger.error(
                "ERROR: Error while reading relation destination next relation partition id offset ID = 7 from "
                "relation block address " +
                std::to_string(address));
        return NULL;
    }

    if (!CentralRelationBlock::centralRelationsDB->read(reinterpret_cast<char*>(&destination.preRelationId),
                                          RECORD_SIZE)) {  // < ------ relation data offset ID = 8
        central_relation_block_logger.error(
                "ERROR: Error while reading relation destination previous relation address data offset ID = 8 from "
                "relation block address " +
                std::to_string(address));
        return NULL;
    }

    if (!CentralRelationBlock::centralRelationsDB->read(reinterpret_cast<char*>(&destination.prePid),
                                          RECORD_SIZE)) {  // < ------ relation data offset ID = 9
        central_relation_block_logger.error(
                "ERROR: Error while reading relation destination previous relation partition id data offset ID = 9 from "
                "relation block address " +
                std::to_string(address));
        return NULL;
    }

    if (!CentralRelationBlock::centralRelationsDB->read(reinterpret_cast<char*>(&propertyReference),
                                          RECORD_SIZE)) {  // < ------ relation data offset ID = 10
        central_relation_block_logger.error(
                "ERROR: Error while reading relation property address data offset ID = 10 from relation block address " +
                std::to_string(address));
        return NULL;
    }

    return new CentralRelationBlock(address, source, destination, propertyReference);
}



CentralRelationBlock* CentralRelationBlock::nextSource() { return CentralRelationBlock::get(this->source.nextRelationId); }


CentralRelationBlock* CentralRelationBlock::previousSource() { return CentralRelationBlock::get(this->source.preRelationId); }

CentralRelationBlock* CentralRelationBlock::nextDestination() { return CentralRelationBlock::get(this->destination.nextRelationId); }


CentralRelationBlock* CentralRelationBlock::previousDestination() { return CentralRelationBlock::get(this->destination.preRelationId); }

bool CentralRelationBlock::setNextSource(unsigned int newAddress) {
    if (this->updateRelationRecords(RelationOffsets::SOURCE_NEXT, newAddress)) {
        this->source.nextRelationId = newAddress;
    } else {
        throw "Exception: Error while updating the relation next source address " + std::to_string(newAddress);
    }
    return true;
}


bool CentralRelationBlock::setPreviousSource(unsigned int newAddress) {
    if (this->updateRelationRecords(RelationOffsets::SOURCE_PREVIOUS, newAddress)) {
        this->source.preRelationId = newAddress;
    } else {
        throw "Exception: Error while updating the relation previous source address " + std::to_string(newAddress);
    }
    return true;
}


bool CentralRelationBlock::setNextDestination(unsigned int newAddress) {
    if (this->updateRelationRecords(RelationOffsets::DESTINATION_NEXT, newAddress)) {
        this->destination.nextRelationId = newAddress;
    } else {
        throw "Exception: Error while updating the relation next destination address " + std::to_string(newAddress);
    }
    return true;
}


bool CentralRelationBlock::setPreviousDestination(unsigned int newAddress) {
    if (this->updateRelationRecords(RelationOffsets::DESTINATION_PREVIOUS, newAddress)) {
        this->destination.preRelationId = newAddress;
    } else {
        throw "Exception: Error while updating the relation previous destination address " + std::to_string(newAddress);
    }
    return true;
}


/**
 * Update relation record block given the offset to the recored from the begining, i:e
 *  recordOffset 0 --> Source address
 *  recordOffset 1 --> Destination address
 *  recordOffset 2 --> Source's next relation block address
 *  recordOffset 3 --> Source's next relation block partition id
 *  recordOffset 4 --> Source's previous relation block address
 *  recordOffset 5 --> Source's previous relation block partition id
 *  recordOffset 6 --> Destination's next relation block address
 *  recordOffset 7 --> Destination's next relation block partition id
 *  recordOffset 8 --> Destination's previous relation block address
 *  recordOffset 9 --> Destination's previous relation block partition id
 * recordOffset 10 --> Relation's property address in the properties DB
 * */
bool CentralRelationBlock::updateRelationRecords(RelationOffsets recordOffset, unsigned int data) {
    int offsetValue = static_cast<int>(recordOffset);
    int dataOffset = RECORD_SIZE * offsetValue;
    CentralRelationBlock::centralRelationsDB->seekg(this->addr + dataOffset);
    if (!CentralRelationBlock::centralRelationsDB->write(reinterpret_cast<char*>(&data), RECORD_SIZE)) {
        central_relation_block_logger.error("Error while updating relation data record offset " + std::to_string(offsetValue) +
                                    "data " + std::to_string(data));
        return false;
    }
    CentralRelationBlock::centralRelationsDB->flush();
    return true;
}



bool CentralRelationBlock::isInUse() { return this->usage == '\1'; }
unsigned int CentralRelationBlock::nextRelationIndex = 1;  // Starting with 1 because of the 0 and '\0' differentiation issue
unsigned int CentralRelationBlock::nextCentralRelationIndex = 1;  // Starting with 1 because of the 0 and '\0' differentiation issue


void CentralRelationBlock::addProperty(std::string name, char* value) {

    if (this->propertyAddress == 0) {
        CentralPropertyEdgeLink* newLink = CentralPropertyEdgeLink::create(name, value);
        if (newLink) {
            this->propertyAddress = newLink->blockAddress;
            // If it was an empty prop link before inserting, Then update the property reference of this node
            // block
            central_relation_block_logger.info("New edge property name  = " + std::string(name));
            central_relation_block_logger.info("property addr  = " + std::to_string(this->propertyAddress));

            this->updateRelationRecords(RelationOffsets::RELATION_PROPS, this->propertyAddress);
        } else {
            throw "Error occurred while adding a new property link to " + std::to_string(this->addr) + " node block";
        }
    } else {
        this->propertyAddress = this->getPropertyHead()->insert(name, value);
    }

}



CentralPropertyEdgeLink* CentralRelationBlock::getPropertyHead() { return CentralPropertyEdgeLink::get(this->propertyAddress); }

std::map<std::string, char*> CentralRelationBlock::getAllProperties() {
    std::map<std::string, char*> allProperties;
    CentralPropertyEdgeLink* current = this->getPropertyHead();
    while (current) {
        allProperties.insert({current->name, current->value});
        CentralPropertyEdgeLink* temp = current->next();
        delete current;  // To prevent memory leaks
        current = temp;
    }
    delete current;
    return allProperties;
}

/**
 * Get the source node in the current (this) relationship
 *
 * */
CentralNodeBlock* CentralRelationBlock::getSource() {
    if (this->sourceBlock) {
        return sourceBlock;
    } else {
        central_relation_block_logger.warn("Get source from node block address is not implemented yet!");
        return NULL;
    }
}

/**
 * Get the destination node in the relationship
 *
 * */
CentralNodeBlock* CentralRelationBlock::getDestination() {
    if (this->destinationBlock) {
        return destinationBlock;
    } else {
        central_relation_block_logger.warn("Get destination from node block address is not implemented yet!");
        return NULL;
    }
}

const unsigned long CentralRelationBlock::BLOCK_SIZE = CentralRelationBlock::RECORD_SIZE * 13;
// One relation block holds 11 recods such as source addres, destination address, source next relation address etc.
// and one record is typically 4 bytes (size of unsigned int)
std::string CentralRelationBlock::DB_PATH = "/home/sandaruwan/ubuntu/software/jasminegraph/streamStore/relations.db";
std::fstream* CentralRelationBlock::centralRelationsDB = NULL;