/**
Copyright 2020 JasmineGraph Team
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

#include "RelationBlock.h"

#include <sstream>
#include <vector>

#include "../util/logger/Logger.h"
#include "NodeManager.h"
#include "MetaPropertyEdgeLink.h"

Logger relation_block_logger;
pthread_mutex_t lockAddProperty;

RelationBlock* RelationBlock::addLocalRelation(NodeBlock source, NodeBlock destination) {
    int RECORD_SIZE = sizeof(unsigned int);

    NodeRelation sourceData;
    NodeRelation destinationData;

    sourceData.address = source.addr;
    destinationData.address = destination.addr;
    long relationBlockAddress = RelationBlock::nextLocalRelationIndex *
            RelationBlock::BLOCK_SIZE;  // Block size is 4 * 13 + 18

    RelationBlock::relationsDB->seekg(relationBlockAddress);
    if (!RelationBlock::relationsDB->write(reinterpret_cast<char*>(&source.nodeId), RECORD_SIZE)) {
        relation_block_logger.error("ERROR: Error while writing  sourceAddr " + std::to_string(source.nodeId) +
                                    " into relation block address " + std::to_string(relationBlockAddress));
        return NULL;
    }
    if (!RelationBlock::relationsDB->write(reinterpret_cast<char*>(&destination.nodeId), RECORD_SIZE)) {
        relation_block_logger.error("ERROR: Error while writing  destinationAddr " +
                                    std::to_string(destination.nodeId) + " into relation block address " +
                                    std::to_string(relationBlockAddress));
        return NULL;
    }
    if (!RelationBlock::relationsDB->write(reinterpret_cast<char*>(&sourceData.address), RECORD_SIZE)) {
        relation_block_logger.error("ERROR: Error while writing relation sourceAddr " +
                                    std::to_string(sourceData.address) + " into relation block address " +
                                    std::to_string(relationBlockAddress));
        return NULL;
    }

    if (!RelationBlock::relationsDB->write(reinterpret_cast<char*>(&destinationData.address), RECORD_SIZE)) {
        relation_block_logger.error("ERROR: Error while writing relation destAddr " +
                                    std::to_string(destinationData.address) + " into relation block address " +
                                    std::to_string(relationBlockAddress));
        return NULL;
    }

    if (!RelationBlock::relationsDB->write(reinterpret_cast<char*>(&sourceData.nextRelationId), RECORD_SIZE)) {
        relation_block_logger.error("ERROR: Error while writing source next relation address " +
                                    std::to_string(sourceData.nextRelationId) + " into relation block address " +
                                    std::to_string(relationBlockAddress));
        return NULL;
    }

    if (!RelationBlock::relationsDB->write(reinterpret_cast<char*>(&sourceData.nextPid), RECORD_SIZE)) {
        relation_block_logger.error("ERROR: Error while writing source next relation partition ID " +
                                    std::to_string(sourceData.nextPid) + " into relation block address " +
                                    std::to_string(relationBlockAddress));
        return NULL;
    }

    if (!RelationBlock::relationsDB->write(reinterpret_cast<char*>(&sourceData.preRelationId), RECORD_SIZE)) {
        relation_block_logger.error("ERROR: Error while writing source previous relation address " +
                                    std::to_string(sourceData.preRelationId) + " into relation block address " +
                                    std::to_string(relationBlockAddress));
        return NULL;
    }

    if (!RelationBlock::relationsDB->write(reinterpret_cast<char*>(&sourceData.prePid), RECORD_SIZE)) {
        relation_block_logger.error("ERROR: Error while writing source previous relation partition ID " +
                                    std::to_string(sourceData.prePid) + " into relation block address " +
                                    std::to_string(relationBlockAddress));
        return NULL;
    }

    if (!RelationBlock::relationsDB->write(reinterpret_cast<char*>(&destinationData.nextRelationId), RECORD_SIZE)) {
        relation_block_logger.error("ERROR: Error while writing destination next relation address " +
                                    std::to_string(destinationData.nextRelationId) + " into relation block address " +
                                    std::to_string(relationBlockAddress));
        return NULL;
    }

    if (!RelationBlock::relationsDB->write(reinterpret_cast<char*>(&destinationData.nextPid), RECORD_SIZE)) {
        relation_block_logger.error("ERROR: Error while writing destination next partition id " +
                                    std::to_string(destinationData.nextPid) + " into relation block address " +
                                    std::to_string(relationBlockAddress));
        return NULL;
    }

    if (!RelationBlock::relationsDB->write(reinterpret_cast<char*>(&destinationData.preRelationId), RECORD_SIZE)) {
        relation_block_logger.error("ERROR: Error while writing destination previous relation address " +
                                    std::to_string(destinationData.preRelationId) + " into relation block address " +
                                    std::to_string(relationBlockAddress));
        return NULL;
    }

    if (!RelationBlock::relationsDB->write(reinterpret_cast<char*>(&destinationData.prePid), RECORD_SIZE)) {
        relation_block_logger.error("ERROR: Error while writing destination previous relation partition id " +
                                    std::to_string(destinationData.prePid) + " into relation block address " +
                                    std::to_string(relationBlockAddress));
        return NULL;
    }

    if (!RelationBlock::relationsDB->write(reinterpret_cast<char*>(&(this->propertyAddress)), RECORD_SIZE)) {
        relation_block_logger.error("ERROR: Error while writing relation property address " +
                                    std::to_string(this->propertyAddress) + " into relation block address " +
                                    std::to_string(relationBlockAddress));
        return NULL;
    }

    if (!RelationBlock::relationsDB->write(reinterpret_cast<char*>(&(this->type)), MAX_TYPE_SIZE)) {
        relation_block_logger.error("ERROR: Error while writing relation type " +
                                    this->type + " into relation block address " +
                                    std::to_string(relationBlockAddress));
        return NULL;
    }

    RelationBlock::nextLocalRelationIndex += 1;
    RelationBlock::relationsDB->flush();
    return new RelationBlock(relationBlockAddress, sourceData, destinationData,
                             this->propertyAddress, this->type);
}

RelationBlock* RelationBlock::addCentralRelation(NodeBlock source, NodeBlock destination) {
    relation_block_logger.debug("Writing central relation with source " + std::to_string(source.nodeId) +
                               " and destination " + std::to_string(destination.nodeId));
    int RECORD_SIZE = sizeof(unsigned int);

    NodeRelation sourceData;
    NodeRelation destinationData;

    sourceData.address = source.addr;
    destinationData.address = destination.addr;

    long relationBlockAddress =
        RelationBlock::nextCentralRelationIndex * RelationBlock::CENTRAL_BLOCK_SIZE;
    RelationBlock::centralRelationsDB->seekg(relationBlockAddress);
    if (!RelationBlock::centralRelationsDB->write(reinterpret_cast<char*>(&source.nodeId), RECORD_SIZE)) {
        relation_block_logger.error("ERROR: Error while writing  sourceAddr " + std::to_string(source.nodeId) +
                                    " into relation block address " + std::to_string(relationBlockAddress));
        return NULL;
    }
    if (!RelationBlock::centralRelationsDB->write(reinterpret_cast<char*>(&destination.nodeId), RECORD_SIZE)) {
        relation_block_logger.error("ERROR: Error while writing  destinationAddr " +
                                    std::to_string(destination.nodeId) + " into relation block address " +
                                    std::to_string(relationBlockAddress));
        return NULL;
    }
    if (!RelationBlock::centralRelationsDB->write(reinterpret_cast<char*>(&sourceData.address), RECORD_SIZE)) {
        relation_block_logger.error("ERROR: Error while writing relation sourceAddr " +
                                    std::to_string(sourceData.address) + " into relation block address " +
                                    std::to_string(relationBlockAddress));
        return NULL;
    }

    if (!RelationBlock::centralRelationsDB->write(reinterpret_cast<char*>(&destinationData.address), RECORD_SIZE)) {
        relation_block_logger.error("ERROR: Error while writing relation destAddr " +
                                    std::to_string(destinationData.address) + " into relation block address " +
                                    std::to_string(relationBlockAddress));
        return NULL;
    }

    if (!RelationBlock::centralRelationsDB->write(reinterpret_cast<char*>(&sourceData.nextRelationId), RECORD_SIZE)) {
        relation_block_logger.error("ERROR: Error while writing source next relation address " +
                                    std::to_string(sourceData.nextRelationId) + " into relation block address " +
                                    std::to_string(relationBlockAddress));
        return NULL;
    }

    if (!RelationBlock::centralRelationsDB->write(reinterpret_cast<char*>(&sourceData.nextPid), RECORD_SIZE)) {
        relation_block_logger.error("ERROR: Error while writing source next relation partition ID " +
                                    std::to_string(sourceData.nextPid) + " into relation block address " +
                                    std::to_string(relationBlockAddress));
        return NULL;
    }

    if (!RelationBlock::centralRelationsDB->write(reinterpret_cast<char*>(&sourceData.preRelationId), RECORD_SIZE)) {
        relation_block_logger.error("ERROR: Error while writing source previous relation address " +
                                    std::to_string(sourceData.preRelationId) + " into relation block address " +
                                    std::to_string(relationBlockAddress));
        return NULL;
    }

    if (!RelationBlock::centralRelationsDB->write(reinterpret_cast<char*>(&sourceData.prePid), RECORD_SIZE)) {
        relation_block_logger.error("ERROR: Error while writing source previous relation partition ID " +
                                    std::to_string(sourceData.prePid) + " into relation block address " +
                                    std::to_string(relationBlockAddress));
        return NULL;
    }

    if (!RelationBlock::centralRelationsDB->write(reinterpret_cast<char*>(&destinationData.nextRelationId),
                                                  RECORD_SIZE)) {
        relation_block_logger.error("ERROR: Error while writing destination next relation address " +
                                    std::to_string(destinationData.nextRelationId) + " into relation block address " +
                                    std::to_string(relationBlockAddress));
        return NULL;
    }

    if (!RelationBlock::centralRelationsDB->write(reinterpret_cast<char*>(&destinationData.nextPid), RECORD_SIZE)) {
        relation_block_logger.error("ERROR: Error while writing destination next partition id " +
                                    std::to_string(destinationData.nextPid) + " into relation block address " +
                                    std::to_string(relationBlockAddress));
        return NULL;
    }

    if (!RelationBlock::centralRelationsDB->write(reinterpret_cast<char*>(&destinationData.preRelationId),
                                                  RECORD_SIZE)) {
        relation_block_logger.error("ERROR: Error while writing destination previous relation address " +
                                    std::to_string(destinationData.preRelationId) + " into relation block address " +
                                    std::to_string(relationBlockAddress));
        return NULL;
    }

    if (!RelationBlock::centralRelationsDB->write(reinterpret_cast<char*>(&destinationData.prePid), RECORD_SIZE)) {
        relation_block_logger.error("ERROR: Error while writing destination previous relation partition id " +
                                    std::to_string(destinationData.prePid) + " into relation block address " +
                                    std::to_string(relationBlockAddress));
        return NULL;
    }

    if (!RelationBlock::centralRelationsDB->write(reinterpret_cast<char*>(&(this->propertyAddress)), RECORD_SIZE)) {
        relation_block_logger.error("ERROR: Error while writing relation property address " +
                                    std::to_string(this->propertyAddress) + " into relation block address " +
                                    std::to_string(relationBlockAddress));
        return NULL;
    }

    if (!RelationBlock::centralRelationsDB->write(reinterpret_cast<char*>(&(this->metaPropertyAddress)),
                                                  RECORD_SIZE)) {
        relation_block_logger.error("ERROR: Error while writing relation property address " +
                                    std::to_string(this->metaPropertyAddress) + " into relation block address " +
                                    std::to_string(relationBlockAddress));
        return NULL;
    }

    if (!RelationBlock::centralRelationsDB->write(reinterpret_cast<char*>(&(this->type)), MAX_TYPE_SIZE)) {
        relation_block_logger.error("ERROR: Error while writing relation type " +
                                    std::to_string(this->propertyAddress) + " into relation block address " +
                                    std::to_string(relationBlockAddress));
        return NULL;
    }

    RelationBlock::nextCentralRelationIndex += 1;
    RelationBlock::centralRelationsDB->flush();
    return new RelationBlock(relationBlockAddress, sourceData, destinationData,
                             this->propertyAddress, this->metaPropertyAddress, this->type);
}

RelationBlock* RelationBlock::getLocalRelation(unsigned int address) {
    int RECORD_SIZE = sizeof(unsigned int);
    if (address == 0) {
        return NULL;
    }
    if (address % RelationBlock::BLOCK_SIZE != 0) {
        relation_block_logger.error("Exception: Invalid relation block address !!\n received address = " + address);
        return NULL;
    }
    RelationBlock::relationsDB->seekg(address);  // Address is relation ID
    NodeRelation source;
    NodeRelation destination;
    unsigned int propertyReference;
    char type[RelationBlock::MAX_TYPE_SIZE] = {0};

    if (!RelationBlock::relationsDB->read(reinterpret_cast<char*>(&source.nodeId),
                                          RECORD_SIZE)) {  // < ------ relation data offset ID = 0
        relation_block_logger.error(
                "Error while reading local relation source node address offset ID = 0 from "
                "relation block address " +
                std::to_string(address));
        return NULL;
    }

    if (!RelationBlock::relationsDB->read(reinterpret_cast<char*>(&destination.nodeId),
                                          RECORD_SIZE)) {  // < ------ relation data offset ID = 1
        relation_block_logger.error(
                "Error while reading local relation source node address offset ID = 0 from "
                "relation block address " +
                std::to_string(address));
        return NULL;
    }

    if (!RelationBlock::relationsDB->read(reinterpret_cast<char*>(&source.address),
                                          RECORD_SIZE)) {  // < ------ relation data offset ID = 2
        relation_block_logger.error(
                "Error while reading local relation source node address offset ID = 0 from "
                "relation block address " +
                std::to_string(address));
        return NULL;
    }

    if (!RelationBlock::relationsDB->read(reinterpret_cast<char*>(&destination.address),
                                          RECORD_SIZE)) {  // < ------ relation data offset ID = 3
        relation_block_logger.error(
            "Error while reading local relation source node address offset ID = 1 from "
            "relation block address " +
            std::to_string(address));
        return NULL;
    }

    if (!RelationBlock::relationsDB->read(reinterpret_cast<char*>(&source.nextRelationId),
                                          RECORD_SIZE)) {  // < ------ relation data offset ID = 4
        relation_block_logger.error(
            "Error while reading local relation source next relation address offset ID = 2 from "
            "relation block address " +
            std::to_string(address));
        return NULL;
    }

    if (!RelationBlock::relationsDB->read(reinterpret_cast<char*>(&source.nextPid),
                                          RECORD_SIZE)) {  // < ------ relation data offset ID = 5
        relation_block_logger.error(
            "Error while reading local relation source next relation partition id offset ID = 3 from "
            "relation block address " +
            std::to_string(address));
        return NULL;
    }

    if (!RelationBlock::relationsDB->read(reinterpret_cast<char*>(&source.preRelationId),
                                          RECORD_SIZE)) {  // < ------ relation data offset ID = 6
        relation_block_logger.error(
            "Error while reading local relation source previous relation address offset ID = 4 from "
            "relation block address " +
            std::to_string(address));
        return NULL;
    }

    if (!RelationBlock::relationsDB->read(reinterpret_cast<char*>(&source.prePid),
                                          RECORD_SIZE)) {  // < ------ relation data offset ID = 7
        relation_block_logger.error(
            "Error while reading local relation source previous relation partition id offset ID = 5 from "
            "relation block address " +
            std::to_string(address));
        return NULL;
    }

    if (!RelationBlock::relationsDB->read(reinterpret_cast<char*>(&destination.nextRelationId),
                                          RECORD_SIZE)) {  // < ------ relation data offset ID = 8
        relation_block_logger.error(
            "Error while reading local relation destination next relation address offset ID = 6 from "
            "relation block address " +
            std::to_string(address));
        return NULL;
    }

    if (!RelationBlock::relationsDB->read(reinterpret_cast<char*>(&destination.nextPid),
                                          RECORD_SIZE)) {  // < ------ relation data offset ID = 9
        relation_block_logger.error(
            "ERROR: Error while reading local relation destination next relation partition id offset ID = 7 from "
            "relation block address " +
            std::to_string(address));
        return NULL;
    }

    if (!RelationBlock::relationsDB->read(reinterpret_cast<char*>(&destination.preRelationId),
                                          RECORD_SIZE)) {  // < ------ relation data offset ID = 10
        relation_block_logger.error(
            "ERROR: Error while reading local relation destination previous relation address data offset ID = 8 from "
            "relation block address " +
            std::to_string(address));
        return NULL;
    }

    if (!RelationBlock::relationsDB->read(reinterpret_cast<char*>(&destination.prePid),
                                          RECORD_SIZE)) {  // < ------ relation data offset ID = 11
        relation_block_logger.error(
            "ERROR: Error while reading local relation destination previous "
            "relation partition id data offset ID = 9 from "
            "relation block address " + std::to_string(address));
        return NULL;
    }

    if (!RelationBlock::relationsDB->read(reinterpret_cast<char*>(&propertyReference),
                                          RECORD_SIZE)) {  // < ------ relation data offset ID = 12
        relation_block_logger.error(
            "ERROR: Error while reading local relation property address data "
            "offset ID = 10 from relation block address " + std::to_string(address));
        return NULL;
    }

    if (!RelationBlock::relationsDB->read(reinterpret_cast<char*>(&type),
                                          RelationBlock::MAX_TYPE_SIZE)) {  // < ------ relation data offset ID = 10
        relation_block_logger.error(
                "ERROR: Error while reading local relation property address data "
                "offset ID = 10 from relation block address " + std::to_string(address));
        return NULL;
    }
    return new RelationBlock(address, source, destination, propertyReference, type);
}

RelationBlock* RelationBlock::getCentralRelation(unsigned int address) {
    int RECORD_SIZE = sizeof(unsigned int);
    if (address == 0) {
        return NULL;
    }
    if (address % RelationBlock::CENTRAL_BLOCK_SIZE != 0) {
        relation_block_logger.error("Exception: Invalid relation block address !!\n received address = " + address);
        return NULL;
    }

    RelationBlock::centralRelationsDB->seekg(address);
    NodeRelation source;
    NodeRelation destination;
    unsigned int propertyReference;
    unsigned int metaPropertyReference;
    char type[RelationBlock::MAX_TYPE_SIZE] = {0};

    if (!RelationBlock::centralRelationsDB->read(reinterpret_cast<char*>(&source.nodeId),
                                          RECORD_SIZE)) {  // < ------ relation data offset ID = 0
        relation_block_logger.error(
                "Error while reading central relation source node address offset ID = 0 from "
                "relation block address " +
                std::to_string(address));
        return NULL;
    }

    if (!RelationBlock::centralRelationsDB->read(reinterpret_cast<char*>(&destination.nodeId),
                                          RECORD_SIZE)) {  // < ------ relation data offset ID = 1
        relation_block_logger.error(
                "Error while reading central relation source node address offset ID = 0 from "
                "relation block address " +
                std::to_string(address));
        return NULL;
    }

    if (!RelationBlock::centralRelationsDB->read(reinterpret_cast<char*>(&source.address),
                                          RECORD_SIZE)) {  // < ------ relation data offset ID = 2
        relation_block_logger.error(
                "Error while reading central relation source node address offset ID = 0 from "
                "relation block address " +
                std::to_string(address));
        return NULL;
    }
    if (!RelationBlock::centralRelationsDB->read(reinterpret_cast<char*>(&destination.address),
                                                 RECORD_SIZE)) {  // < ------ relation data offset ID = 3
        relation_block_logger.error(
            "Error while reading central relation source node address offset ID = 1 from "
            "relation block address " +
            std::to_string(address));
        return NULL;
    }

    if (!RelationBlock::centralRelationsDB->read(reinterpret_cast<char*>(&source.nextRelationId),
                                                 RECORD_SIZE)) {  // < ------ relation data offset ID = 4
        relation_block_logger.error(
            "Error while reading central relation source next relation address offset ID = 2 from "
            "relation block address " +
            std::to_string(address));
        return NULL;
    }

    if (!RelationBlock::centralRelationsDB->read(reinterpret_cast<char*>(&source.nextPid),
                                                 RECORD_SIZE)) {  // < ------ relation data offset ID = 5
        relation_block_logger.error(
            "Error while reading central relation source next relation partition id offset ID = 3 from "
            "relation block address " +
            std::to_string(address));
        return NULL;
    }

    if (!RelationBlock::centralRelationsDB->read(reinterpret_cast<char*>(&source.preRelationId),
                                                 RECORD_SIZE)) {  // < ------ relation data offset ID = 6
        relation_block_logger.error(
            "Error while reading central relation source previous relation address offset ID = 4 from "
            "relation block address " +
            std::to_string(address));
        return NULL;
    }

    if (!RelationBlock::centralRelationsDB->read(reinterpret_cast<char*>(&source.prePid),
                                                 RECORD_SIZE)) {  // < ------ relation data offset ID = 7
        relation_block_logger.error(
            "Error while reading central relation source previous relation partition id offset ID = 5 from "
            "relation block address " +
            std::to_string(address));
        return NULL;
    }

    if (!RelationBlock::centralRelationsDB->read(reinterpret_cast<char*>(&destination.nextRelationId),
                                                 RECORD_SIZE)) {  // < ------ relation data offset ID = 8
        relation_block_logger.error(
            "Error while reading central relation destination next relation address offset ID = 6 from "
            "relation block address " +
            std::to_string(address));
        return NULL;
    }

    if (!RelationBlock::centralRelationsDB->read(reinterpret_cast<char*>(&destination.nextPid),
                                                 RECORD_SIZE)) {  // < ------ relation data offset ID = 9
        relation_block_logger.error(
            "ERROR: Error while reading central relation destination next relation partition id offset ID = 7 from "
            "relation block address " +
            std::to_string(address));
        return NULL;
    }

    if (!RelationBlock::centralRelationsDB->read(reinterpret_cast<char*>(&destination.preRelationId),
                                                 RECORD_SIZE)) {  // < ------ relation data offset ID = 10
        relation_block_logger.error(
            "ERROR: Error while reading central relation destination previous relation address "
            "data offset ID = 8 from "
            "relation block address " + std::to_string(address));
        return NULL;
    }

    if (!RelationBlock::centralRelationsDB->read(reinterpret_cast<char*>(&destination.prePid),
                                                 RECORD_SIZE)) {  // < ------ relation data offset ID = 11
        relation_block_logger.error(
            "ERROR: Error while reading central relation destination previous relation partition "
            "id data offset ID = 9 from "
            "relation block address " + std::to_string(address));
        return NULL;
    }

    if (!RelationBlock::centralRelationsDB->read(reinterpret_cast<char*>(&propertyReference),
                                                 RECORD_SIZE)) {  // < ------ relation data offset ID = 12
        relation_block_logger.error(
            "ERROR: Error while reading central relation property address data offset ID = 10 from "
            "relation block address " + std::to_string(address));
        return NULL;
    }

    if (!RelationBlock::centralRelationsDB->read(reinterpret_cast<char*>(&metaPropertyReference),
                                                 RECORD_SIZE)) {  // < ------ relation data offset ID = 13
        relation_block_logger.error(
                "ERROR: Error while reading central relation meta property address data offset ID = 10 from "
                "relation block address " + std::to_string(address));
        return NULL;
    }

    if (!RelationBlock::centralRelationsDB->read(reinterpret_cast<char*>(&type),
                                          RelationBlock::MAX_TYPE_SIZE)) {  // < ------ relation data offset ID = 10
        relation_block_logger.error(
                "ERROR: Error while reading local relation property address data "
                "offset ID = 10 from relation block address " + std::to_string(address));
        return NULL;
    }

    return new RelationBlock(address, source, destination, propertyReference,
                             metaPropertyReference, type);
}

RelationBlock* RelationBlock::nextLocalSource() {
    return RelationBlock::getLocalRelation(this->source.nextRelationId);
}

RelationBlock* RelationBlock::nextCentralSource() {
    return RelationBlock::getCentralRelation(this->source.nextRelationId);
}

RelationBlock* RelationBlock::previousLocalSource() {
    return RelationBlock::getLocalRelation(this->source.preRelationId);
}

RelationBlock* RelationBlock::previousCentralSource() {
    return RelationBlock::getCentralRelation(this->source.preRelationId);
}

RelationBlock* RelationBlock::nextLocalDestination() {
    return RelationBlock::getLocalRelation(this->destination.nextRelationId);
}

RelationBlock* RelationBlock::nextCentralDestination() {
    return RelationBlock::getCentralRelation(this->destination.nextRelationId);
}

RelationBlock* RelationBlock::previousLocalDestination() {
    return RelationBlock::getLocalRelation(this->destination.preRelationId);
}

RelationBlock* RelationBlock::previousCentralDestination() {
    return RelationBlock::getCentralRelation(this->destination.preRelationId);
}

bool RelationBlock::setLocalNextSource(unsigned int newAddress) {
    if (this->updateLocalRelationRecords(RelationOffsets::SOURCE_NEXT, newAddress)) {
        this->source.nextRelationId = newAddress;
    } else {
        relation_block_logger.error("Exception: Error while updating the relation next source address " +
                std::to_string(newAddress));
    }
    return true;
}

bool RelationBlock::setCentralNextSource(unsigned int newAddress) {
    if (this->updateCentralRelationRecords(RelationOffsets::SOURCE_NEXT, newAddress)) {
        this->source.nextRelationId = newAddress;
    } else {
        relation_block_logger.error("Exception: Error while updating the relation next source address " +
                std::to_string(newAddress));
    }
    return true;
}

bool RelationBlock::setLocalPreviousSource(unsigned int newAddress) {
    if (this->updateLocalRelationRecords(RelationOffsets::SOURCE_PREVIOUS, newAddress)) {
        this->source.preRelationId = newAddress;
    } else {
        relation_block_logger.error("Exception: Error while updating the relation previous source address " +
                std::to_string(newAddress));
    }
    return true;
}

bool RelationBlock::setCentralPreviousSource(unsigned int newAddress) {
    if (this->updateCentralRelationRecords(RelationOffsets::SOURCE_PREVIOUS, newAddress)) {
        this->source.preRelationId = newAddress;
    } else {
        relation_block_logger.error("Exception: Error while updating the relation previous source address " +
                std::to_string(newAddress));
    }
    return true;
}

bool RelationBlock::setLocalNextDestination(unsigned int newAddress) {
    if (this->updateLocalRelationRecords(RelationOffsets::DESTINATION_NEXT, newAddress)) {
        this->destination.nextRelationId = newAddress;
    } else {
        relation_block_logger.error("Exception: Error while updating the relation next destination address " +
                std::to_string(newAddress));
    }
    return true;
}

bool RelationBlock::setCentralNextDestination(unsigned int newAddress) {
    if (this->updateCentralRelationRecords(RelationOffsets::DESTINATION_NEXT, newAddress)) {
        this->destination.nextRelationId = newAddress;
    } else {
        relation_block_logger.error("Exception: Error while updating the relation next destination address " +
                std::to_string(newAddress));
    }
    return true;
}

bool RelationBlock::setLocalPreviousDestination(unsigned int newAddress) {
    if (this->updateLocalRelationRecords(RelationOffsets::DESTINATION_PREVIOUS, newAddress)) {
        this->destination.preRelationId = newAddress;
    } else {
        relation_block_logger.error("Exception: Error while updating the relation previous destination address " +
                std::to_string(newAddress));
    }
    return true;
}

bool RelationBlock::setCentralPreviousDestination(unsigned int newAddress) {
    if (this->updateCentralRelationRecords(RelationOffsets::DESTINATION_PREVIOUS, newAddress)) {
        this->destination.preRelationId = newAddress;
    } else {
        relation_block_logger.error("Exception: Error while updating the relation previous destination address " +
                std::to_string(newAddress));
    }
    return true;
}

/**
 * Update relation record block given the offset to the record from the beginning, i:e
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
bool RelationBlock::updateLocalRelationRecords(RelationOffsets recordOffset, unsigned int data) {
    int offsetValue = static_cast<int>(recordOffset);
    int dataOffset = RECORD_SIZE * offsetValue;
    RelationBlock::relationsDB->seekg(this->addr + dataOffset);
    if (!RelationBlock::relationsDB->write(reinterpret_cast<char*>(&data), RECORD_SIZE)) {
        relation_block_logger.error("Error while updating relation data record offset " + std::to_string(offsetValue) +
                                    "data " + std::to_string(data));
        return false;
    }
    RelationBlock::relationsDB->flush();
    return true;
}

bool RelationBlock::updateCentralRelationRecords(RelationOffsets recordOffset, unsigned int data) {
    int offsetValue = static_cast<int>(recordOffset);
    int dataOffset = RECORD_SIZE * offsetValue;
    RelationBlock::centralRelationsDB->seekg(this->addr + dataOffset);
    if (!RelationBlock::centralRelationsDB->write(reinterpret_cast<char*>(&data), RECORD_SIZE)) {
        relation_block_logger.error("Error while updating relation data record offset " + std::to_string(offsetValue) +
                                    "data " + std::to_string(data));
        return false;
    }
    RelationBlock::centralRelationsDB->flush();
    return true;
}

bool RelationBlock::updateLocalRelationshipType(int offset, std::string data) {
    int offsetValue = static_cast<int>(offset);
    int dataOffset = RECORD_SIZE * offsetValue;
    RelationBlock::centralRelationsDB->seekg(this->addr + dataOffset);
    char type[RelationBlock::MAX_TYPE_SIZE] = {0};
    std::memcpy(type, data.c_str(),
                MAX_TYPE_SIZE);
    if (!RelationBlock::relationsDB->write(reinterpret_cast<char*>(&type), MAX_TYPE_SIZE)) {
        relation_block_logger.error("Error while updating relation data record offset " + std::to_string(offsetValue) +
                                    "data " + data);
        return false;
    }
    RelationBlock::relationsDB->flush();
    return true;
}

bool RelationBlock::updateCentralRelationshipType(int offset, std::string data) {
    int offsetValue = static_cast<int>(offset);
    int dataOffset = RECORD_SIZE * offsetValue;
    RelationBlock::centralRelationsDB->seekg(this->addr + dataOffset);
    char type[RelationBlock::MAX_TYPE_SIZE] = {0};
    std::memcpy(type, data.c_str(),
                MAX_TYPE_SIZE);
    if (!RelationBlock::centralRelationsDB->write(reinterpret_cast<char*>(&type), MAX_TYPE_SIZE)) {
        relation_block_logger.error("Error while updating relation data record offset " + std::to_string(offsetValue) +
                                    "data " + data);
        return false;
    }
    RelationBlock::centralRelationsDB->flush();
    return true;
}

bool RelationBlock::isInUse() { return this->usage == '\1'; }
thread_local unsigned int RelationBlock::nextLocalRelationIndex =
        1;  // Starting with 1 because of the 0 and '\0' differentiation issue
thread_local unsigned int RelationBlock::nextCentralRelationIndex =
    1;  // Starting with 1 because of the 0 and '\0' differentiation issue


void RelationBlock::addLocalProperty(std::string name, char* value) {
    if (this->propertyAddress == 0) {
        PropertyEdgeLink* newLink = PropertyEdgeLink::create(name, value);
        if (newLink) {
            this->propertyAddress = newLink->blockAddress;
            // If it was an empty prop link before inserting, Then update the property reference of this node
            // block
            this->updateLocalRelationRecords(RelationOffsets::RELATION_PROPS, this->propertyAddress);
            delete newLink; // Free the allocated PropertyEdgeLink to prevent memory leak
        } else {
            relation_block_logger.error("Error occurred while adding a new property link to " +
                    std::to_string(this->addr) + " node block");
        }
    } else {
    PropertyEdgeLink* propHead = this->getPropertyHead();
    this->propertyAddress = propHead->insert(name, value);
    delete propHead; // Free the allocated PropertyEdgeLink to prevent memory leak
    }
}
void RelationBlock::addCentralProperty(std::string name, char* value) {
    if (this->propertyAddress == 0) {
        PropertyEdgeLink* newLink = PropertyEdgeLink::create(name, value);
        if (newLink) {
            this->propertyAddress = newLink->blockAddress;
            // If it was an empty prop link before inserting, Then update the property reference of this node
            // block
            this->updateCentralRelationRecords(RelationOffsets::RELATION_PROPS, this->propertyAddress);
            delete newLink;
        } else {
            relation_block_logger.error("Error occurred while adding a new property link to " +
                    std::to_string(this->addr) + " node block");
        }
    } else {
       PropertyEdgeLink* propHead = this->getPropertyHead();
       unsigned int newPropAddr = propHead->insert(name, value);
       delete propHead; // Free the allocated PropertyEdgeLink
       this->propertyAddress = newPropAddr;;
    }
}

void RelationBlock::addMetaProperty(std::string name, char *value) {
    if (this->metaPropertyAddress == 0) {
        MetaPropertyEdgeLink* newLink = MetaPropertyEdgeLink::create(name, value);
        if (newLink) {
            this->metaPropertyAddress = newLink->blockAddress;
            // If it was an empty prop link before inserting, Then update the property reference of this node
            // block
            this->updateCentralRelationRecords(RelationOffsets::RELATION_PROPS_META,
                                               this->metaPropertyAddress);
            delete newLink; // Free the allocated MetaPropertyEdgeLink
        } else {
            relation_block_logger.error("Error occurred while adding a new property link to " +
                                        std::to_string(this->addr) + " node block");
        }
    } else {
      MetaPropertyEdgeLink* metaPropHead = this->getMetaPropertyHead();
      this->metaPropertyAddress = metaPropHead->insert(name, value);
      delete metaPropHead; // Free the allocated MetaPropertyEdgeLink
    }
}

void RelationBlock::addLocalRelationshipType(char *value, LabelIndexManager* labelIndexManager, size_t edgeIndex) {
    relation_block_logger.debug("Attempting to add local relationship type: " + std::string(value) +
                               " at address " + std::to_string(this->addr));
    if (this->type == DEFAULT_TYPE) {
        relation_block_logger.debug("Current type is DEFAULT_TYPE, updating to: " + std::string(value));
        this->updateLocalRelationshipType(RelationBlock::LOCAL_RELATIONSHIP_TYPE_OFFSET, value);
        labelIndexManager->setLabel(labelIndexManager->getOrCreateLabelID(value),  edgeIndex);

    } else {
        relation_block_logger.info("Relation type is already set to " + this->type);
    }
}

void RelationBlock::addCentralRelationshipType(char *value , LabelIndexManager* labelIndexManager, size_t edgeIndex) {
    if (this->type == DEFAULT_TYPE) {
        this->updateCentralRelationshipType(RelationBlock::CENTRAL_RELATIONSHIP_TYPE_OFFSET,
                                          value);
        labelIndexManager->setLabel(labelIndexManager->getOrCreateLabelID(value),  edgeIndex);
    } else {
        relation_block_logger.info("Relation type is already set to " + this->type);
    }
}

std::string RelationBlock::getLocalRelationshipType() {
    int address = this->addr;
    RelationBlock::relationsDB->seekg(address +
        RelationBlock::LOCAL_RELATIONSHIP_TYPE_OFFSET * RelationBlock::RECORD_SIZE);
    char type[RelationBlock::MAX_TYPE_SIZE] = {0};
    if (!RelationBlock::relationsDB->read(reinterpret_cast<char*>(&type), RelationBlock::MAX_TYPE_SIZE)) {
        relation_block_logger.error("Error while reading local relation type from " +
                                    std::to_string(address));
        return "";
    }
    std::string relationType(type);
    return relationType;
}

std::string RelationBlock::getCentralRelationshipType() {
    int address = this->addr;
    RelationBlock::centralRelationsDB->seekg(address +
        RelationBlock::CENTRAL_RELATIONSHIP_TYPE_OFFSET * RelationBlock::RECORD_SIZE);
    char type[RelationBlock::MAX_TYPE_SIZE] = {0};
    if (!RelationBlock::centralRelationsDB->read(reinterpret_cast<char*>(&type), RelationBlock::MAX_TYPE_SIZE)) {
        relation_block_logger.error("Error while reading local relation type from " +
                                    std::to_string(address));
        return "";
    }
    std::string relationType(type);
    return relationType;
}


PropertyEdgeLink* RelationBlock::getPropertyHead() { return PropertyEdgeLink::get(this->propertyAddress); }

MetaPropertyEdgeLink *RelationBlock::getMetaPropertyHead() {
    return MetaPropertyEdgeLink::get(this->metaPropertyAddress);
}

std::map<std::string, char*> RelationBlock::getAllProperties() {
    std::map<std::string, char*> allProperties;
    PropertyEdgeLink* current = this->getPropertyHead();
    while (current) {
        // don't forget to free the allocated memory after using this method
        char* copiedValue = new char[PropertyEdgeLink::MAX_VALUE_SIZE];
        std::strncpy(copiedValue, current->value, PropertyEdgeLink::MAX_VALUE_SIZE);
        allProperties.insert({current->name, copiedValue});
        PropertyEdgeLink* temp = current->next();
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
NodeBlock* RelationBlock::getSource() {
    if (this->sourceBlock) {
        return sourceBlock;
    }
    relation_block_logger.warn("Get source from node block address is not implemented yet!");
    return NULL;
}

/**
 * Get the destination node in the relationship
 *
 * */
NodeBlock* RelationBlock::getDestination() {
    if (this->destinationBlock) {
        return destinationBlock;
    }
    relation_block_logger.warn("Get destination from node block address is not implemented yet!");
    return NULL;
}

thread_local const unsigned long RelationBlock::BLOCK_SIZE = RelationBlock::RECORD_SIZE *
        RelationBlock::NUMBER_OF_LOCAL_RELATION_RECORDS + RelationBlock::MAX_TYPE_SIZE;
thread_local const unsigned long RelationBlock::CENTRAL_BLOCK_SIZE = RelationBlock::RECORD_SIZE *
        RelationBlock::NUMBER_OF_CENTRAL_RELATION_RECORDS + RelationBlock::MAX_TYPE_SIZE;
const std::string RelationBlock::DEFAULT_TYPE = "DEFAULT";

// One relation block holds 11 recods such as source addres, destination address, source next relation address etc.
// and one record is typically 4 bytes (size of unsigned int)
thread_local std::fstream* RelationBlock::relationsDB = NULL;
thread_local std::fstream* RelationBlock::centralRelationsDB = NULL;
