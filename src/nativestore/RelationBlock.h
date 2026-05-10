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

#include <cstring>
#include <fstream>
#include <set>
#include <string>

#include "NodeBlock.h"
#include "PropertyEdgeLink.h"
#include "MetaPropertyEdgeLink.h"

#ifndef RELATION_BLOCK
#define RELATION_BLOCK
struct NodeRelation {
    unsigned int address = 0;
    unsigned int nextRelationId = 0;
    unsigned int nextPid = 0;
    unsigned int preRelationId = 0;
    unsigned int prePid = 0;
    unsigned int nodeId = 0;
};

enum class RelationOffsets : int {
    SOURCE_ID = 0,
    DESTINATION_ID = 1,
    SOURCE = 2,
    DESTINATION = 3,
    SOURCE_NEXT = 4,
    SOURCE_NEXT_PID = 5,
    SOURCE_PREVIOUS = 6,
    SOURCE_PREVIOUS_PID = 7,
    DESTINATION_NEXT = 8,
    DESTINATION_NEXT_PID = 9,
    DESTINATION_PREVIOUS = 10,
    DESTINATION_PREVIOUS_PID = 11,
    RELATION_PROPS = 12,
    RELATION_PROPS_META = 13,
};

/**
 * Relation states
 *     Source       Destination
 *       x               -
 *       x               x
 *       -               -
 *       -               x
 *
 * **/

class RelationBlock {
 private:
    std::string id;
    bool updateLocalRelationRecords(RelationOffsets, unsigned int);
    bool updateCentralRelationRecords(RelationOffsets recordOffset, unsigned int data);
    bool updateLocalRelationshipType(int offset, std::string type);
    bool updateCentralRelationshipType(int offset, std::string type);

    NodeBlock *sourceBlock;
    NodeBlock *destinationBlock;

 public:
    RelationBlock(NodeBlock source, NodeBlock destination) {
        this->sourceBlock = &source;
        this->destinationBlock = &destination;
    }

    RelationBlock(unsigned int addr, NodeRelation source, NodeRelation destination, unsigned int propertyAddress,
                  std::string type) : addr(addr), source(source), destination(destination),
                  propertyAddress(propertyAddress), type(type){
        this->sourceBlock = NodeBlock::get(source.address);
        this->destinationBlock = NodeBlock::get(destination.address);
        this->source = source;
        this->destination = destination;
    };

    RelationBlock(unsigned int address, NodeRelation source, NodeRelation destination, unsigned int propertyAddress,
                  unsigned int metaPropertyAddress, std::string type) : addr(address), source(source),
                  destination(destination), propertyAddress(propertyAddress), metaPropertyAddress(metaPropertyAddress),
                  type(type){
        this->sourceBlock = NodeBlock::get(source.address);
        this->destinationBlock = NodeBlock::get(destination.address);
        this->source = source;
        this->destination = destination;
    };

    char usage;
    unsigned int addr = 0;  // Block size * block ID for this block
    NodeRelation source;
    NodeRelation destination;
    unsigned int propertyAddress = 0;
    unsigned int metaPropertyAddress = 0;
    std::string type = DEFAULT_TYPE;
    PropertyEdgeLink *propertyHead = NULL;
    static thread_local unsigned int nextLocalRelationIndex;
    static thread_local unsigned int nextCentralRelationIndex;
    static thread_local const unsigned long BLOCK_SIZE;  // Size of a relation record block in bytes
    static thread_local const unsigned long CENTRAL_BLOCK_SIZE;  // Size of a relation record block in bytes
    static thread_local std::string DB_PATH;
    static thread_local std::fstream *relationsDB;
    static thread_local std::fstream *centralRelationsDB;
    static const int RECORD_SIZE = sizeof(unsigned int);
    static const int MAX_TYPE_SIZE = 18;
    static const std::string DEFAULT_TYPE;
    static const int NUMBER_OF_CENTRAL_RELATION_RECORDS = 14;
    static const int CENTRAL_RELATIONSHIP_TYPE_OFFSET = 14;
    static const int NUMBER_OF_LOCAL_RELATION_RECORDS = 13;
    static const int LOCAL_RELATIONSHIP_TYPE_OFFSET = 13;



    void save(std::fstream *cursor);
    bool isInUse();
    int getFlags();

    bool setLocalNextSource(unsigned int);
    bool setCentralNextSource(unsigned int);

    bool setLocalNextDestination(unsigned int);
    bool setCentralNextDestination(unsigned int);

    bool setLocalPreviousSource(unsigned int);
    bool setCentralPreviousSource(unsigned int);

    bool setLocalPreviousDestination(unsigned int);
    bool setCentralPreviousDestination(unsigned int);

    NodeBlock *getSource();
    NodeBlock *getDestination();
    void setSource(NodeBlock *src) { sourceBlock = src; };
    void setDestination(NodeBlock *dst) { destinationBlock = dst; };


    RelationBlock *previousLocalSource();
    RelationBlock *previousCentralSource();

    RelationBlock *nextLocalSource();
    RelationBlock *nextCentralSource();

    RelationBlock *nextLocalDestination();
    RelationBlock *nextCentralDestination();

    RelationBlock *previousLocalDestination();
    RelationBlock *previousCentralDestination();

    RelationBlock *addLocalRelation(NodeBlock, NodeBlock);
    RelationBlock *addCentralRelation(NodeBlock source, NodeBlock destination);

    static RelationBlock *getLocalRelation(unsigned int);
    static RelationBlock *getCentralRelation(unsigned int address);

    void addLocalProperty(std::string, char *);
    void addCentralProperty(std::string name, char *value);
    void addMetaProperty(std::string name, char *value);
    void addLocalRelationshipType(char *value);
    void addCentralRelationshipType(char *value);

    std::string getLocalRelationshipType();
    std::string getCentralRelationshipType();
    PropertyEdgeLink *getPropertyHead();
    MetaPropertyEdgeLink *getMetaPropertyHead();
    std::map<std::string, std::string, std::less<>> getAllProperties();
};

#endif
