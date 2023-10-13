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

#ifndef RELATION_BLOCK
#define RELATION_BLOCK
struct NodeRelation {
    unsigned int address = 0;
    unsigned int nextRelationId = 0;
    unsigned int nextPid = 0;
    unsigned int preRelationId = 0;
    unsigned int prePid = 0;
};

enum class RelationOffsets : int {
    SOURCE_ID = 0,
    DESTINATION_ID =1,
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
    bool updateRelationRecords(RelationOffsets, unsigned int);
    NodeBlock *sourceBlock;
    NodeBlock *destinationBlock;

   public:
    RelationBlock(NodeBlock source, NodeBlock destination){
        this->sourceBlock = &source;
        this->destinationBlock = &destination;
    }


    RelationBlock(unsigned int addr, NodeRelation source, NodeRelation destination, unsigned int propertyAddress)
        : addr(addr), source(source), destination(destination), propertyAddress(propertyAddress){};


    char usage;
    unsigned int addr =0;  // Block size * block ID for this block
    NodeRelation source;
    NodeRelation destination;
    unsigned int propertyAddress =0;
    PropertyEdgeLink *propertyHead = NULL;

    void save(std::fstream *cursor);
    bool isInUse();
    int getFlags();

    bool setNextSource(unsigned int);
    bool setNextDestination(unsigned int);
    bool setPreviousSource(unsigned int);
    bool setPreviousDestination(unsigned int);

    NodeBlock *getSource();
    NodeBlock *getDestination();
    void setSource(NodeBlock *src) { sourceBlock = src; };
    void setDestination(NodeBlock *dst) { destinationBlock = dst; };

    RelationBlock *nextSource();
    RelationBlock *previousSource();
    RelationBlock *nextDestination();
    RelationBlock *previousDestination();

    RelationBlock *add(NodeBlock, NodeBlock);
    static RelationBlock *get(unsigned int);
    void addProperty(std::string, char *);
    PropertyEdgeLink *getPropertyHead();
    std::map<std::string, char *> getAllProperties();

    static unsigned int nextRelationIndex;
    static unsigned int nextCentralRelationIndex;
    static const unsigned long BLOCK_SIZE;  // Size of a relation record block in bytes
    static std::string DB_PATH;
    static std::fstream *relationsDB;
    static std::fstream *centralrelationsDB;
    static const int RECORD_SIZE = sizeof(unsigned int);

    static RelationBlock *addCentral(NodeBlock source, NodeBlock destination);
    static RelationBlock *getCentral(unsigned int address);

    void addCentralProperty(std::string name, char *value);

    bool updateCentralRelationRecords(RelationOffsets recordOffset, unsigned int data);

    bool setCentralPreviousSource(unsigned int newAddress);

    bool setCentralPreviousDestination(unsigned int newAddress);

    bool setCentralNextSource(unsigned int newAddress);

    bool setCentralNextDestination(unsigned int newAddress);

    RelationBlock *nextCentralSource();

    RelationBlock *nextCentralDestination();
};

#endif