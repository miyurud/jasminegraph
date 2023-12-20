//
// Created by sandaruwan on 5/3/23.
//

#include <cstring>
#include <fstream>
#include <set>
#include <string>

#include "CentralNodeBlock.h"
#include "CentralPropertyEdgeLink.h"


#ifndef JASMINEGRAPH_CENTRALRELATIONBLOCK_H
#define JASMINEGRAPH_CENTRALRELATIONBLOCK_H

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

class CentralRelationBlock {
private:
    std::string id;
    bool updateRelationRecords(RelationOffsets, unsigned int);
    CentralNodeBlock *sourceBlock;
    CentralNodeBlock *destinationBlock;

public:
    CentralRelationBlock(CentralNodeBlock source, CentralNodeBlock destination){
        this->sourceBlock = &source;
        this->destinationBlock = &destination;
    }


    CentralRelationBlock(unsigned int addr, NodeRelation source, NodeRelation destination, unsigned int propertyAddress)
            : addr(addr), source(source), destination(destination), propertyAddress(propertyAddress){};


    char usage;
    unsigned int addr =0;  // Block size * block ID for this block
    NodeRelation source;
    NodeRelation destination;
    unsigned int propertyAddress =0;
    CentralPropertyEdgeLink *propertyHead = NULL;

    void save(std::fstream *cursor);
    bool isInUse();
    int getFlags();

    bool setNextSource(unsigned int);
    bool setNextDestination(unsigned int);
    bool setPreviousSource(unsigned int);
    bool setPreviousDestination(unsigned int);

    CentralNodeBlock *getSource();
    CentralNodeBlock *getDestination();
    void setSource(CentralNodeBlock *src) { sourceBlock = src; };
    void setDestination(CentralNodeBlock *dst) { destinationBlock = dst; };

    CentralRelationBlock *nextSource();
    CentralRelationBlock *previousSource();
    CentralRelationBlock *nextDestination();
    CentralRelationBlock *previousDestination();

    CentralRelationBlock *add(CentralNodeBlock, CentralNodeBlock);
    static CentralRelationBlock *get(unsigned int);
    void addProperty(std::string, char *);
    CentralPropertyEdgeLink *getPropertyHead();
    std::map<std::string, char *> getAllProperties();

    static unsigned int nextRelationIndex;
    static unsigned int nextCentralRelationIndex;
    static const unsigned long BLOCK_SIZE;  // Size of a relation record block in bytes
    static std::string DB_PATH;
    static std::fstream *centralRelationsDB;
    static const int RECORD_SIZE = sizeof(unsigned int);




};



#endif //JASMINEGRAPH_CENTRALRELATIONBLOCK_H
