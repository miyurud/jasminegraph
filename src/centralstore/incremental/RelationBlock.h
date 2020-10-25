#include <cstring>
#include <fstream>
#include <iostream>  // for cout
#include <set>
#include <string>

#include "NodeBlock.h"
#include "PropertyLink.h"

#ifndef RELATION_BLOCK
#define RELATION_BLOCK
struct NodeRelation {
    unsigned int address = 0;
    unsigned int nextRelationId = 0;
    unsigned int nextPid = 0;
    unsigned int preRelationId = 0;
    unsigned int prePid = 0;
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
    bool updateRelationRecords(int recordType, unsigned int data);

   public:
    RelationBlock(unsigned int addr, NodeRelation source, NodeRelation destination, unsigned int propertyAddress)
        : addr(addr), source(source), destination(destination), propertyAddress(propertyAddress){};

    char usage;
    unsigned int addr;  // Block size * block ID ID for this block
    NodeRelation source;
    NodeRelation destination;
    unsigned int propertyAddress;
    PropertyLink *propertyHead = NULL;

    void save(std::fstream *cursor);
    bool isInUse();
    int getFlags();

    static RelationBlock *add(NodeBlock, NodeBlock);
    static RelationBlock *get(unsigned int);
    bool updateSourceNextRelAddr(unsigned int);
    bool updateDestinationNextRelAddr(unsigned int);
    
    static unsigned int nextRelationIndex;
    static const unsigned long BLOCK_SIZE;  // Size of a relation record block in bytes
    static std::string DB_PATH;
    static std::fstream *relationsDB;
    static const int RECORD_SIZE = sizeof(unsigned int);
};

#endif