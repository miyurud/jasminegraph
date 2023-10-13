//
// Created by sandaruwan on 5/3/23.
//

#include <cstring>
#include <fstream>
#include <list>
#include <map>
#include <string>

#include "CentralPropertyLink.h"

#ifndef JASMINEGRAPH_CENTRALNODEBLOCK_H
#define JASMINEGRAPH_CENTRALNODEBLOCK_H

class CentralRelationBlock;  // Forward declaration


class CentralNodeBlock {
private:
    bool isDirected = false;

public:
    unsigned int addr = 0;
    std::string id = "";  // Node ID for this block ie: citation paper ID, Facebook accout ID, Twitter account ID etc
    char usage = false;   // Whether this block is in use or not
    unsigned int nodeId ;      // nodeId for each block
    unsigned int edgeRef = 0;      // edges database block address for relations size of edgeRef is 4 bytes
    unsigned int centralEdgeRef = 0; // edges cut database block address for edge cut relations
    unsigned char edgeRefPID = 0;  // Partition ID of the edge reference
    unsigned int propRef = 0;      // Properties DB block address for node properties

    static const unsigned long BLOCK_SIZE = 24;  // Size of a node block in bytes
    static const unsigned int LABEL_SIZE = 6;    // Size of a node label in bytes
    char label[LABEL_SIZE] = {
            0};  // Initialize with null chars label === ID if length(id) < 6 else ID will be stored as a Node's property

    static std::fstream *centralNodesDB;

    /**
     * This constructor is used when creating a node for very first time.
     * Where user don't have properties DB address or edge DB addresses
     *
     **/
    CentralNodeBlock(std::string newId, unsigned int node, unsigned int address) {
        id = newId;
        nodeId = node;
        addr = address;
        usage = true;
    };

    CentralNodeBlock(std::string id, unsigned int nodeId, unsigned int address, unsigned int propRef, unsigned int edgeRef,unsigned int centralEdgeRef,
              unsigned char edgeRefPID, char _label[], bool usage);
    bool updateRelation(CentralRelationBlock *, bool relocateHead = true);
    void save();
    std::string getLabel();
    bool isInUse();
    std::map<std::string, char *> getProperty(std::string);
    CentralPropertyLink *getPropertyHead();
    std::map<std::string, char *> getAllProperties();
    static CentralNodeBlock *get(unsigned int);
    int getFlags();
    void addProperty(std::string, char *);
    CentralRelationBlock *getRelationHead();
    std::list<CentralNodeBlock> getEdges();
    bool setRelationHead(CentralRelationBlock);
    CentralRelationBlock *searchRelation(CentralNodeBlock);

    bool updateCentralRelation(CentralRelationBlock *newRelation, bool relocateHead  = true);

    CentralRelationBlock *getCentralRelationHead();

    bool setCentralRelationHead(CentralRelationBlock newRelation);

    CentralRelationBlock *searchCentralRelation(CentralNodeBlock withNode);
};


#endif //JASMINEGRAPH_CENTRALNODEBLOCK_H
