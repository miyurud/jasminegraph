#include <cstring>
#include <fstream>
#include <iostream>  // for cout
#include <set>
#include <string>

#include "PropertyLink.h"

#ifndef NODE_BLOCK
#define NODE_BLOCK

class NodeBlock {
   private:
    unsigned int addr;

   public:
    std::string id;  // Node ID for this block ie: citation paper ID, Facebook accout ID, Twitter account ID etc
    char usage;      // Wheather this block is in use or not
    char label[6] = {
        0};  // Initialize with null chars label === ID if length(id) < 6 else ID will be store as a Node's property
    unsigned int edgeRef;  // edges database block address for relations
    unsigned int propRef;  // Properties DB block address for node properties
    PropertyLink properties;

    static const unsigned long BLOCK_SIZE;  // Size of a node block in bytes
    static std::fstream *nodesDB;

    /**
     * This constructor is used when creating a node for very first time.
     * Where user don't have properties DB address or edge DB addresses
     *
     **/
    NodeBlock(std::string id, unsigned int address) {
        this->id = id;
        this->addr = address;
        this->usage = true;
        this->edgeRef = 0;
        this->propRef = 0;
    };

    NodeBlock(std::string id, unsigned int address, unsigned int edgeRef, unsigned int propRef, char label[],
              bool usage)
        : id(id), addr(address), usage(usage), edgeRef(edgeRef), properties(propRef), propRef(propRef) {
        strcpy(this->label, label);
    };

    void save();
    std::string getLabel();
    bool isInUse();
    std::set<int> getEdges();
    std::set<int> getProps();
    int getFlags();
    void addProperty(std::string, char *);
};

#endif