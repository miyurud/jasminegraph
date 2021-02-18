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
#include <list>
#include <map>
#include <string>

#include "PropertyLink.h"

class RelationBlock;  // Forward declaration

#ifndef NODE_BLOCK
#define NODE_BLOCK

class NodeBlock {
   private:
    bool isDirected = false;

   public:
    unsigned int addr = 0;
    std::string id = "";  // Node ID for this block ie: citation paper ID, Facebook accout ID, Twitter account ID etc
    char usage = false;   // Whether this block is in use or not
    unsigned int edgeRef = 0;  // edges database block address for relations
    unsigned int propRef = 0;  // Properties DB block address for node properties

    static const unsigned long BLOCK_SIZE = 15;  // Size of a node block in bytes
    static const unsigned int LABEL_SIZE = 6;    // Size of a node label in bytes
    char label[LABEL_SIZE] = {
        0};  // Initialize with null chars label === ID if length(id) < 6 else ID will be stored as a Node's property

    static std::fstream *nodesDB;

    /**
     * This constructor is used when creating a node for very first time.
     * Where user don't have properties DB address or edge DB addresses
     *
     **/
    NodeBlock(std::string newId, unsigned int address) {
        id = newId;
        addr = address;
        usage = true;
    };

    NodeBlock(std::string id, unsigned int address, unsigned int propRef, unsigned int edgeRef, char _label[],
              bool usage);
    bool updateRelation(RelationBlock *, bool relocateHead = true);
    void save();
    std::string getLabel();
    bool isInUse();
    std::map<std::string, char *> getProperty(std::string);
    PropertyLink *getPropertyHead();
    std::map<std::string, char *> getAllProperties();
    static NodeBlock *get(unsigned int);
    int getFlags();
    void addProperty(std::string, char *);
    RelationBlock *getRelationHead();
    std::list<NodeBlock> getEdges();
    bool setRelationHead(RelationBlock);
    RelationBlock *searchRelation(NodeBlock);
};

#endif