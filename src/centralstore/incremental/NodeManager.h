#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include "NodeBlock.h"

#ifndef NODE_MANAGER
#define NODE_MANAGER

class NodeManager {
   private:
    unsigned int nextNodeIndex = 0;
    std::fstream *nodeDBT;

    int dbSize(std::string path);
    void persistNodeIndex();
    std::unordered_map<std::string, unsigned int> readNodeIndex();
    static const unsigned long INDEX_KEY_SIZE;  // Size of a index key entry in bytes
    static std::string NODE_DB_PATH;  // Size of a index key entry in bytes

   public:
    static unsigned int nextPropertyIndex; // Next available property block index // unless open in wipe data mode(trunc) need to set this value to property db seekp()/BLOCK_SIZE
    std::string index_db_loc = "/mnt/wd_ubuntu_data_mnt/research/jasminegraph/streamStore/nodes.index.db";

    std::unordered_map<std::string, unsigned int> nodeIndex;

    NodeManager(std::string);
    ~NodeManager() {
        delete NodeBlock::nodesDB;
    };

    void addEdge(std::pair<int, int>);
    void close();
    unsigned int addNode(std::string); // will redurn DB block address
    NodeBlock* get(std::string);

};

#endif