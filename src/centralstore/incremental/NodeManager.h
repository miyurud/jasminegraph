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

#include <fstream>
#include <string>
#include <unordered_map>

#include "NodeBlock.h"

#ifndef NODE_MANAGER
#define NODE_MANAGER

struct GraphConfig {
    unsigned long maxLabelSize;
    unsigned int graphID = 0;
    unsigned int partitionID = 0;
    std::string openMode;
};

class NodeManager {
   private:
    unsigned int nextNodeIndex = 0;
    std::fstream* nodeDBT;
    unsigned int graphID = 0;
    unsigned int partitionID = 0;
    static const std::string FILE_MODE;
    unsigned long INDEX_KEY_SIZE = 5;  // Size of an index key entry in bytes

    int dbSize(std::string path);
    void persistNodeIndex();
    std::unordered_map<std::string, unsigned int> readNodeIndex();
    static std::string NODE_DB_PATH;  // Node database file path
                                      // TODO(tmkasun): This NODE_DB_PATH should be moved to NodeBlock header definition

   public:
    static unsigned int nextPropertyIndex;  // Next available property block index
    // unless open in wipe data
    // mode(trunc) need to set this value to property db seekp()/BLOCK_SIZE
    std::string index_db_loc;

    std::unordered_map<std::string, unsigned int> nodeIndex;

    NodeManager(GraphConfig);
    ~NodeManager() { delete NodeBlock::nodesDB; };
    void setIndexKeySize(unsigned long);
    RelationBlock* addEdge(std::pair<std::string, std::string>);
    RelationBlock* addRelation(NodeBlock, NodeBlock);
    void close();
    NodeBlock* addNode(std::string);  // will redurn DB block address
    NodeBlock* get(std::string);
    std::list<NodeBlock> getGraph(int limit = 10);
};

#endif