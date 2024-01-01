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
#include <unordered_set>

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
    unsigned long INDEX_KEY_SIZE = 6;  // Size of an index key entry in bytes

    void persistNodeIndex();
    std::unordered_map<std::string, unsigned int> readNodeIndex();
    void addNodeIndex(std::string nodeId, unsigned int nodeIndex);
    static std::string NODE_DB_PATH;  // Node database file path
                                      // TODO(tmkasun): This NODE_DB_PATH should be moved to NodeBlock header definition

 public:
    static unsigned int nextPropertyIndex;  // Next available property block index
    // unless open in wipe data
    // mode(trunc) need to set this value to property db seekp()/BLOCK_SIZE
    std::string indexDBPath;

    std::unordered_map<std::string, unsigned int> nodeIndex;
    std::string dbPrefix;

    NodeManager(GraphConfig);
    ~NodeManager() { delete NodeBlock::nodesDB; };
    void setIndexKeySize(unsigned long);
    RelationBlock* addEdge(std::pair<std::string, std::string>);
    RelationBlock* addRelation(NodeBlock, NodeBlock);
    static int dbSize(std::string path);
    void close();
    NodeBlock* addNode(std::string);  // will redurn DB block address
    NodeBlock* get(std::string);
    std::list<NodeBlock> getLimitedGraph(int limit = 10);
    std::list<NodeBlock*> getGraph();
    std::list<NodeBlock*> getCentralGraph();

    RelationBlock* addCentralRelation(NodeBlock source, NodeBlock destination);

    RelationBlock* addCentralEdge(std::pair<std::string, std::string> edge);
    std::map<long, std::unordered_set<long>> getAdjacencyList();
    std::map<long, long> getDistributionMap ();

    int getGraphID();

    int getPartitionID();
};

#endif
