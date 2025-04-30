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
    unsigned int graphID;
    unsigned int partitionID;
    std::string openMode;
};

class NodeManager {
 private:
    unsigned int nextNodeIndex = 0;
    std::fstream* nodeDBT;
    unsigned int graphID = 0;
    unsigned int partitionID = 0;
    std::string dbPrefix;
    // static const std::string FILE_MODE;
    unsigned long INDEX_KEY_SIZE = 6;  // Size of an index key entry in bytes
    std::string indexDBPath;
    std::unordered_map<std::string, unsigned int> nodeIndex;

    void persistNodeIndex();
    std::unordered_map<std::string, unsigned int> readNodeIndex();
    void addNodeIndex(std::string nodeId, unsigned int nodeIndex);

 public:
    static unsigned int nextPropertyIndex;  // Next available property block index

    NodeManager(GraphConfig);
    ~NodeManager() { delete NodeBlock::nodesDB; };

    void setIndexKeySize(unsigned long);
    static int dbSize(std::string path);
    static const std::string FILE_MODE;
    int getGraphID();
    int getPartitionID();
    std::string getDbPrefix();
    void close();

    RelationBlock* addLocalEdge(std::pair<std::string, std::string>);
    RelationBlock* addCentralEdge(std::pair<std::string, std::string> edge);

    RelationBlock* addLocalRelation(NodeBlock, NodeBlock);
    RelationBlock* addCentralRelation(NodeBlock source, NodeBlock destination);

    NodeBlock* addNode(std::string);  // will return DB block address
    NodeBlock* get(std::string);

    std::list<NodeBlock*> getCentralGraph();
    std::list<NodeBlock> getLimitedGraph(int limit = 10);
    std::list<NodeBlock*> getGraph();

    std::map<long, std::unordered_set<long>> getAdjacencyList();
    std::map<long, std::unordered_set<long>> getAdjacencyList(bool isLocal);
    std::map<long, long> getDistributionMap();
};

#endif
