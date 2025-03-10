/**
Copyright 2020-2024 JasmineGraph Team
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

#include "NodeManager.h"

#include <sys/stat.h>

#include <exception>
#include <mutex>

#include "../util/Utils.h"
#include "../util/logger/Logger.h"
#include "NodeBlock.h"  // To setup node DB
#include "PropertyEdgeLink.h"
#include "PropertyLink.h"
#include "MetaPropertyLink.h"
#include "RelationBlock.h"
#include "iostream"
#include <sys/stat.h>
#include <thread>

Logger node_manager_logger;
pthread_mutex_t lockEdgeAdd;

NodeManager::NodeManager(GraphConfig gConfig) {
    this->graphID = gConfig.graphID;
    this->partitionID = gConfig.partitionID;
    Utils utils;

    std::string instanceDataFolderLocation =
        utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    std::string graphPrefix = instanceDataFolderLocation + "/g" + std::to_string(graphID);
    dbPrefix = graphPrefix + "_p" + std::to_string(partitionID);
    std::string nodesDBPath = dbPrefix + "_nodes.db";
    indexDBPath = dbPrefix + "_nodes.index.db";
    std::string propertiesDBPath = dbPrefix + "_properties.db";
    std::string metaPropertiesDBPath = dbPrefix + "_meta_properties.db";
    std::string edgePropertiesDBPath = dbPrefix + "_edge_properties.db";
    std::string relationsDBPath = dbPrefix + "_relations.db";
    std::string centralRelationsDBPath = dbPrefix + "_central_relations.db";
    // This needs to be set in order to prevent index DB key overflows
    // Expected maximum length of a key in the dataset

    node_manager_logger.info("Derived nodesDBPath: " + nodesDBPath);
    node_manager_logger.info("Derived index_db_loc: " + indexDBPath);

    if (gConfig.maxLabelSize) {
        setIndexKeySize(gConfig.maxLabelSize);
    }

    if (gConfig.maxLabelSize) {
        node_manager_logger.info("Setting index key size to: " + std::to_string(gConfig.maxLabelSize));
    }

    std::ios_base::openmode openMode = std::ios::in | std::ios::out;  // Default mode
    if (gConfig.openMode == NodeManager::FILE_MODE) {
        this->nodeIndex = readNodeIndex();
    } else {
        openMode |= std::ios::trunc;
    }

    if (gConfig.openMode == NodeManager::FILE_MODE) {
        node_manager_logger.info("Using APPEND mode for file operations.");
    } else {
        node_manager_logger.info("Using TRUNC mode for file operations.");
    }

    NodeBlock::nodesDB = Utils::openFile(nodesDBPath, openMode);
    PropertyLink::propertiesDB = Utils::openFile(propertiesDBPath, openMode);
    MetaPropertyLink::metaPropertiesDB = Utils::openFile(metaPropertiesDBPath, openMode);
    PropertyEdgeLink::edgePropertiesDB = Utils::openFile(edgePropertiesDBPath, openMode);
    RelationBlock::relationsDB = utils.openFile(relationsDBPath, openMode);
    RelationBlock::centralRelationsDB = Utils::openFile(centralRelationsDBPath, openMode);

    //    RelationBlock::centralpropertiesDB =
    //            new std::fstream(dbPrefix + "_central_relations.db", std::ios::in | std::ios::out | openMode |
    //            std::ios::binary);
    // TODO (tmkasun): set PropertyLink nextPropertyIndex after validating by modulus check from file number of bytes

    node_manager_logger.info("NodesDB, PropertiesDB, and RelationsDB files opened (or created) successfully.");
    //    unsigned int nextAddress;
    //    unsigned int propertyBlockAddress = 0;
    //    PropertyLink::propertiesDB->seekg(propertyBlockAddress * PropertyLink::PROPERTY_BLOCK_SIZE);
    //    if (PropertyLink::propertiesDB && PropertyLink::propertiesDB->is_open()) {
    //        PropertyLink::propertiesDB->seekg(propertyBlockAddress * PropertyLink::PROPERTY_BLOCK_SIZE);
    //        if (!PropertyLink::propertiesDB->read(reinterpret_cast<char*>(&(nextAddress)), sizeof(unsigned int))) {
    //            node_manager_logger.error("Error while reading node property next address from block = " +
    //            std::to_string(propertyBlockAddress));
    //        }
    //    } else {
    //        node_manager_logger.error("Failed to open propertiesDB file.");
    //    }
    //
    //    node_manager_logger.log("Checking NextAddress : " + nextAddress, "info");

    if (dbSize(nodesDBPath) % NodeBlock::BLOCK_SIZE != 0) {
        node_manager_logger.warn("NodesDB size: " + std::to_string(dbSize(nodesDBPath)) +
                                 ", NodeBlock::BLOCK_SIZE: " + std::to_string(NodeBlock::BLOCK_SIZE));
        node_manager_logger.error("RelationsDB size does not comply to node block size Path = " + relationsDBPath);
    }
    if (dbSize(relationsDBPath) % RelationBlock::BLOCK_SIZE != 0) {
        node_manager_logger.warn("RelationsDB size: " + std::to_string(dbSize(relationsDBPath)) +
                                ", RelationBlock::BLOCK_SIZE: " + std::to_string(RelationBlock::BLOCK_SIZE));
        node_manager_logger.error("RelationsDB size does not comply to node block size Path = " + relationsDBPath);
    }
    if (dbSize(centralRelationsDBPath) % RelationBlock::BLOCK_SIZE != 0) {
        node_manager_logger.warn("CentralRelationsDB size: " + std::to_string(dbSize(centralRelationsDBPath)) +
                                ", RelationBlock::BLOCK_SIZE: " + std::to_string(RelationBlock::BLOCK_SIZE));
        node_manager_logger.error("CentralRelationsDB size does not comply to node block size Path = " +
                                                            centralRelationsDBPath);
    }

    struct stat stat_buf;

    if (stat(propertiesDBPath.c_str(), &stat_buf) == 0) {
        PropertyLink::nextPropertyIndex = (stat_buf.st_size / PropertyLink::PROPERTY_BLOCK_SIZE) == 0 ? 1 :
                (stat_buf.st_size / PropertyLink::PROPERTY_BLOCK_SIZE);

    } else {
        node_manager_logger.error("Error getting file size for: " + propertiesDBPath);
    }

    if (stat(metaPropertiesDBPath.c_str(), &stat_buf) == 0) {
        MetaPropertyLink::nextPropertyIndex = (stat_buf.st_size / MetaPropertyLink::META_PROPERTY_BLOCK_SIZE) == 0 ? 1 :
                                          (stat_buf.st_size / MetaPropertyLink::META_PROPERTY_BLOCK_SIZE);

    } else {
        node_manager_logger.error("Error getting file size for: " + metaPropertiesDBPath);
    }

    if (stat(edgePropertiesDBPath.c_str(), &stat_buf) == 0) {
        PropertyEdgeLink::nextPropertyIndex = (stat_buf.st_size / PropertyEdgeLink::PROPERTY_BLOCK_SIZE) == 0 ? 1 :
                                            (stat_buf.st_size / PropertyEdgeLink::PROPERTY_BLOCK_SIZE);
    } else {
        node_manager_logger.error("Error getting file size for: " + edgePropertiesDBPath);
    }

    if (stat(relationsDBPath.c_str(), &stat_buf) == 0) {
        RelationBlock::nextLocalRelationIndex = (stat_buf.st_size / RelationBlock::BLOCK_SIZE) == 0 ? 1 :
                                        (stat_buf.st_size / RelationBlock::BLOCK_SIZE);
    } else {
        node_manager_logger.error("Error getting file size for: " + relationsDBPath);
    }

    if (stat(centralRelationsDBPath.c_str(), &stat_buf) == 0) {
        RelationBlock::nextCentralRelationIndex = (stat_buf.st_size / RelationBlock::BLOCK_SIZE)== 0 ? 1 :
                                                (stat_buf.st_size / RelationBlock::BLOCK_SIZE);
    } else {
        node_manager_logger.error("Error getting file size for: " + centralRelationsDBPath);
    }
    node_manager_logger.info("Node Manager Execution Completed!");
}

std::unordered_map<std::string, unsigned int> NodeManager::readNodeIndex() {
    std::ifstream index_db(indexDBPath, std::ios::app | std::ios::binary);
    std::unordered_map<std::string, unsigned int> _nodeIndex;  // temporary node index data holder
    if (index_db.is_open()) {
        int iSize = dbSize(indexDBPath);
        unsigned long dataWidth = NodeManager::INDEX_KEY_SIZE + sizeof(unsigned int);
        if (iSize % dataWidth != 0) {
            node_manager_logger.error("Index DB size does not comply to index block size Path = " + indexDBPath);
            node_manager_logger.error("Node index DB in " + indexDBPath + " is corrupted!");
        }
        NodeManager::nextNodeIndex = iSize/dataWidth;
        char nodeIDC[NodeManager::INDEX_KEY_SIZE];
        bzero(nodeIDC, NodeManager::INDEX_KEY_SIZE);  // Fill with null chars before putting data
        unsigned int nodeIndexId;
        for (size_t i = 0; i < iSize / dataWidth; i++) {
            if (!index_db.read(&nodeIDC[0], NodeManager::INDEX_KEY_SIZE)) {
                node_manager_logger.error("Error while reading index key data from block i = " + std::to_string(i));
            }
            if (!index_db.read(reinterpret_cast<char *>(&nodeIndexId), sizeof(unsigned int))) {
                node_manager_logger.error("Error while reading index ID data from block i = " + std::to_string(i));
            }
            _nodeIndex[std::string(nodeIDC)] = nodeIndexId;
        }
    } else {
        std::string errorMessage = "Error while opening the node index DB";
        node_manager_logger.error(errorMessage);
    }

    index_db.close();
    return _nodeIndex;
}

RelationBlock *NodeManager::addLocalRelation(NodeBlock source, NodeBlock destination) {
    RelationBlock *newRelation = NULL;
    if (source.edgeRef == 0 || destination.edgeRef == 0 ||
        !source.searchLocalRelation(destination)) {  // certainly a new relation block needed
        RelationBlock *relationBlock = new RelationBlock(source, destination);
        newRelation = relationBlock->addLocalRelation(source, destination);
        if (newRelation) {
            source.updateLocalRelation(newRelation, true);
            destination.updateLocalRelation(newRelation, true);
        } else {
            node_manager_logger.error("Error while adding the new edge/relation for source = " +
                                      std::string(source.id) + " destination = " + std::string(destination.id));
        }
    }
    //    else {
    //        // TODO[tmkasun]: implement get edge support and return existing edge/relation if already exist
    //        node_manager_logger.warn("Relation/Edge already exist for source = " + std::string(source.id) +
    //                                 " destination = " + std::string(destination.id));
    ////        newRelation = source.searchRelation(destination);
    //    }
    return newRelation;
}

RelationBlock *NodeManager::addCentralRelation(NodeBlock source, NodeBlock destination) {
    RelationBlock *newRelation = NULL;
    if (source.centralEdgeRef == 0 || destination.centralEdgeRef == 0 ||
        !source.searchCentralRelation(destination)) {  // certainly a new relation block needed
        RelationBlock *relationBlock = new RelationBlock(source, destination);
        newRelation = relationBlock->addCentralRelation(source, destination);
        if (newRelation) {
            source.updateCentralRelation(newRelation, true);
            destination.updateCentralRelation(newRelation, true);
        } else {
            node_manager_logger.error("Error while adding the new edge/relation for source = " +
                                      std::string(source.id) + " destination = " + std::string(destination.id));
        }
    }
    //    else {
    //        // TODO[tmkasun]: implement get edge support and return existing edge/relation if already exist
    //        node_manager_logger.warn("Central Relation/Edge already exist for source = " + std::string(source.id) +
    //                                 " destination = " + std::string(destination.id));
    ////        newRelation = source.searchCentralRelation(destination);
    //    }
    return newRelation;
}

NodeBlock *NodeManager::addNode(std::string nodeId) {
    unsigned int assignedNodeIndex;
    node_manager_logger.debug("Adding node index " + std::to_string(this->nextNodeIndex));
    if (this->nodeIndex.find(nodeId) == this->nodeIndex.end()) {
        node_manager_logger.debug("Can't find NodeId (" + nodeId + ") in the index database");
        unsigned int vertexId = std::stoul(nodeId);
        NodeBlock *sourceBlk = new NodeBlock(nodeId, vertexId, this->nextNodeIndex * NodeBlock::BLOCK_SIZE);
        this->addNodeIndex(nodeId, this->nextNodeIndex);
        assignedNodeIndex = this->nextNodeIndex;
        this->nextNodeIndex++;
        sourceBlk->setLabel(nodeId.c_str());
        sourceBlk->save();
        return sourceBlk;
    }
    node_manager_logger.debug("NodeId found in index for node ID " + nodeId);
    return this->get(nodeId);
}

RelationBlock *NodeManager::addLocalEdge(std::pair<std::string, std::string> edge) {
    pthread_mutex_lock(&lockEdgeAdd);

    NodeBlock *sourceNode = this->addNode(edge.first);
    NodeBlock *destNode = this->addNode(edge.second);
    RelationBlock *newRelation = this->addLocalRelation(*sourceNode, *destNode);
    if (newRelation) {
        newRelation->setDestination(destNode);
        newRelation->setSource(sourceNode);
    }
    pthread_mutex_unlock(&lockEdgeAdd);

    node_manager_logger.debug("DEBUG: Source DB block address " + std::to_string(sourceNode->addr) +
                              " Destination DB block address " + std::to_string(destNode->addr));
    return newRelation;
}

RelationBlock *NodeManager::addCentralEdge(std::pair<std::string, std::string> edge) {
    //    std::unique_lock<std::mutex> guard1(lockCentralEdgeAdd);
    //
    //    guard1.lock();
    pthread_mutex_lock(&lockEdgeAdd);

    NodeBlock *sourceNode = this->addNode(edge.first);
    NodeBlock *destNode = this->addNode(edge.second);
    RelationBlock *newRelation = this->addCentralRelation(*sourceNode, *destNode);
    if (newRelation) {
        newRelation->setDestination(destNode);
        newRelation->setSource(sourceNode);
    }
    pthread_mutex_unlock(&lockEdgeAdd);

    //    guard1.unlock();
    node_manager_logger.debug("DEBUG: Source DB block address " + std::to_string(sourceNode->addr) +
                              " Destination DB block address " + std::to_string(destNode->addr));
    return newRelation;
}

void NodeManager::addNodeIndex(std::string nodeId, unsigned int nodeIndex) {
    this->nodeIndex.insert({nodeId, this->nextNodeIndex});

    std::ofstream index_db(indexDBPath, std::ios::app | std::ios::binary);
    if (index_db.is_open()) {
        char nodeIDC[NodeManager::INDEX_KEY_SIZE] = {0};  // Initialize with null chars
        std::strcpy(nodeIDC, nodeId.c_str());
        index_db.write(nodeIDC, sizeof(nodeIDC));
        index_db.write(reinterpret_cast<char *>(&nodeIndex), sizeof(unsigned int));
        index_db.flush();
        node_manager_logger.debug("Writing node index --> Node key = " +
                                  std::string(nodeIDC) + ", value = " + std::to_string(nodeIndex));
    } else {
        node_manager_logger.error("Failed to open index database file.");
    }
    index_db.close();
}

int NodeManager::dbSize(std::string path) {
    /*
        The structure stat contains at least the following members:
        st_dev     ID of the device containing file
        st_ino     file serial number
        st_mode    mode of file (see below)
        st_nlink   number of links to the file
        st_uid     user ID of file
        st_gid     group ID of file
        st_rdev    device ID (if file is character or block special)
        st_size    file size in bytes (if file is a regular file)
        st_atime   time of last access
        st_mtime   time of last data modification
        st_ctime   time of last status change
        st_blksize a filesystem-specific preferred I/O block size for
                        this object.  In some filesystem types, this may
                        vary from file to file
        st_blocks  number of blocks allocated for this object
    */
    struct stat result;
    if (stat(path.c_str(), &result) == 0) {
        node_manager_logger.debug("Size of the " + path + " is " + std::to_string(result.st_size));
    } else {
        node_manager_logger.error("Error while reading file stats in " + path);
        return -1;
    }

    return result.st_size;
}

/**
 * @Deprecated use NodeBlock.get() instead
 **/
NodeBlock *NodeManager::get(std::string nodeId) {
    NodeBlock *nodeBlockPointer = NULL;
    if (this->nodeIndex.find(nodeId) == this->nodeIndex.end()) {  // Not found
        return nodeBlockPointer;
    }
    unsigned int nodeIndex = this->nodeIndex[nodeId];
    const unsigned int blockAddress = nodeIndex * NodeBlock::BLOCK_SIZE;
    NodeBlock::nodesDB->seekg(blockAddress);
    unsigned int vertexId;
    unsigned int edgeRef;
    unsigned int centralEdgeRef;
    unsigned char edgeRefPID;
    unsigned int propRef;
    unsigned int metaPropRef;
    char usageBlock;
    char label[NodeBlock::LABEL_SIZE];

    if (!NodeBlock::nodesDB->read(reinterpret_cast<char *>(&usageBlock), sizeof(unsigned char))) {
        node_manager_logger.error("Error while reading usage data from block " + std::to_string(blockAddress));
    }
    if (!NodeBlock::nodesDB->read(reinterpret_cast<char *>(&vertexId), sizeof(unsigned int))) {
        node_manager_logger.error("Error while reading nodeId  data from block " + std::to_string(blockAddress));
    }
    if (!NodeBlock::nodesDB->read(reinterpret_cast<char *>(&edgeRef), sizeof(unsigned int))) {
        node_manager_logger.error("Error while reading edge reference data from block " + std::to_string(blockAddress));
    }

    if (!NodeBlock::nodesDB->read(reinterpret_cast<char *>(&centralEdgeRef), sizeof(unsigned int))) {
        node_manager_logger.error("Error while reading central edge reference data from block " +
                                  std::to_string(blockAddress));
    }

    if (!NodeBlock::nodesDB->read(reinterpret_cast<char *>(&edgeRefPID), sizeof(unsigned char))) {
        node_manager_logger.error("Error while reading usage data from block " + std::to_string(blockAddress));
    }

    if (!NodeBlock::nodesDB->read(reinterpret_cast<char *>(&propRef), sizeof(unsigned int))) {
        node_manager_logger.error("Error while reading prop reference data from block " + std::to_string(blockAddress));
    }

    if (!NodeBlock::nodesDB->read(reinterpret_cast<char *>(&metaPropRef), sizeof(unsigned int))) {
        node_manager_logger.error("Error while reading prop reference data from block " + std::to_string(blockAddress));
    }

    if (!NodeBlock::nodesDB->read(&label[0], NodeBlock::LABEL_SIZE)) {
        node_manager_logger.error("Error while reading label data from block " + std::to_string(blockAddress));
    }
    bool usage = usageBlock == '\1';
    node_manager_logger.debug("Label = " + std::string(label));
    node_manager_logger.debug("Length of label = " + std::to_string(strlen(label)));
    node_manager_logger.debug("DEBUG: raw edgeRef from DB (disk) " + std::to_string(edgeRef));

    nodeBlockPointer = new NodeBlock(nodeId, vertexId, blockAddress, propRef, metaPropRef, edgeRef,
                                     centralEdgeRef, edgeRefPID, label, usage);

    node_manager_logger.debug("DEBUG: nodeBlockPointer after creating the object edgeRef " +
                              std::to_string(nodeBlockPointer->edgeRef));

    if (nodeBlockPointer->edgeRef % RelationBlock::BLOCK_SIZE != 0) {
        node_manager_logger.error("Exception: Invalid edge reference address = " + nodeBlockPointer->edgeRef);
    }
    return nodeBlockPointer;
}

void NodeManager::persistNodeIndex() {
    std::ofstream index_db(indexDBPath, std::ios::trunc | std::ios::binary);
    if (index_db.is_open()) {
        if (this->nodeIndex.size() > 0 && (this->nodeIndex.begin()->first.length() > NodeManager::INDEX_KEY_SIZE)) {
            node_manager_logger.error("Node label/ID is longer ( " +
                                      std::to_string(this->nodeIndex.begin()->first.length()) +
                                      " ) than the index key size " + std::to_string(NodeManager::INDEX_KEY_SIZE));
            node_manager_logger.error("Node label/ID is longer than the index key size!");
        }
        for (auto nodeMap : this->nodeIndex) {
            char nodeIDC[NodeManager::INDEX_KEY_SIZE] = {0};  // Initialize with null chars
            std::strcpy(nodeIDC, nodeMap.first.c_str());
            index_db.write(nodeIDC, sizeof(nodeIDC));
            unsigned int nodeBlockIndex = nodeMap.second;
            index_db.write(reinterpret_cast<char *>(&(nodeBlockIndex)), sizeof(unsigned int));
            node_manager_logger.debug("Writing node index --> Node key = " + std::string(nodeIDC) + " value " +
                                      std::to_string(nodeBlockIndex));
        }
    }
    index_db.close();
}

/**
 * Return the number of nodes upto the limit given in the arg from nodes index
 * Default limit is 10
 * */
std::list<NodeBlock> NodeManager::getLimitedGraph(int limit) {
    int i = 0;
    std::list<NodeBlock> vertices;
    for (auto it : this->nodeIndex) {
        i++;
        if (i > limit) {
            break;
        }
        auto nodeId = it.first;
        NodeBlock *node = this->get(nodeId);
        vertices.push_back(*node);
    }
    return vertices;
}

/**
 * Return all nodes
 * */
std::list<NodeBlock*> NodeManager::getGraph() {
    std::list<NodeBlock*> vertices;
    for (auto it : this->nodeIndex) {
        auto nodeId = it.first;
        NodeBlock *node = this->get(nodeId);
        vertices.push_back(node);
        node_manager_logger.debug("Read node index for node  " + nodeId + " with node index " +
                                  std::to_string(it.second));
    }
    return vertices;
}

/**
 * Return all central nodes
 * */
std::list<NodeBlock*> NodeManager::getCentralGraph() {
    std::list<NodeBlock*> vertices;
    for (auto it : this->nodeIndex) {
        auto nodeId = it.first;
        NodeBlock *node = this->get(nodeId);
        if (node->getCentralRelationHead()) {
            vertices.push_back(node);
        }
        node_manager_logger.debug("Read node index for central node " + nodeId + " with node index " +
                                  std::to_string(it.second));
    }
    return vertices;
}

// Get adjacency list for the graph
std::map<long, std::unordered_set<long>> NodeManager::getAdjacencyList() {
    map<long, std::unordered_set<long>> adjacencyList;
    for (auto it : this->nodeIndex) {
        auto nodeId = it.first;
        NodeBlock *node = this->get(nodeId);
        std::unordered_set<long> neighbors;
        std::list<NodeBlock*> neighborNodes = node->getAllEdgeNodes();

        for (auto neighborNode : neighborNodes) {
            neighbors.insert(neighborNode->nodeId);
        }
        adjacencyList.emplace((long)node->nodeId, neighbors);
    }
    return adjacencyList;
}

std::map<long, std::unordered_set<long>> NodeManager::getAdjacencyList(bool isLocal) {
    std::map<long, std::unordered_set<long>> adjacencyList;

    int relationBlockSize = RelationBlock::BLOCK_SIZE;
    long  newRelationCount;

    if (isLocal) {
         newRelationCount = dbSize(dbPrefix + "_relations.db") / relationBlockSize - 1;
    } else {
         newRelationCount = dbSize(dbPrefix + "_central_relations.db") / relationBlockSize - 1;
    }

    RelationBlock* relationBlock;

    node_manager_logger.info("Relation count : "+ to_string(newRelationCount));
    for (int i = 1; i <=  newRelationCount ; i++) {
        if (isLocal) {
            relationBlock = RelationBlock::getLocalRelation(i * relationBlockSize);
        } else {
            relationBlock = RelationBlock::getCentralRelation(i * relationBlockSize);
        }
        long src = std::stol(relationBlock->getSource()->id);
        long dest = std::stol(relationBlock->getDestination()->id);

        // Insert into adjacency list
        adjacencyList[src].insert(dest);
    }
    return adjacencyList;
}

// Get degree map
std::map<long, long> NodeManager::getDistributionMap() {
    std::map<long, std::unordered_set<long>> adjacencyList = getAdjacencyList();
    std::map<long, long> distributionMap;
    for (auto it : adjacencyList) {
        distributionMap.emplace(it.first, it.second.size());
    }

    return distributionMap;
}

/**
 *
 * When closing the node manager,
 * It closes all the open databases and persist the node index in-memory hash map to node index database
 *
 * **/
void NodeManager::close() {
    this->persistNodeIndex();
    if (PropertyLink::propertiesDB) {
        PropertyLink::propertiesDB->flush();
        PropertyLink::propertiesDB->close();
    }
    if (NodeBlock::nodesDB) {
        NodeBlock::nodesDB->flush();
        NodeBlock::nodesDB->close();
    }
    if (RelationBlock::relationsDB) {
        RelationBlock::relationsDB->flush();
        RelationBlock::relationsDB->close();
    }
    if (RelationBlock::centralRelationsDB) {
        RelationBlock::centralRelationsDB->flush();
        RelationBlock::centralRelationsDB->close();
    }
}

/**
 *
 * Set the size of node index key size at the run time.
 * By default Node/Vertext ID or the label is used as the key in the node index database, which act as a lookup table
 * for node IDs to their block address in node DB
 *
 * */
void NodeManager::setIndexKeySize(unsigned long newIndexKeySize) {
    if (NodeBlock::LABEL_SIZE > newIndexKeySize) {
        node_manager_logger.warn("Node/Vertext ID or label is larger than the index key size!!");
    }

    this->INDEX_KEY_SIZE = newIndexKeySize;
}

int NodeManager::getGraphID() {
    return this->graphID;
}

int NodeManager::getPartitionID() {
    return this->partitionID;
}

std::string NodeManager::getDbPrefix() {
    return dbPrefix;
}

const std::string NodeManager::FILE_MODE = "app";  // for appending to existing DB
