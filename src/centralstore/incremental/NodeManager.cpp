#include "NodeManager.h"

#include <sys/stat.h>

#include "NodeBlock.h"  // To setup node DB
#include "PropertyLink.h"
#include "RelationBlock.h"

NodeManager::NodeManager(std::string mode) {
    std::ios_base::openmode openMode = std::ios::trunc;  // default is Trunc mode which overrides the entire file
    if (mode == "app") {
        openMode = std::ios::app;  // if app, open in append mode
        this->nodeIndex = readNodeIndex();
    }
    NodeBlock::nodesDB =
        new std::fstream(NodeManager::NODE_DB_PATH, std::ios::in | std::ios::out | openMode | std::ios::binary);
    PropertyLink::propertiesDB =
        new std::fstream(PropertyLink::DB_PATH, std::ios::in | std::ios::out | openMode | std::ios::binary);
    RelationBlock::relationsDB =
        new std::fstream(RelationBlock::DB_PATH, std::ios::in | std::ios::out | openMode | std::ios::binary);
    // TODO (tmkasun): set PropertyLink nextPropertyIndex after validating by modulus check from file number of bytes

    if (dbSize(NodeManager::NODE_DB_PATH) % NodeBlock::BLOCK_SIZE != 0) {
        std::cout << "WARNING: " << NodeManager::NODE_DB_PATH << " might be corrupted!" << std::endl;
    }
}

std::unordered_map<std::string, unsigned int> NodeManager::readNodeIndex() {
    std::ifstream index_db(this->index_db_loc);
    std::unordered_map<std::string, unsigned int> _nodeIndex;  // temproy node index data holder

    if (index_db.is_open()) {
        int iSize = dbSize(this->index_db_loc);
        unsigned long dataWidth = NodeManager::INDEX_KEY_SIZE + sizeof(unsigned int);
        if (iSize % dataWidth != 0) {
            std::cout << "ERROR: " << this->index_db_loc << " is corrupted!" << std::endl;
            throw std::runtime_error("Node index DB in " + this->index_db_loc + " is corrupted!");
        }

        char nodeIDC[NodeManager::INDEX_KEY_SIZE];
        unsigned int nodeIndexId;
        for (size_t i = 0; i < iSize / dataWidth; i++) {
            nodeIDC[NodeManager::INDEX_KEY_SIZE] = {0};  // Fill with null chars before puting data
            if (!index_db.read(&nodeIDC[0], NodeManager::INDEX_KEY_SIZE)) {
                std::cout << "ERROR: Error while reading index data from block " << i << std::endl;
            }
            if (!index_db.read(reinterpret_cast<char *>(&nodeIndexId), sizeof(unsigned int))) {
                std::cout << "ERROR: Error while reading index data from block " << i << std::endl;
            }
            _nodeIndex[std::string(nodeIDC)] = nodeIndexId;
        }
    }
    index_db.close();
    return _nodeIndex;
}

unsigned int NodeManager::addRelation(NodeBlock source, NodeBlock destination) {
    RelationBlock *newRelation = NULL;
    if (source.edgeRef == 0 || destination.edgeRef == 0) {  // certainly a new relation block needed
        newRelation = RelationBlock::add(source, destination);
        if (newRelation) {
            source.updateRelation(newRelation);
            destination.updateRelation(newRelation);
        } else {
            std::cout << "WARNING: Something went wrong while adding the new edge/relation !" << std::endl;
        }

    } else {
        // check if the relation already exist or not
        std::cout << "DEBUG: Relations already exist for both nodes" << std::endl;
    }

    std::cout << "all done" << std::endl;
}

NodeBlock *NodeManager::addNode(std::string nodeId) {
    unsigned int assignedNodeIndex;
    if (this->nodeIndex.find(nodeId) == this->nodeIndex.end()) {
        std::cout << "DEBUG: nodeId not found in index " << nodeId << std::endl;
        NodeBlock *sourceBlk = new NodeBlock(nodeId, this->nextNodeIndex * NodeBlock::BLOCK_SIZE);
        this->nodeIndex.insert({nodeId, this->nextNodeIndex});
        assignedNodeIndex = this->nextNodeIndex;
        this->nextNodeIndex++;
        sourceBlk->save();
        return sourceBlk;
    } else {
        std::cout << "DEBUG: Found nodeIndex for nodeId " << nodeId << " at " << assignedNodeIndex << std::endl;
        return this->get(nodeId);
    }
}

void NodeManager::addEdge(std::pair<int, int> edge) {
    std::string sourceId = std::to_string(edge.first);
    std::string destinationId = std::to_string(edge.second);
    NodeBlock *sourceNode = this->addNode(sourceId);
    NodeBlock *destNode = this->addNode(destinationId);
    unsigned int relationAddr = this->addRelation(*sourceNode, *destNode);

    std::cout << "DEBUG: Source DB block address " << sourceNode->addr << " Destination DB block address "
              << destNode->addr << std::endl;
    delete sourceNode;
    delete destNode;
}

int NodeManager::dbSize(std::string path) {
    /*
        The structure stat contains at least the following members:
        st_dev     ID of device containing file
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
        std::cout << "DEBUG: Size of the " << path << " is " << result.st_size << std::endl;
    } else {
        std::cout << "ERROR: Error while reading file stats of " << path << std::endl;
        return -1;
    }

    return result.st_size;
}

NodeBlock *NodeManager::get(std::string nodeId) {
    NodeBlock *nodeBlockPointer = NULL;

    if (this->nodeIndex.find(nodeId) == this->nodeIndex.end()) {  // Not found
        return nodeBlockPointer;
    }
    unsigned int nodeIndex = this->nodeIndex[nodeId];
    const unsigned int blockAddress = nodeIndex * NodeBlock::BLOCK_SIZE;
    NodeBlock::nodesDB->seekg(blockAddress);
    unsigned int edgeRef;
    unsigned int propRef;
    char usageBlock;
    char label[6];

    if (!NodeBlock::nodesDB->get(usageBlock)) {
        std::cout << "ERROR: Error while reading usage data from block " << blockAddress << std::endl;
    }

    if (!NodeBlock::nodesDB->read(reinterpret_cast<char *>(&edgeRef), sizeof(unsigned int))) {
        std::cout << "ERROR: Error while reading edge reference data from block " << blockAddress << std::endl;
    }

    if (!NodeBlock::nodesDB->read(reinterpret_cast<char *>(&propRef), sizeof(unsigned int))) {
        std::cout << "ERROR: Error while reading prop reference data from block " << blockAddress << std::endl;
    }

    if (!NodeBlock::nodesDB->read(&label[0], 6)) {
        std::cout << "ERROR: Error while reading label data from block " << blockAddress << std::endl;
    }
    bool usage = usageBlock == '\1';
    std::cout << "Label = " << label << std::endl;
    std::cout << "Length of label = " << strlen(label) << std::endl;
    nodeBlockPointer = new NodeBlock(nodeId, blockAddress, edgeRef, propRef, label, usage);
    if (nodeBlockPointer->edgeRef % RelationBlock::BLOCK_SIZE != 0) {
        std::cout << "WARNING: Invalid edge reference address = " << nodeBlockPointer->edgeRef << std::endl;
    }
    return nodeBlockPointer;
}

void NodeManager::persistNodeIndex() {
    std::ofstream index_db(this->index_db_loc, std::ios::trunc | std::ios::binary);
    if (index_db.is_open()) {
        for (auto nodeMap : this->nodeIndex) {
            char nodeIDC[NodeManager::INDEX_KEY_SIZE] = {0};  // Initialize with null chars
            std::strcpy(nodeIDC, nodeMap.first.c_str());
            index_db.write(nodeIDC, sizeof(nodeIDC));
            unsigned int nodeBlockIndex = nodeMap.second;
            index_db.write(reinterpret_cast<char *>(&(nodeBlockIndex)), sizeof(unsigned int));

            std::cout << "DEBUG: writing node index --> Node key " << nodeIDC << " value " << nodeBlockIndex
                      << std::endl;
        }
    }
    index_db.close();
}

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
}

const unsigned long NodeManager::INDEX_KEY_SIZE = 8;
unsigned int NodeManager::nextPropertyIndex = 0;
std::string NodeManager::NODE_DB_PATH = "streamStore/nodes.db";