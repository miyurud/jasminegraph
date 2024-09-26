#include "HashPartitioner.h"
#include "../../server/JasmineGraphServer.h"
#include <string>
#include <vector>
#include <thread>

Logger hash_partitioner_logger;
std::mutex partitionFileMutex;
std::mutex centralStoreFileMutex;

partitionedEdge HashPartitioner::hashPartitioning(std::pair<std::string, std::string> edge) {
    int firstPartition = std::hash<std::string>()(edge.first) % this->numberOfPartitions;
    int secondPartition = std::hash<std::string>()(edge.second) % this->numberOfPartitions;

    if (firstPartition == secondPartition) {
        std::lock_guard<std::mutex> lock(partitionLocks[firstPartition]);
        this->partitions[firstPartition].addEdge(edge);
    } else {
        // Lock the partitions in sequence to avoid deadlock
        std::lock_guard<std::mutex> lockFirst(partitionLocks[firstPartition]);
        this->partitions[firstPartition].addToEdgeCuts(edge.first, edge.second, secondPartition);

        std::lock_guard<std::mutex> lockSecond(partitionLocks[secondPartition]);
        this->partitions[secondPartition].addToEdgeCuts(edge.second, edge.first, firstPartition);
    }

    return {{edge.first,  firstPartition},
            {edge.second, secondPartition}};
}

void HashPartitioner::printStats() {
    int totalVertices = 0;
    int totalEdges = 0;
    for (auto &partition: this->partitions) {
        totalVertices += partition.getVertextCount();
        totalEdges += partition.getEdgesCount();
    }
    this->vertexCount = totalVertices;
    this->edgeCount = totalEdges;
    hash_partitioner_logger.info("Total vertices: " + std::to_string(totalVertices));
    hash_partitioner_logger.info("Total edges: " + std::to_string(totalEdges));
}

long HashPartitioner::getVertexCount() {
    if (this->vertexCount == 0) {
        int totalVertices = 0;
        int totalEdges = 0;
        for (auto &partition: this->partitions) {
            totalVertices += partition.getVertextCount();
            totalEdges += partition.getEdgesCount();
        }
        this->vertexCount = totalVertices;
        this->edgeCount = totalEdges;
    }
    return this->vertexCount;
}

long HashPartitioner::getEdgeCount() {
    if (this->edgeCount == 0) {
        int totalVertices = 0;
        int totalEdges = 0;
        for (auto &partition: this->partitions) {
            totalVertices += partition.getVertextCount();
            totalEdges += partition.getEdgesCount();
        }
        this->vertexCount = totalVertices;
        this->edgeCount = totalEdges;
    }
    return this->edgeCount;
}

std::vector<std::map<int, std::string>> HashPartitioner::generateFullFileList() {
    hash_partitioner_logger.info("Generating full file list..");
    std::thread threads[numberOfPartitions * 3];
    int count = 0;
    for (int i = 0; i < numberOfPartitions; i++) {
        threads[count++] = std::thread(&HashPartitioner::writeSerializedPartitionFiles, this, i);
        threads[count++] = std::thread(&HashPartitioner::writeSerializedMasterFiles, this, i);
//        threads[count++] = std::thread(&HashPartitioner::writeSerializedDuplicateMasterFiles, this, i);
    }

    for (int i = 0; i < count; i++) {
        threads[i].join(); // Join threads to ensure serialization is complete
    }

    // Generate full file list
    this->fullFileList.push_back(this->partitionFileMap);
    this->fullFileList.push_back(this->centralStoreFileList);
    this->fullFileList.push_back(this->centralStoreDuplicateFileList);
    this->fullFileList.push_back(this->partitionAttributeFileList);
    this->fullFileList.push_back(this->centralStoreAttributeFileList);
    this->fullFileList.push_back(this->compositeCentralStoreFileList);

    hash_partitioner_logger.info("Successfully generated full file list");
    return this->fullFileList;
}

void HashPartitioner::uploadGraphLocally(std::string masterIP) {
    hash_partitioner_logger.info("Uploading graph locally..");
    JasmineGraphServer *server = JasmineGraphServer::getInstance();
    server->uploadGraphLocally(graphId, Conts::GRAPH_TYPE_NORMAL, generateFullFileList(), masterIP);
    hash_partitioner_logger.info("Successfully uploaded the graph.");
}

void HashPartitioner::writeSerializedPartitionFiles(int partition) {
    std::string outputFilePartition = outputFilePath + "/" + std::to_string(this->graphId) + "_" + std::to_string(partition);

    std::map<int, std::vector<int>> partitionEdgeMap = partitions[partition].getLocalStorageMap();

    JasmineGraphHashMapLocalStore::storePartEdgeMap(partitionEdgeMap, outputFilePartition);

    Utils::compressFile(outputFilePartition);
    std::lock_guard<std::mutex> lock(partitionFileMutex);
    partitionFileMap[partition] = outputFilePartition + ".gz";
    hash_partitioner_logger.log("Serializing done for local part " + std::to_string(partition), "info");
}

void HashPartitioner::writeSerializedMasterFiles(int partition) {
    std::string outputFilePartMaster = outputFilePath + "/" + std::to_string(this->graphId) + "_centralstore_" + std::to_string(partition);

    std::map<int, std::vector<int>> partitionMasterEdgeMap = partitions[partition].getPartitionMasterEdgeMap();

    JasmineGraphHashMapCentralStore::storePartEdgeMap(partitionMasterEdgeMap, outputFilePartMaster);

    Utils::compressFile(outputFilePartMaster);
    std::lock_guard<std::mutex> lock(centralStoreFileMutex);
    centralStoreFileList[partition] = outputFilePartMaster + ".gz";
    hash_partitioner_logger.log("Serializing done for central part " + std::to_string(partition), "info");
}

//void HashPartitioner::writeSerializedDuplicateMasterFiles(int partition) {
//    std::string outputFilePartMaster = outputFilePath + "/" + std::to_string(this->graphId) + "_centralstore_dp_" + std::to_string(partition);
//
//    std::map<int, std::vector<int>> partMasterEdgeMap = partitions[partition].getDuplicateMasterGraphStorageMap();
//
//    JasmineGraphHashMapCentralStore::storePartEdgeMap(partMasterEdgeMap, outputFilePartMaster);
//
//    Utils::compressFile(outputFilePartMaster);
//    std::lock_guard<std::mutex> lock(centralStoreFileMutex);
//    centralStoreDuplicateFileList[partition] = outputFilePartMaster + ".gz";
//    hash_partitioner_logger.log("Serializing done for duplicate central part " + std::to_string(partition), "info");
//}
