#ifndef JASMINE_HASH_PARTITIONER_HEADER
#define JASMINE_HASH_PARTITIONER_HEADER

#include "Partitioner.h"
#include "../../util/logger/Logger.h"
#include "../../util/Utils.h"
#include <vector>
#include <string>
#include <stdio.h>
#include <stdlib.h>
#include <map>
#include <functional>  // For std::hash
#include <mutex>
#include <condition_variable>

class HashPartitioner {
    std::vector<Partition> partitions; // Holds partition objects
    std::vector<std::mutex> partitionLocks; // Array of mutexes for each partition
    int numberOfPartitions;
    int graphId;

public:
    HashPartitioner(int numberOfPartitions, int graphID)
            : numberOfPartitions(numberOfPartitions), graphId(graphID), partitionLocks(numberOfPartitions),
              vertexCount(0), edgeCount(0) {
        for (size_t i = 0; i < numberOfPartitions; i++) {
            this->partitions.push_back(Partition(i, numberOfPartitions));
        }
        this->outputFilePath=Utils::getHomeDir() + "/.jasminegraph/tmp/" + std::to_string(this->graphId);
    }

//    std::vector<std::map<int, std::string>> generateFullFileList();

    void uploadGraphLocally(std::string masterIP);

    partitionedEdge hashPartitioning(std::pair<std::string, std::string> edge);

    void writeSerializedPartitionFiles(int partition);
    void writeSerializedMasterFiles(int partition);
//    void writeSerializedDuplicateMasterFiles(int partition);
//
    void printStats();

    long getVertexCount();

    long getEdgeCount();

private:
    long vertexCount;
    long edgeCount;
    std::string outputFilePath;
    std::map<int, std::string> partitionFileMap;
    std::map<int, std::string> centralStoreFileList;
    std::map<int, std::string> centralStoreDuplicateFileList;
    std::map<int, std::string> partitionAttributeFileList;
    std::map<int, std::string> centralStoreAttributeFileList;
    std::map<int, std::string> compositeCentralStoreFileList;

    std::vector<std::map<int, std::string>> fullFileList;
    std::map<int, std::map<int, std::vector<int>>> partitionedLocalGraphStorageMap;

    std::vector<std::map<int, std::string>> generateFullFileList();

};

#endif  // !JASMINE_HASH_PARTITIONER_HEADER
