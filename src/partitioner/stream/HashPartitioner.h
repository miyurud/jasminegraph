/*
 * Copyright 2024 JasminGraph Team
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef JASMINE_HASH_PARTITIONER_HEADER
#define JASMINE_HASH_PARTITIONER_HEADER

#include "Partitioner.h"
#include "../../util/logger/Logger.h"
#include "../../util/Utils.h"
#include "../../server/JasmineGraphServer.h"
#include "../../nativestore/DataPublisher.h"
#include <vector>
#include <string>
#include <map>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <thread>

class HashPartitioner {
    std::vector<Partition> partitions;

    std::atomic<bool> terminateConsumers;
    std::vector<std::thread> localEdgeThreads;
    std::vector<std::thread> edgeCutThreads;

    std::vector<std::mutex> partitionLocks;  // Array of mutexes for each partition
    std::vector<std::vector<std::pair<std::string, std::string>>> localEdgeArrays;
    std::vector<std::vector<std::pair<std::string, std::string>>> edgeCutsArrays;

    std::vector<std::mutex> localEdgeMutexes;
    std::vector<std::condition_variable> edgeAvailableCV;
    std::vector<bool> edgeReady;

    std::vector<std::mutex> edgeCutsMutexes;
    std::vector<std::condition_variable> edgeCutsAvailableCV;
    std::vector<bool> edgeCutsReady;

    int numberOfPartitions;
    int graphId;
    std::string masterIp;

    std::vector<std::mutex> partitionMutexArray;

 public:
    ~HashPartitioner();
    HashPartitioner(int numberOfPartitions, int graphID, std::string masterIp);
    long getVertexCount();
    long getEdgeCount();
    void addEdgeCut(const pair<std::string, std::string> &edge, int index);
    void addLocalEdge(const pair<std::string, std::string> &edge, int index);
    void updatePartitionTable();

 private:
    long vertexCount;
    long edgeCount;
    std::string outputFilePath;

    void consumeLocalEdges(int partitionIndex, JasmineGraphServer::worker worker);
    void consumeEdgeCuts(int partitionIndex, JasmineGraphServer::worker worker);
    void stopConsumerThreads();
};

#endif  // !JASMINE_HASH_PARTITIONER_HEADER
