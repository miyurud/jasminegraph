/**
Copyright 2024 JasminGraph Team
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

#ifndef JASMINEGRAPH_SHEEPPARTITIONER_H
#define JASMINEGRAPH_SHEEPPARTITIONER_H

#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <queue>
#include "../../metadb/SQLiteDBInterface.h"
#include "../../util/Utils.h"
#include "../../localstore/JasmineGraphHashMapLocalStore.h"
#include "../../centralstore/JasmineGraphHashMapCentralStore.h"

using std::string;

typedef unsigned long vertex_id;
typedef short partition_id;

/**
 * SheepPartitioner implements a streaming graph partitioning algorithm
 * based on elimination trees and balanced bin-packing.
 * 
 * The algorithm processes vertices in degree-sorted order and assigns
 * them to partitions to minimize edge cuts while maintaining balance.
 */
class SheepPartitioner {
 public:
    SheepPartitioner(SQLiteDBInterface *sqlite);

    /**
     * Partition a graph using the sheep streaming partitioning algorithm
     * @param graphID Graph identifier
     * @param graphPath Path to the input graph file (edge list format)
     * @param outputPath Base path for output partition files
     * @param numPartitions Number of partitions to create
     * @return vector of file maps for distribution to workers
     */
    std::vector<std::map<int, std::string>> partitionGraph(int graphID, const string &graphPath, 
                                                            const string &outputPath, int numPartitions);

    /**
     * Get partitioning statistics after partitioning
     */
    void getPartitioningStats();

 private:
    SQLiteDBInterface *sqlite;
    
    // Graph structure
    std::unordered_map<vertex_id, std::vector<vertex_id>> adjacencyList;
    std::unordered_map<vertex_id, partition_id> vertexToPartition;
    std::vector<size_t> partitionSizes;
    
    // Statistics
    size_t totalEdges;
    size_t edgeCuts;
    size_t numPartitions;
    std::vector<size_t> partitionVertexCounts;
    std::vector<size_t> partitionEdgeCountsVec;
    
    // File lists for worker distribution
    std::map<int, std::string> partitionFileMap;
    std::map<int, std::string> centralStoreFileList;
    std::map<int, std::string> centralStoreDuplicateFileList;
    std::vector<std::map<int, std::string>> fullFileList;
    
    /**
     * Load graph from edge list file
     * Expected format: source_vertex target_vertex (per line)
     */
    bool loadGraph(const string &graphPath);
    
    /**
     * Sort vertices by degree (descending order)
     */
    std::vector<vertex_id> getDegreeSequence();
    
    /**
     * Assign vertices to partitions using streaming algorithm
     * Based on elimination tree partitioning with balance constraints
     */
    void assignPartitions();
    
    /**
     * Calculate partition score for a vertex
     * Score favors partitions with more neighbors and less load
     */
    double calculatePartitionScore(vertex_id vertex, partition_id partition, 
                                   size_t maxPartitionSize);
    
    /**
     * Write partitions to output files
     */
    bool writePartitions(const string &outputPath);
    
    /**
     * Calculate edge cut statistics
     */
    void calculateEdgeCuts();
};

#endif  // JASMINEGRAPH_SHEEPPARTITIONER_H
