/**
Copyright 2026 JasminGraph Team
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
#include <string_view>
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
using std::string_view;

using vertex_id = unsigned long;
using partition_id = short;

/**
 * SheepPartitioner implements a streaming graph partitioning algorithm
 * based on elimination trees and balanced bin-packing.
 * 
 * The algorithm processes vertices in degree-sorted order and assigns
 * them to partitions to minimize edge cuts while maintaining balance.
 */
class SheepPartitioner {
 public:
   /**
    * Create a graph partitioner using the metadata database connection
    * @param sqlite Metadata database interface
    */
    explicit SheepPartitioner(SQLiteDBInterface *sqlite);

    /**
     * Partition a graph using the sheep streaming partitioning algorithm
     * @param graphID Graph identifier
     * @param graphPath Path to the input graph file (edge list format)
     * @param outputPath Base path for output partition files
     * @param numPartitions Number of partitions to create
     * @return vector of file maps for distribution to workers
     */
    std::vector<std::map<int, std::string>> partitionGraph(int graphID, const string &graphPath,
                                                            const string &outputPath, int numberOfPartitions);

    /**
     * Get partitioning statistics after partitioning
     */
    void getPartitioningStats();

 private:
    SQLiteDBInterface *sqlite = nullptr;

    // Graph structure
    std::unordered_map<vertex_id, std::vector<vertex_id>> adjacencyList;
    std::unordered_map<vertex_id, partition_id> vertexToPartition;
    std::vector<size_t> partitionSizes;

    // Statistics
    size_t totalEdges = 0;
    size_t edgeCuts = 0;
    size_t numPartitions = 0;
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
        * @param graphPath Path to the input graph file
        * @return true if the graph was loaded successfully, false otherwise
     */
    bool loadGraph(const string &graphPath);

    /**
     * Sort vertices by degree (descending order)
     */
    std::vector<vertex_id> getDegreeSequence() const;

    /**
     * Assign vertices to partitions using streaming algorithm
     * Based on elimination tree partitioning with balance constraints
     */
    void assignPartitions();

    /**
     * Calculate partition score for a vertex
     * Score favors partitions with more neighbors and less load
        * @param vertex Vertex to score
        * @param partition Partition being evaluated
        * @param maxPartitionSize Maximum allowed size of a partition
        * @return Score for assigning the vertex to the partition
     */
    double calculatePartitionScore(vertex_id vertex, partition_id partition,
                                   size_t maxPartitionSize);

    /**
     * Write partitions to output files
        * @param outputPath Base path for the output partition files
        * @return true if the partitions were written successfully, false otherwise
     */
    bool writePartitions(const string &outputPath);

    /**
     * Extract graphID from outputPath (format: path/graphID_)
        * @param outputPath Output path containing the graph identifier
        * @return Extracted graph identifier
     */
    string extractGraphID(string_view outputPath) const;

    /**
     * Build local and central edge maps and counts from adjacency list
        * @param partitionVertices Vertex sets assigned to each partition
        * @param localEdgeCounts Number of local edges in each partition
        * @param centralEdgeCounts Number of central edges in each partition
        * @param localStoreSets Local store edge sets for each partition
        * @param centralStoreSets Central store edge sets for each partition
        * @param duplicateCentralStoreSets Duplicate central store edge sets for each partition
     */
    void buildEdgeMaps(
        std::vector<std::set<vertex_id>> &partitionVertices,
        std::vector<size_t> &localEdgeCounts,
        std::vector<size_t> &centralEdgeCounts,
        std::vector<std::map<int, std::unordered_set<int>>> &localStoreSets,
        std::vector<std::map<int, std::unordered_set<int>>> &centralStoreSets,
        std::vector<std::map<int, std::unordered_set<int>>> &duplicateCentralStoreSets);

    /**
     * Convert sets to vectors for serialization
        * @param sets Store sets to convert
        * @param maps Destination maps containing vectors instead of sets
     */
    void convertStoreSetsToMaps(
        const std::vector<std::map<int, std::unordered_set<int>>> &sets,
        std::vector<std::map<int, std::vector<int>>> &maps) const;

    /**
     * Serialize and compress a single partition using FlatBuffers
        * @param partitionId Partition to serialize
        * @param outputPath Base path for the serialized output
        * @param graphID Graph identifier
        * @param localMap Local store data for the partition
        * @param centralMap Central store data for the partition
        * @param duplicateCentralMap Duplicate central store data for the partition
        * @return true if serialization succeeds, false otherwise
     */
    bool serializeSinglePartition(
        size_t partitionId,
        const string &outputPath,
        const string &graphID,
        const std::map<int, std::vector<int>> &localMap,
        const std::map<int, std::vector<int>> &centralMap,
        const std::map<int, std::vector<int>> &duplicateCentralMap);

    /**
     * Calculate edge cut statistics
     */
    void calculateEdgeCuts();
};

#endif  // JASMINEGRAPH_SHEEPPARTITIONER_H
