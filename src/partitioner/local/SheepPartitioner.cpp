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

#include "SheepPartitioner.h"
#include "../../util/logger/Logger.h"
#include <filesystem>
#include <sstream>
#include <fstream>
#include <algorithm>
#include <cmath>

using namespace std;

Logger sheep_partitioner_logger;

SheepPartitioner::SheepPartitioner(SQLiteDBInterface *sqlite) {
    this->sqlite = sqlite;
    this->totalEdges = 0;
    this->edgeCuts = 0;
    this->numPartitions = 0;
}

bool SheepPartitioner::loadGraph(const string &graphPath) {
    sheep_partitioner_logger.info("Loading graph from: " + graphPath);
    
    ifstream inputFile(graphPath);
    if (!inputFile.is_open()) {
        sheep_partitioner_logger.error("Failed to open graph file: " + graphPath);
        return false;
    }
    
    string line;
    size_t lineCount = 0;
    
    while (getline(inputFile, line)) {
        lineCount++;
        
        // Skip empty lines and comments
        if (line.empty() || line[0] == '#') {
            continue;
        }
        
        istringstream iss(line);
        vertex_id source, target;
        
        if (iss >> source >> target) {
            // Add edge to adjacency list
            adjacencyList[source].push_back(target);
            adjacencyList[target].push_back(source);
            totalEdges++;
        }
    }
    
    inputFile.close();
    
    sheep_partitioner_logger.info("Loaded " + to_string(adjacencyList.size()) + 
                                 " vertices and " + to_string(totalEdges) + " edges");
    return true;
}

std::vector<vertex_id> SheepPartitioner::getDegreeSequence() {
    sheep_partitioner_logger.info("Computing degree sequence");
    
    // Create vector of (vertex, degree) pairs
    std::vector<std::pair<vertex_id, size_t>> vertexDegrees;
    vertexDegrees.reserve(adjacencyList.size());
    
    for (const auto &entry : adjacencyList) {
        vertexDegrees.emplace_back(entry.first, entry.second.size());
    }
    
    // Sort by degree (descending)
    std::sort(vertexDegrees.begin(), vertexDegrees.end(),
              [](const auto &a, const auto &b) { return a.second > b.second; });
    
    // Extract vertex sequence
    std::vector<vertex_id> sequence;
    sequence.reserve(vertexDegrees.size());
    
    for (const auto &pair : vertexDegrees) {
        sequence.push_back(pair.first);
    }
    
    sheep_partitioner_logger.info("Degree sequence computed with " + 
                                 to_string(sequence.size()) + " vertices");
    return sequence;
}

double SheepPartitioner::calculatePartitionScore(vertex_id vertex, partition_id partition, 
                                                 size_t maxPartitionSize) {
    // Count neighbors in this partition
    size_t neighborsInPartition = 0;
    
    const auto &neighbors = adjacencyList[vertex];
    for (vertex_id neighbor : neighbors) {
        auto it = vertexToPartition.find(neighbor);
        if (it != vertexToPartition.end() && it->second == partition) {
            neighborsInPartition++;
        }
    }
    
    // Calculate balance penalty
    double currentSize = partitionSizes[partition];
    double balancePenalty = 0.0;
    
    if (currentSize >= maxPartitionSize) {
        // Heavily penalize over-capacity partitions
        balancePenalty = -1000.0 * (currentSize - maxPartitionSize + 1);
    } else {
        // Slight penalty for imbalance
        double loadFactor = currentSize / (double)maxPartitionSize;
        balancePenalty = -10.0 * loadFactor;
    }
    
    // Score = neighbors in partition + balance penalty
    // Higher score is better
    return neighborsInPartition + balancePenalty;
}

void SheepPartitioner::assignPartitions() {
    sheep_partitioner_logger.info("Assigning vertices to partitions");
    
    // Get degree-ordered sequence
    std::vector<vertex_id> sequence = getDegreeSequence();
    
    // Initialize partition sizes
    partitionSizes.resize(numPartitions, 0);
    
    // Calculate maximum partition size (with 3% balance tolerance)
    size_t totalVertices = adjacencyList.size();
    size_t maxPartitionSize = (totalVertices / numPartitions) * 1.03;
    
    sheep_partitioner_logger.info("Max partition size: " + to_string(maxPartitionSize));
    
    // Process vertices in degree order
    for (vertex_id vertex : sequence) {
        partition_id bestPartition = 0;
        double bestScore = -std::numeric_limits<double>::infinity();
        
        // Evaluate each partition
        for (partition_id p = 0; p < numPartitions; p++) {
            double score = calculatePartitionScore(vertex, p, maxPartitionSize);
            
            if (score > bestScore) {
                bestScore = score;
                bestPartition = p;
            }
        }
        
        // Assign vertex to best partition
        vertexToPartition[vertex] = bestPartition;
        partitionSizes[bestPartition]++;
    }
    
    sheep_partitioner_logger.info("Vertex assignment complete");
    
    // Log partition sizes
    for (size_t i = 0; i < numPartitions; i++) {
        sheep_partitioner_logger.info("Partition " + to_string(i) + " size: " + 
                                     to_string(partitionSizes[i]));
    }
}

void SheepPartitioner::calculateEdgeCuts() {
    edgeCuts = 0;
    
    for (const auto &entry : adjacencyList) {
        vertex_id source = entry.first;
        partition_id sourcePart = vertexToPartition[source];
        
        for (vertex_id target : entry.second) {
            if (source < target) {  // Count each edge once
                partition_id targetPart = vertexToPartition[target];
                if (sourcePart != targetPart) {
                    edgeCuts++;
                }
            }
        }
    }
    
    sheep_partitioner_logger.info("Edge cuts: " + to_string(edgeCuts) + 
                                 " (" + to_string((edgeCuts * 100.0) / totalEdges) + "%)");
}

bool SheepPartitioner::writePartitions(const string &outputPath) {
    sheep_partitioner_logger.info("Writing partitions with central/local store format to: " + outputPath);
    
    try {
        // Create output directory if needed
        std::filesystem::path basePath(outputPath);
        std::filesystem::path parentDir = basePath.parent_path();
        if (!parentDir.empty() && !std::filesystem::exists(parentDir)) {
            std::filesystem::create_directories(parentDir);
        }
        
        // Track vertices and edges per partition
        std::vector<std::set<vertex_id>> partitionVertices(numPartitions);
        std::vector<size_t> localEdgeCounts(numPartitions, 0);
        std::vector<size_t> centralEdgeCounts(numPartitions, 0);
        
        // Build edge maps for serialization (same format as Metis)
        std::vector<std::map<int, std::vector<int>>> localStoreMaps(numPartitions);
        std::vector<std::map<int, std::vector<int>>> centralStoreMaps(numPartitions);
        std::vector<std::map<int, std::vector<int>>> duplicateCentralStoreMaps(numPartitions);
        
        // Extract graphID from outputPath (format: path/graphID_)
        string graphID = "0";
        size_t lastSlash = outputPath.find_last_of("/\\");
        if (lastSlash != string::npos) {
            string filename = outputPath.substr(lastSlash + 1);
            size_t firstUnderscore = filename.find('_');
            if (firstUnderscore != string::npos) {
                graphID = filename.substr(0, firstUnderscore);
            }
        }
        
        // Build edge maps from adjacency list
        for (const auto &entry : adjacencyList) {
            vertex_id source = entry.first;
            partition_id sourcePart = vertexToPartition[source];
            
            // Track vertex in its partition
            partitionVertices[sourcePart].insert(source);
            
            for (vertex_id target : entry.second) {
                partition_id targetPart = vertexToPartition[target];
                
                // Track vertex in its partition
                partitionVertices[targetPart].insert(target);
                
                if (sourcePart == targetPart) {
                    // Local edge - both vertices in same partition
                    // Add both directions to maintain undirected graph
                    localStoreMaps[sourcePart][source].push_back(target);
                    if (source < target) {  // Count each edge only once
                        localEdgeCounts[sourcePart]++;
                    }
                } else {
                    // Cross-partition edge
                    // To maintain undirected graph for triangle counting, we need both directions:
                    // 1. Add source->target to source partition's central store
                    centralStoreMaps[sourcePart][source].push_back(target);
                    // 2. Add source->target to target partition's duplicate central store
                    duplicateCentralStoreMaps[targetPart][source].push_back(target);
                    
                    if (source < target) {  // Count each edge only once
                        centralEdgeCounts[sourcePart]++;
                    }
                }
            }
        }
        
        // Serialize and compress files using FlatBuffers format
        for (size_t i = 0; i < numPartitions; i++) {
            string localFilename = outputPath + to_string(i);
            string centralFilename = outputPath.substr(0, outputPath.find_last_of("/\\") + 1) + 
                                   graphID + "_centralstore_" + to_string(i);
            string duplicateCentralFilename = outputPath.substr(0, outputPath.find_last_of("/\\") + 1) + 
                                            graphID + "_centralstore_dp_" + to_string(i);
            
            // Store local store using FlatBuffers
            if (!JasmineGraphHashMapLocalStore::storePartEdgeMap(localStoreMaps[i], localFilename)) {
                sheep_partitioner_logger.error("Failed to serialize local store for partition " + to_string(i));
                return false;
            }
            
            // Compress local store file
            Utils::compressFile(localFilename);
            partitionFileMap[i] = localFilename + ".gz";
            
            // Store central store using FlatBuffers
            if (!JasmineGraphHashMapCentralStore::storePartEdgeMap(centralStoreMaps[i], centralFilename)) {
                sheep_partitioner_logger.error("Failed to serialize central store for partition " + to_string(i));
                return false;
            }
            
            // Compress central store file
            Utils::compressFile(centralFilename);
            centralStoreFileList[i] = centralFilename + ".gz";
            
            // Store duplicate central store using FlatBuffers
            if (!JasmineGraphHashMapCentralStore::storePartEdgeMap(duplicateCentralStoreMaps[i], 
                                                                   duplicateCentralFilename)) {
                sheep_partitioner_logger.error("Failed to serialize duplicate central store for partition " + 
                                             to_string(i));
                return false;
            }
            
            // Compress duplicate central store file
            Utils::compressFile(duplicateCentralFilename);
            centralStoreDuplicateFileList[i] = duplicateCentralFilename + ".gz";
            
            sheep_partitioner_logger.info("Serialized and compressed partition " + to_string(i));
        }
        
        // Store partition statistics for later metadb update
        partitionVertexCounts.clear();
        partitionEdgeCountsVec.clear();
        for (size_t i = 0; i < numPartitions; i++) {
            partitionVertexCounts.push_back(partitionVertices[i].size());
            // Total edges = local edges + central edges
            partitionEdgeCountsVec.push_back(localEdgeCounts[i] + centralEdgeCounts[i]);
        }
        
        sheep_partitioner_logger.info("Successfully wrote " + to_string(numPartitions) + 
                                     " partition files (local store, central store, and duplicate central store)");
        
        // Log statistics for each partition
        for (size_t i = 0; i < numPartitions; i++) {
            sheep_partitioner_logger.info("Partition " + to_string(i) + 
                                        ": local edges=" + to_string(localEdgeCounts[i]) + 
                                        ", central edges=" + to_string(centralEdgeCounts[i]));
        }
        
        return true;
        
    } catch (const std::exception &e) {
        sheep_partitioner_logger.error("Error writing partitions: " + string(e.what()));
        return false;
    }
}

std::vector<std::map<int, std::string>> SheepPartitioner::partitionGraph(int graphID, const string &graphPath, 
                                                                          const string &outputPath, int numPartitions) {
    sheep_partitioner_logger.info("Starting sheep partitioning for graph ID: " + to_string(graphID));
    sheep_partitioner_logger.info("Input graph: " + graphPath);
    sheep_partitioner_logger.info("Output path: " + outputPath);
    sheep_partitioner_logger.info("Number of partitions: " + to_string(numPartitions));
    
    this->numPartitions = numPartitions;
    
    // Check if input graph exists
    if (!std::filesystem::exists(graphPath)) {
        sheep_partitioner_logger.error("Input graph file does not exist: " + graphPath);
        return fullFileList;
    }
    
    // Load the graph
    if (!loadGraph(graphPath)) {
        return fullFileList;
    }
    
    // Assign vertices to partitions
    assignPartitions();
    
    // Calculate edge cuts
    calculateEdgeCuts();
    
    // Write partitions to files
    if (!writePartitions(outputPath)) {
        return fullFileList;
    }
    
    // Update metadata database
    try {
        // Update graph table with statistics
        string sqlStatement = "UPDATE graph SET vertexcount = '" + to_string(adjacencyList.size()) +
                            "', centralpartitioncount = '" + to_string(numPartitions) +
                            "', edgecount = '" + to_string(totalEdges) +
                            "', id_algorithm = 'sheep' WHERE idgraph = '" + to_string(graphID) + "'";
        sqlite->runUpdate(sqlStatement);
        
        sheep_partitioner_logger.info("Updated graph table with statistics");
        
        // Insert partition records
        for (size_t i = 0; i < numPartitions; i++) {
            string partitionInsert =
                "INSERT INTO partition (idpartition, graph_idgraph, vertexcount, central_vertexcount, edgecount) "
                "VALUES('" + to_string(i) + "', '" + to_string(graphID) + "', '" +
                to_string(partitionVertexCounts[i]) + "', '0', '" +
                to_string(partitionEdgeCountsVec[i]) + "')";
            sqlite->runUpdate(partitionInsert);
        }
        
        sheep_partitioner_logger.info("Successfully partitioned graph " + to_string(graphID) + 
                                     " using sheep algorithm with " + to_string(numPartitions) + " partitions");
    } catch (const std::exception &e) {
        sheep_partitioner_logger.error("Error updating database: " + string(e.what()));
        return fullFileList;
    }
    
    // Build and return file lists for worker distribution
    fullFileList.push_back(partitionFileMap);
    fullFileList.push_back(centralStoreFileList);
    fullFileList.push_back(centralStoreDuplicateFileList);
    fullFileList.push_back(std::map<int, std::string>());  // Empty attribute files
    fullFileList.push_back(std::map<int, std::string>());  // Empty central attribute files
    fullFileList.push_back(std::map<int, std::string>());  // Empty composite central files
    
    return fullFileList;
}

void SheepPartitioner::getPartitioningStats() {
    sheep_partitioner_logger.info("=== Partitioning Statistics ===");
    sheep_partitioner_logger.info("Total vertices: " + to_string(adjacencyList.size()));
    sheep_partitioner_logger.info("Total edges: " + to_string(totalEdges));
    sheep_partitioner_logger.info("Number of partitions: " + to_string(numPartitions));
    sheep_partitioner_logger.info("Edge cuts: " + to_string(edgeCuts));
    sheep_partitioner_logger.info("Edge cut ratio: " + 
                                 to_string((edgeCuts * 100.0) / totalEdges) + "%");
    
    for (size_t i = 0; i < numPartitions; i++) {
        sheep_partitioner_logger.info("Partition " + to_string(i) + " size: " + 
                                     to_string(partitionSizes[i]));
    }
}
