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
    sheep_partitioner_logger.info("Writing partitions to: " + outputPath);
    
    try {
        // Create output directory if needed
        std::filesystem::path basePath(outputPath);
        std::filesystem::path parentDir = basePath.parent_path();
        if (!parentDir.empty() && !std::filesystem::exists(parentDir)) {
            std::filesystem::create_directories(parentDir);
        }
        
        // Create separate file for each partition
        std::vector<ofstream> partitionFiles(numPartitions);
        
        for (size_t i = 0; i < numPartitions; i++) {
            string filename = outputPath + to_string(i);
            partitionFiles[i].open(filename);
            
            if (!partitionFiles[i].is_open()) {
                sheep_partitioner_logger.error("Failed to create partition file: " + filename);
                return false;
            }
        }
        
        // Write edges to partition files
        // Each edge is written to the partition that contains both vertices
        // Cross-partition edges are written to both partitions
        for (const auto &entry : adjacencyList) {
            vertex_id source = entry.first;
            partition_id sourcePart = vertexToPartition[source];
            
            for (vertex_id target : entry.second) {
                if (source < target) {  // Write each edge once
                    partition_id targetPart = vertexToPartition[target];
                    
                    // Write to source partition
                    partitionFiles[sourcePart] << source << " " << target << "\n";
                    
                    // If cross-partition edge, also write to target partition
                    if (sourcePart != targetPart) {
                        partitionFiles[targetPart] << source << " " << target << "\n";
                    }
                }
            }
        }
        
        // Close all files
        for (auto &file : partitionFiles) {
            file.close();
        }
        
        sheep_partitioner_logger.info("Successfully wrote " + to_string(numPartitions) + 
                                     " partition files");
        return true;
        
    } catch (const std::exception &e) {
        sheep_partitioner_logger.error("Error writing partitions: " + string(e.what()));
        return false;
    }
}

bool SheepPartitioner::partitionGraph(int graphID, const string &graphPath, 
                                     const string &outputPath, int numPartitions) {
    sheep_partitioner_logger.info("Starting sheep partitioning for graph ID: " + to_string(graphID));
    sheep_partitioner_logger.info("Input graph: " + graphPath);
    sheep_partitioner_logger.info("Output path: " + outputPath);
    sheep_partitioner_logger.info("Number of partitions: " + to_string(numPartitions));
    
    this->numPartitions = numPartitions;
    
    // Check if input graph exists
    if (!std::filesystem::exists(graphPath)) {
        sheep_partitioner_logger.error("Input graph file does not exist: " + graphPath);
        return false;
    }
    
    // Load the graph
    if (!loadGraph(graphPath)) {
        return false;
    }
    
    // Assign vertices to partitions
    assignPartitions();
    
    // Calculate edge cuts
    calculateEdgeCuts();
    
    // Write partitions to files
    if (!writePartitions(outputPath)) {
        return false;
    }
    
    // Update metadata database
    try {
        string sqlStatement = "UPDATE graph SET partition_algo='sheep' WHERE idgraph=" + 
                            to_string(graphID);
        sqlite->runUpdate(sqlStatement);
        
        sheep_partitioner_logger.info("Successfully partitioned graph " + to_string(graphID) + 
                                     " using sheep algorithm");
    } catch (const std::exception &e) {
        sheep_partitioner_logger.error("Error updating database: " + string(e.what()));
        return false;
    }
    
    return true;
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
