/*
 * Copyright 2021 JasminGraph Team
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

#include <map>
#include <set>
#include <string>
#include <unordered_set>
#include <vector>
#include <atomic>
#include <mutex>

#ifndef JASMINE_PARTITION
#define JASMINE_PARTITION

class Partition {
    /*
    What is set good for? (Source: http://lafstern.org/matt/col1.pdf)
        - The collection can potentially grow so large that the difference between O(N) and O(log N) is important.
        - The number of lookups is the same order of magnitude as the number of insertions; there aren't so few
            insertions that insertion speed is irrelevant.
        - Elements are inserted in random order, rather than being inserted in order.
        - Insertions and lookups are interleaved; we don't have distinct insertion and lookup phases.
    */
    std::map<std::string, std::set<std::string>> edgeList;
    /**
     * Edge cuts data structure
     * [id]                    [id]                 ...       [id]
     *  |                       |                              |
     *  ↓                       ↓                              ↓
     * {res->(foreign,..)} {res->(foreign,..) }     ...   {res->(foreign,..) }
     *
     * **/
    std::vector<std::map<std::string, std::unordered_set<std::string>>> edgeCuts;
    int id;
    int numberOfPartitions;  // Size of the cluster TODO: can be removed

    std::atomic<int> vertexCount;
    mutable std::mutex partitionMutex;  // Mutex for thread-safe operations

 public:
    Partition(int id, int numberOfPartitions) {
        this->id = id;
        this->numberOfPartitions = numberOfPartitions;
        for (size_t i = 0; i < numberOfPartitions; i++) {
            this->edgeCuts.push_back({});
        }
        this->vertexCount = 0;
    };
    
    // Copy constructor - needed for compatibility
    Partition(const Partition& other) 
        : edgeList(other.edgeList),
          edgeCuts(other.edgeCuts),
          id(other.id),
          numberOfPartitions(other.numberOfPartitions),
          vertexCount(other.vertexCount.load()) {
    }
    
    // Copy assignment operator
    Partition& operator=(const Partition& other) {
        if (this != &other) {
            edgeList = other.edgeList;
            edgeCuts = other.edgeCuts;
            id = other.id;
            numberOfPartitions = other.numberOfPartitions;
            vertexCount = other.vertexCount.load();
        }
        return *this;
    }
    
    // Move constructor
    Partition(Partition&& other) noexcept 
        : edgeList(std::move(other.edgeList)),
          edgeCuts(std::move(other.edgeCuts)),
          id(other.id),
          numberOfPartitions(other.numberOfPartitions),
          vertexCount(other.vertexCount.load()) {
    }
    
    // Move assignment operator
    Partition& operator=(Partition&& other) noexcept {
        if (this != &other) {
            edgeList = std::move(other.edgeList);
            edgeCuts = std::move(other.edgeCuts);
            id = other.id;
            numberOfPartitions = other.numberOfPartitions;
            vertexCount = other.vertexCount.load();
        }
        return *this;
    }
    void addEdge(std::pair<std::string, std::string> edge, bool isDirected = false);
    std::set<std::string> getNeighbors(std::string);
    double partitionScore(std::string vertex);
    double getEdgesCount(bool isDirected = false);
    double getVertextCount();
    double getVertextCountQuick();
    void addToEdgeCuts(std::string resident, std::string foreign, int partitionId);
    float edgeCutsRatio();
    template <typename Out>
    static void _split(const std::string &s, char delim, Out result);
    static std::vector<std::string> _split(const std::string &s, char delim);
    long edgeCutsCount(bool isDirected = true);
    long getCentralVertexCount(int partitionIndex);
    long getLocalVertexCount();
    void printEdgeCuts();
    void printEdges();
    bool isExist(std::string);
    bool isExistInEdgeCuts(std::string);
    void incrementVertexCount();
    void addToEdgeList(std::string vertex);
    std::mutex& getPartitionMutex() const { return partitionMutex; }  // Getter for mutex
    
private:
    bool isExistUnsafe(std::string vertext);  // Unsafe version - assumes caller holds lock
    bool isExistInEdgeCutsUnsafe(std::string vertext);  // Unsafe version - assumes caller holds lock
};

#endif
