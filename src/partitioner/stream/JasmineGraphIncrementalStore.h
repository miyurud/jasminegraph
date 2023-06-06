/*
 * Copyright 2019 JasminGraph Team
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
#include <unordered_set>
#include <vector>

#ifndef JASMINE_PARTITION
#define JASMINE_PARTITION

class JasmineGraphIncrementalStore {
    /*
    What is set good for? (Source: http://lafstern.org/matt/col1.pdf)
        - The collection can potentially grow so large that the difference between O(N) and O(log N) is important.
        - The number of lookups is the same order of magnitude as the number of insertions; there aren't so few
            insertions that insertion speed is irrelevant.
        - Elements are inserted in random order, rather than being inserted in order.
        - Insertions and lookups are interleaved; we don't have distinct insertion and lookup phases.
    */
    std::map<int, std::set<int>> edgeList;
    /**
     * Edge cuts data structure
     * [id]                    [id]                 ...       [id]
     *  |                       |                              |
     *  ↓                       ↓                              ↓
     * {res->(foreign,..)} {res->(foreign,..) }     ...   {res->(foreign,..) }
     * 
     * **/
    std::vector<std::map<int, std::unordered_set<int>>> edgeCuts;
    int id;
    int numberOfPartitions;  // Size of the cluster TODO: can be removed

   public:
    JasmineGraphIncrementalStore(int id, int numberOfPartitions) {
        this->id = id;
        this->numberOfPartitions = numberOfPartitions;
        for (size_t i = 0; i < numberOfPartitions; i++) {
            this->edgeCuts.push_back({});
        }
    };
    void addEdge(std::pair<int, int> edge);
    std::set<int> getNeighbors(int);
    double partitionScore(int vertex);
    double getEdgesCount();
    double getVertextCount();
    void addToEdgeCuts(int resident, int foriegn, int partitionId);
    float edgeCutsRatio();
    template <typename Out>
    static void _split(const std::string &s, char delim, Out result);
    static std::vector<std::string> _split(const std::string &s, char delim);
    long edgeCutsCount();
    void printEdgeCuts();
    void printEdges();
    bool isExist(double vertext);
};

#endif