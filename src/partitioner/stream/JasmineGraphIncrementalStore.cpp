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

#include "JasmineGraphIncrementalStore.h"
#include <iostream>
#include <sstream>
#include <vector>

// This addition is undirectional , Add both items of the pair as keys
void JasmineGraphIncrementalStore::addEdge(std::pair<int, int> edge) {
    auto exsistFirstVertext = this->edgeList.find(edge.first);
    if (exsistFirstVertext != this->edgeList.end()) {
        this->edgeList[edge.first].insert(edge.second);
    } else {
        this->edgeList[edge.first] = std::set<int>({edge.second});
    }

    auto exsistSecondVertext = this->edgeList.find(edge.second);
    if (exsistSecondVertext != this->edgeList.end()) {
        this->edgeList[edge.second].insert(edge.first);
    } else {
        this->edgeList[edge.second] = std::set<int>({edge.first});
    }
}

std::set<int> JasmineGraphIncrementalStore::getNeighbors(int vertex) {
    auto exsist = this->edgeList.find(vertex);
    if (exsist != this->edgeList.end()) {
        return this->edgeList[vertex];
    } else {
        return {};
    }
}

// The number of edges, the cardinality of E, is called the size of graph and denoted by |E|. We usually use m to denote
// the size of G.
double JasmineGraphIncrementalStore::getEdgesCount() {
    double total = 0;
    std::set<int> uniqueEdges;
    for (auto edge : this->edgeList) {
        for (auto vertext : edge.second) {
            uniqueEdges.insert(edge.first + vertext);
        }
    }
    return uniqueEdges.size();
}

// The number of vertices, the cardinality of V, is called the order of graph and devoted by |V|. We usually use n to
// denote the order of G.
double JasmineGraphIncrementalStore::getVertextCount() {
    double edgeListVetices = this->edgeList.size();
    double edgeCutVertices = 0;
    for (size_t i = 0; i < this->numberOfPartitions; i++) {
        for (auto edge : this->edgeCuts[i]) {
            bool isExistInEdgeList = this->edgeList.find(edge.first) != this->edgeList.end();
            if (!isExistInEdgeList) {
                edgeCutVertices += 1;
            }
        }
    }
    return edgeListVetices + edgeCutVertices;
}

template <typename Out>
void JasmineGraphIncrementalStore::_split(const std::string &s, char delim, Out result) {
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        *(result++) = item;
    }
}

std::vector<std::string> JasmineGraphIncrementalStore::_split(const std::string &s, char delim) {
    std::vector<std::string> elems;
    JasmineGraphIncrementalStore::_split(s, delim, std::back_inserter(elems));
    return elems;
}

void JasmineGraphIncrementalStore::addToEdgeCuts(int resident, int foreign, int partitionId) {
    if (partitionId < this->numberOfPartitions) {
        auto exsistResidentVertiext = this->edgeCuts[partitionId].find(resident);
        if (exsistResidentVertiext != this->edgeCuts[partitionId].end()) {
            this->edgeCuts[partitionId][resident].insert(foreign);
        } else {
            this->edgeCuts[partitionId][resident] = std::unordered_set<int>({foreign});
        }
    }
}

long JasmineGraphIncrementalStore::edgeCutsCount() {
    long total = 0;
    for (auto partition : this->edgeCuts) {
        for (auto edgeCuts : partition) {
            total += edgeCuts.second.size();
        }
    }
    return total;
}

float JasmineGraphIncrementalStore::edgeCutsRatio() { return this->edgeCutsCount() / (this->getEdgesCount() + this->edgeCutsCount()); }

void JasmineGraphIncrementalStore::printEdgeCuts() {
    std::cout << "Printing edge cuts of " << id << " partition" << std::endl;
    for (auto partition : this->edgeCuts) {
        for (auto edgeList : partition) {
            std::cout << edgeList.first << " ____" << std::endl;
            for (int vertext : edgeList.second) {
                std::cout << "\t| ---> " << vertext << std::endl;
            }
            std::cout << "\n" << std::endl;
        }
    }
}

void JasmineGraphIncrementalStore::printEdges() {
    std::cout << "Printing edge list of " << id << " partition" << std::endl;
    std::unordered_set<long> compositeVertextIDs;
    for (auto edgeList : this->edgeList) {
        bool isFirst = true;
        for (int vertext : edgeList.second) {
            long compositeVertextID = edgeList.first + vertext;
            if (compositeVertextIDs.find(compositeVertextID) == compositeVertextIDs.end()) {
                if (isFirst) {
                    std::cout << edgeList.first << " ____" << std::endl;
                    isFirst = false;
                }
                std::cout << "\t| ===> " << vertext << std::endl;
                compositeVertextIDs.insert(edgeList.first + vertext);
            }
        }
        std::cout << "\n" << std::endl;
    }
}

bool JasmineGraphIncrementalStore::isExist(double vertext) {
    bool inEdgeList = this->edgeList.find(vertext) != this->edgeList.end();
    bool inEdgeCuts = false;
    for (size_t i = 0; i < this->numberOfPartitions; i++) {
        inEdgeCuts = this->edgeCuts[i].find(vertext) != this->edgeCuts[i].end();
    }
    return inEdgeCuts || inEdgeList;
}
