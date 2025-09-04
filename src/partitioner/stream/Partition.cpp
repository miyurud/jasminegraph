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

#include "Partition.h"

#include <iostream>
#include <sstream>
#include <vector>
#include <string>

#include "../../util/logger/Logger.h"

Logger streaming_partition_logger;

// This addition is unidirectional , Add both items of the pair as keys
void Partition::addEdge(std::pair<std::string, std::string> edge, bool isDirected) {
    auto existFirstVertex = this->edgeList.find(edge.first);
    if (existFirstVertex != this->edgeList.end()) {
        this->edgeList[edge.first].insert(edge.second);
    } else {
        this->edgeList[edge.first] = std::set<std::string>({edge.second});

        if (!isExistInEdgeCuts(edge.first)) {
            this->vertexCount += 1;
        }
    }

    if (isDirected) {
        auto existSecondVertex = this->edgeList.find(edge.second);
        if (existSecondVertex == this->edgeList.end() && !isExistInEdgeCuts(edge.second)) {
            this->vertexCount += 1;
            this->edgeList[edge.second] = std::set<std::string>();
        }
    } else {
        auto existSecondVertex = this->edgeList.find(edge.second);
        if (existSecondVertex != this->edgeList.end()) {
            this->edgeList[edge.second].insert(edge.first);
        } else {
            this->edgeList[edge.second] = std::set<std::string>({edge.first});

            if (!isExistInEdgeCuts(edge.second)) {
                this->vertexCount += 1;
            }
        }
    }
}

std::set<std::string> Partition::getNeighbors(std::string vertex) {
    auto exsist = this->edgeList.find(vertex);
    if (exsist != this->edgeList.end()) {
        return this->edgeList[vertex];
    }
    return {};
}

// The number of edges, the cardinality of E, is called the size of graph and denoted by |E|. We usually use m to denote
// the size of G.
double Partition::getEdgesCount(bool isDirected) {
    double total = 0;
    std::set<std::string> uniqueEdges;
    for (auto edge : this->edgeList) {
        std::string vertex1 = edge.first;
        for (auto vertext : edge.second) {
            // Use a delimiter to avoid accidental merging of IDs
            uniqueEdges.insert(vertex1 + "," + vertext);
        }
    }
    if (isDirected) {
        return  uniqueEdges.size();
    }
    return uniqueEdges.size()/2;
}

// The number of vertices, the cardinality of V, is called the order of graph and devoted by |V|. We usually use n to
// denote the order of G.
double Partition::getVertextCount() {
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

double Partition::getVertextCountQuick() { return this->vertexCount; }

template <typename Out>
void Partition::_split(const std::string &s, char delim, Out result) {
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        *(result++) = item;
    }
}

std::vector<std::string> Partition::_split(const std::string &s, char delim) {
    std::vector<std::string> elems;
    Partition::_split(s, delim, std::back_inserter(elems));
    return elems;
}

void Partition::addToEdgeCuts(std::string resident, std::string foreign, int partitionId) {
    if (partitionId < this->numberOfPartitions) {
        if (!isExist(resident)) {
            this->vertexCount += 1;
        }
        auto exsistResidentVertex = this->edgeCuts[partitionId].find(resident);
        if (exsistResidentVertex != this->edgeCuts[partitionId].end()) {
            this->edgeCuts[partitionId][resident].insert(foreign);
        } else {
            this->edgeCuts[partitionId][resident] = std::unordered_set<std::string>({foreign});
        }
    }
}

long Partition::edgeCutsCount(bool isDirected) {
    long total = 0;
    for (auto partition : this->edgeCuts) {
        for (auto edgeCuts : partition) {
            total += edgeCuts.second.size();
        }
    }
    
    if (isDirected) {
        return total;
    }
    return total / 2;  // For undirected graphs, each edge is counted twice
}

float Partition::edgeCutsRatio() { return this->edgeCutsCount() / (this->getEdgesCount() + this->edgeCutsCount()); }

void Partition::printEdgeCuts() {
    streaming_partition_logger.debug("Printing edge cuts of " + std::to_string(id) + " partition");

    for (auto partition : this->edgeCuts) {
        for (auto edgeList : partition) {
            streaming_partition_logger.debug("edgeList.first " + edgeList.first + " ____");

            for (std::string vertext : edgeList.second) {
                streaming_partition_logger.debug("\t| ===> " + vertext);
            }
            streaming_partition_logger.debug("\n");
        }
    }
}

void Partition::printEdges() {
    streaming_partition_logger.debug("Printing edge list of " + std::to_string(id) + " partition");
    std::unordered_set<std::string> compositeVertextIDs;
    for (auto edgeList : this->edgeList) {
        bool isFirst = true;
        for (std::string vertext : edgeList.second) {
            std::string compositeVertextID = edgeList.first + "," + vertext;
            if (compositeVertextIDs.find(compositeVertextID) == compositeVertextIDs.end()) {
                if (isFirst) {
                    streaming_partition_logger.debug("edgeList.first " + edgeList.first + " ____");
                    isFirst = false;
                }
                streaming_partition_logger.debug("\t| ===> " + vertext);
                compositeVertextIDs.insert(compositeVertextID);
            }
        }
        streaming_partition_logger.debug("\n");
    }
}

bool Partition::isExist(std::string vertext) {
    bool inEdgeList = this->edgeList.find(vertext) != this->edgeList.end();
    bool inEdgeCuts = false;
    for (size_t i = 0; i < this->numberOfPartitions; i++) {
        if (this->edgeCuts[i].find(vertext) != this->edgeCuts[i].end()) {
            inEdgeCuts = true;
            break;
        }
    }
    return inEdgeCuts || inEdgeList;
}

bool Partition::isExistInEdgeCuts(std::string vertext) {
    bool inEdgeCuts = false;
    for (size_t i = 0; i < this->numberOfPartitions; i++) {
        if (this->edgeCuts[i].find(vertext) != this->edgeCuts[i].end()) {
            inEdgeCuts = true;
            break;
        }
    }
    return inEdgeCuts;
}

long Partition::getCentralVertexCount(int partitionIndex) {
    long edgeCutVertices = 0;
    for (auto edge : this->edgeCuts[partitionIndex]) {
        bool isExistInEdgeList = this->edgeList.find(edge.first) != this->edgeList.end();
        if (!isExistInEdgeList) {
            edgeCutVertices += 1;
        }
    }
    return edgeCutVertices;
}

long Partition::getLocalVertexCount() {
    return static_cast<long>(edgeList.size());
}

void Partition::incrementVertexCount() {
    this->vertexCount += 1;
}

