#include <algorithm>
#include <cmath>
#include <iostream>
#include <vector>

#include "Partitioner.h"

void Partitioner::addEdge(std::pair<long, long> edge) {
    switch (this->algorithmInUse) {
        case Algorithms::HASH:
            this->hashPartitioning(edge);
            break;
        case Algorithms::FENNEL:
            this->fennelPartitioning(edge);
            break;
        // case Algorithms::LDG:
        //     this->ldgPartitioning(edge);
        //     break;
        default:
            break;
    }
}

void Partitioner::hashPartitioning(std::pair<int, int> edge) {
    int firstIndex = edge.first % this->numberOfPartitions;    // Hash partitioning
    int secondIndex = edge.second % this->numberOfPartitions;  // Hash partitioning

    if (firstIndex == secondIndex) {
        this->partitions[firstIndex].addEdge(edge);
    } else {
        this->partitions[firstIndex].addToEdgeCuts(edge.first, edge.second, secondIndex);
        this->partitions[secondIndex].addToEdgeCuts(edge.second, edge.first, firstIndex);
    }
}

void Partitioner::printStats() {
    int id = 0;
    for (auto partition : this->partitions) {
        std::cout << id << " => Vertext count = " << partition.getVertextCount() << std::endl;
        std::cout << id << " => Edges count = " << partition.getEdgesCount() << std::endl;
        std::cout << id << " => Edge cuts count = " << partition.edgeCutsCount() << std::endl;
        std::cout << id << " => Cut ratio = " << partition.edgeCutsRatio() << std::endl;
        partition.printEdgeCuts();
        partition.printEdges();
        id++;
    }
}

/**
 * Greedy vertex assignment objectives of minimizing the number of cut edges
and balancing of the partition sizes. Assign the vertext to partition P that maximize the partition score
 * |N(v) ∩ Si| + ∂c(|Si|)
 * |N(v) ∩ Si| =  number of neighbours of vertex v that are assigned to partition Si.
 * ∂c(|Si|) = the marginal cost of increasing the partition i by one additional vertex.
 *
 * Intra-partition cost function : c(x) = αx^γ, for α > 0 and γ ≥ 1 where x = vertex cardinality
 *   α = m*(k^γ-1) / (n^γ)
 *   Total number of vertices and edges in the graph denoted as |V| = n and |E| = m.
 *   k is number of partitions
 **/
void Partitioner::fennelPartitioning(std::pair<int, int> edge) {
    std::vector<double> partitionScoresFirst(numberOfPartitions, 0);   // Calculate per incoming edge
    std::vector<double> partitionScoresSecond(numberOfPartitions, 0);  // Calculate per incoming edge
    const double gamma = 3 / 2.0;
    const double alpha =
        this->totalEdges * pow(this->numberOfPartitions, (gamma - 1)) / pow(this->totalVertices, gamma);
    bool firstVertextAlreadyExist(false);
    bool secondVertextAlreadyExist(false);

    int id = 0;
    for (auto partition : partitions) {
        double partitionSize = partition.getVertextCount();
        long thisCostSecond, thisCostFirst = 0;
        std::set<int> firstVertextNeighbors = partition.getNeighbors(edge.first);
        std::set<int> secondVertextNeighbors = partition.getNeighbors(edge.second);
        double firstVertextIntraCost;
        double secondVertextIntraCost;
        if (partition.isExist(edge.first) && partition.isExist(edge.second)) {
            partition.addEdge(edge);
            this->totalEdges += 1; // TODO: Check whether edge already exist
            return;
        }
        double firstVertextInterCost = firstVertextNeighbors.size();
        double secondVertextInterCost = secondVertextNeighbors.size();
        if (firstVertextNeighbors.size() == 0) {
            // firstVertextIntraCost = alpha * gamma * pow(firstVertextNeighbors.size(), (gamma - 1));
            firstVertextIntraCost = alpha * (pow(partitionSize + 1, gamma) - pow(partitionSize, gamma));
        } else {
            if (firstVertextNeighbors.find(edge.second) != firstVertextNeighbors.end())
                return;  // Nothing to do, edge already exisit
            firstVertextAlreadyExist = true;
            firstVertextIntraCost = 0;  // No cost of adding the vertext to this partition because it's already exist
        }

        partitionScoresFirst[id] = firstVertextInterCost - firstVertextIntraCost;

        if (secondVertextNeighbors.size() == 0) {
            // secondVertextIntraCost = alpha * gamma * pow(secondVertextNeighbors.size(), (gamma - 1));
            secondVertextIntraCost = firstVertextIntraCost;
        } else {
            if (secondVertextNeighbors.find(edge.second) != secondVertextNeighbors.end())
                return;  // Nothing to do, edge already exisit
            secondVertextAlreadyExist = true;
            secondVertextIntraCost = 0;  // No cost of adding the vertext this partition because it's already exist
        }

        partitionScoresSecond[id] = secondVertextInterCost - secondVertextIntraCost;
        id++;
    }
    if (!firstVertextAlreadyExist) this->totalVertices += 1;
    if (!secondVertextAlreadyExist) this->totalVertices += 1;

    int firstIndex =
        distance(partitionScoresFirst.begin(), max_element(partitionScoresFirst.begin(), partitionScoresFirst.end()));

    // partitionScoresSecond[firstIndex] -= alpha * (pow(partitions[firstIndex].getVertextCount() + 2, gamma) -
    //                                               pow(partitions[firstIndex].getVertextCount() + 1, gamma));
    int secondIndex = distance(partitionScoresSecond.begin(),
                               max_element(partitionScoresSecond.begin(), partitionScoresSecond.end()));
    if (firstIndex == secondIndex) {
        partitions[firstIndex].addEdge(edge);
    } else {
        partitions[firstIndex].addToEdgeCuts(edge.first, edge.second, secondIndex);
        partitions[secondIndex].addToEdgeCuts(edge.second, edge.first, firstIndex);
    }
    this->totalEdges += 1;
}

/**
 * Expect a space seperated pair of vertexts representing an edge in the graph.
 **/
std::pair<long, long> Partitioner::deserialize(std::string data) {
    std::vector<std::string> v = Partition::_split(data, ' ');
    std::cout << "Vertext 1 = " << stoi(v[0]) << std::endl;
    std::cout << "Vertext 2 = " << stoi(v[1]) << std::endl;
    return {stoi(v[0]), stoi(v[1])};
}
