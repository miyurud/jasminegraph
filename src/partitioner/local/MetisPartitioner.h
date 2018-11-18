//
// Created by chinthaka on 11/11/18.
//

#ifndef JASMINGRAPH_METISPARTITIONER_H
#define JASMINGRAPH_METISPARTITIONER_H

#include <string>
#include <fstream>
#include <iostream>
#include <sstream>
#include <map>
#include <vector>
#include <unordered_set>
#include <set>
#include "metis.h"
#include <cstddef>

using std::string;


class MetisPartitioner {
public:
    void loadDataSet(string inputFilePath, string outputFilePath);
    void partitionGraph();

private:
    idx_t edgeCount;
    idx_t largestVertex;
    idx_t vertexCount;
    idx_t nWeights = 1;
    idx_t nParts = 3;
    idx_t objVal;
    string outputFilePath;


    std::map<int,std::set<int>> graphStorageMap;
    std::map<int,std::set<int>> graphEdgeMap;
    std::map<int,std::set<int>> partVertexMap;
    std::map<int,std::map<int,std::set<int>>> partitionedLocalGraphStorageMap;
    std::map<int,std::map<int,std::set<int>>> masterGraphStorageMap;
    std::vector<int> xadj;
    std::vector<int> adjncy;

    void constructMetisFormat();
    void createPartitionFiles(idx_t part[]);
};


#endif //JASMINGRAPH_METISPARTITIONER_H
