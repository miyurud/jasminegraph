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
    void loadDataSet(string filePath);
    void partitionGraph();

private:
    idx_t edgeCount;
    idx_t largestVertex;
    idx_t vertexCount;
    idx_t nWeights = 1;
    idx_t nParts = 3;
    idx_t objVal;


    std::map<int,std::set<int>> graphStorageMap;
    std::vector<int> xadj;
    std::vector<int> adjncy;

    void constructMetisFormat();
};


#endif //JASMINGRAPH_METISPARTITIONER_H
