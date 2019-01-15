/**
Copyright 2018 JasmineGraph Team
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

#ifndef JASMINEGRAPH_METISPARTITIONER_H
#define JASMINEGRAPH_METISPARTITIONER_H

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
#include <algorithm>

using std::string;


class MetisPartitioner {
public:
    void loadDataSet(string inputFilePath, string outputFilePath);
    //void partitionGraph();
    void constructMetisFormat();
    void partitioneWithGPMetis();

private:
    idx_t edgeCount;
    idx_t largestVertex;
    idx_t vertexCount;
    idx_t nWeights = 1;
    idx_t nParts = 3;
    idx_t objVal;
    string outputFilePath;
    bool zeroflag = false;


    std::map<int,std::vector<int>> graphStorageMap;
    std::map<int,std::vector<int>> graphEdgeMap;
    std::map<int,std::vector<int>> partVertexMap;
    std::map<int,std::map<int,std::vector<int>>> partitionedLocalGraphStorageMap;
    std::map<int,std::map<int,std::vector<int>>> masterGraphStorageMap;
    std::vector<int> xadj;
    std::vector<int> adjncy;

    void createPartitionFiles(idx_t part[]);
};


#endif //JASMINGRAPH_METISPARTITIONER_H
