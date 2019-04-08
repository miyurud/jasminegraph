/**
Copyright 2019 JasmineGraph Team
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
#include <string.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <map>
#include <vector>
#include <unordered_set>
#include <set>
#include "metis.h"
#include "../../metadb/SQLiteDBInterface.h"
#include "../../util/Utils.h"
#include <cstddef>
#include <algorithm>
#include <thread>
#include "RDFParser.h"
#include <flatbuffers/util.h>


using std::string;


class MetisPartitioner {
public:
    void loadDataSet(string inputFilePath, int graphID);

    //void partitionGraph();
    void constructMetisFormat();

    void partitioneWithGPMetis();

    //return list of partition files
    static std::vector<string> getPartitionFiles();

    //return list of centralStore files
    static std::vector<string> getCentalStoreFiles();

    MetisPartitioner(SQLiteDBInterface *);


private:
    idx_t edgeCount = 0;
    idx_t largestVertex = 0;
    idx_t vertexCount = 0;
    //TODO:Need to remove this hardcoded value
    idx_t nParts = 4;
    string outputFilePath;
    bool zeroflag = false;
    SQLiteDBInterface sqlite;
    int graphID;
    Utils utils;

    std::map<int, std::vector<int>> graphStorageMap;
    std::map<int, std::vector<int>> graphEdgeMap;
    std::map<int, std::vector<int>> partVertexMap;
    std::map<int, std::map<int, std::vector<int>>> partitionedLocalGraphStorageMap;
    std::map<int, std::map<int, std::vector<int>>> masterGraphStorageMap;
    std::vector<int> xadj;
    std::vector<int> adjncy;
    std::map<std::pair<int, int>, int> edgeMap;
    std::map<long, string[7]> articlesMap;



    void createPartitionFiles(idx_t part[]);
};


#endif //JASMINGRAPH_METISPARTITIONER_H
