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
#include <unordered_map>
#include <vector>
#include <unordered_set>
#include <set>
#include "metis.h"
#include "../../metadb/SQLiteDBInterface.h"
#include "../../util/Utils.h"
#include "../../localstore/JasmineGraphHashMapLocalStore.h"
#include <cstddef>
#include <algorithm>
#include <thread>
#include <mutex>
#include "RDFParser.h"
#include <flatbuffers/util.h>


using std::string;


class MetisPartitioner {
public:
    void loadDataSet(string inputFilePath, int graphID);

    //void partitionGraph();
    int constructMetisFormat(string graph_type);

    std::vector<std::map<int,std::string>> partitioneWithGPMetis(string partitionCount);

    //reformat the vertex list by mapping vertex values to new sequntial IDs
    std::string reformatDataSet(string inputFilePath, int graphID);

    void loadContentData(string inputAttributeFilePath, string graphAttributeType,int graphID, string attrType);

    MetisPartitioner(SQLiteDBInterface *);


private:
    idx_t edgeCount = 0;
    idx_t edgeCountForMetis = 0;
    idx_t largestVertex = 0;
    idx_t vertexCount = 0;
    int nParts = 0;
    string outputFilePath;
    bool zeroflag = false;
    SQLiteDBInterface sqlite;
    int graphID;
    Utils utils;
    string graphType;
    int smallestVertex = std::numeric_limits<int>::max();
    string graphAttributeType;

    std::map<int,std::string> partitionFileList;
    std::map<int,std::string> centralStoreFileList;
    std::map<int,std::string> compositeCentralStoreFileList;
    std::map<int,std::string> centralStoreDuplicateFileList;
    std::map<int,std::string> partitionAttributeFileList;
    std::map<int,std::string> centralStoreAttributeFileList;
    std::vector<std::map<int,std::string>> fullFileList;

    std::map<int, std::vector<int>> graphStorageMap;
    std::map<int, std::vector<int>> graphEdgeMap;
    std::unordered_map<int, size_t> partVertexCounts;
    std::unordered_map<int, size_t> masterEdgeCounts;
    std::unordered_map<int, size_t> masterEdgeCountsWithDups;
    std::map<int, std::map<int, std::vector<int>>> partitionedLocalGraphStorageMap;
    std::map<int, std::map<int, std::vector<int>>> masterGraphStorageMap;
    std::map<string, std::map<int, std::vector<int>>> compositeMasterGraphStorageMap;
    std::map<int, std::map<int, std::vector<int>>> duplicateMasterGraphStorageMap;
    std::map<int, std::map<int,std::map<int, std::vector<int>>>> commonCentralStoreEdgeMap;
    std::vector<int> xadj;
    std::vector<int> adjncy;
    std::map<std::pair<int, int>, int> edgeMap;
    std::map<long, string[7]> articlesMap;
    std::map<int, int> vertexToIDMap;
    std::map<int, int> idToVertexMap;
    std::map<int, std::string> attributeDataMap;


    void createPartitionFiles(std::map<int,int> partMap);

    void populatePartMaps(std::map<int,int> partMap, int part);

    void writePartitionFiles(int part);

    void writeMasterFiles(int part);

    void writeSerializedMasterFiles(int part);

    void writeSerializedCompositeMasterFiles(std::string part);

    void writeSerializedDuplicateMasterFiles(int part);

    void writeSerializedPartitionFiles(int part);

    void writeRDFAttributeFilesForPartitions(int part);

    void writeRDFAttributeFilesForMasterParts(int part);

    void writeTextAttributeFilesForPartitions(int part);

    void writeTextAttributeFilesForMasterParts(int part);
};


#endif //JASMINGRAPH_METISPARTITIONER_H
