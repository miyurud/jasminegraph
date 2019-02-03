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

#ifndef JASMINEGRAPH_RDFPARTITIONER_H
#define JASMINEGRAPH_RDFPARTITIONER_H


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

using std::string;


class RDFPartitioner {
public:
    void convert(string graphName, string graphID, string inputFilePath,
                 string outputFilePath,
                 int nParts,
                 bool isDistributedCentralPartitions,
                 int nThreads,
                 int nPlaces);

    void convertWithoutDistribution(string graphName,
                                    int graphID,
                                    string inputFilePath,
                                    string outputFilePath,
                                    int nParts,
                                    bool isDistributedCentralPartitions,
                                    int nThreads,
                                    int nPlaces);

    void convertWithoutDistribution(string graphName, string graphID, string inputFilePath, string outputFilePath,
                                    int nParts, bool isDistributedCentralPartitions, int nThreads, int nPlaces);

    void loadDataSet(string inputFilePath, string outputFilePath);

    void distributeEdges();
private:

    string outputFilePath;
    int nParts;
    string graphName;
    string graphID;
    int nThreads;
    int nPlaces;

    string inputFilePath;

    Utils utils;


    std::map<string, long> nodes;
    std::map<long, string> nodesTemp;
    std::map<string, long> predicates;
    std::map<long, string> predicatesTemp;
    std::map<long, std::vector<string>> relationsMap;
    std::map<long, std::vector<string>> attributeMap;

};


#endif //JASMINEGRAPH_RDFPARTITIONER_H
