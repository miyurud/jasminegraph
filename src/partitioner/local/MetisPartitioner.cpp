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

#include "MetisPartitioner.h"

void MetisPartitioner::loadDataSet(string inputFilePath, string outputFilePath) {
    this->outputFilePath = outputFilePath;
    std::ifstream dbFile;
    dbFile.open(inputFilePath,std::ios::binary | std::ios::in);

    int firstVertex = -1;
    int secondVertex = -1;
    string line;
    char splitter;

    std::getline(dbFile, line);

    if (!line.empty()) {
        if (line.find(" ") != std::string::npos) {
            splitter = ' ';
        } else if (line.find('\t') != std::string::npos) {
            splitter = '\t';
        } else if (line.find(",") != std::string::npos) {
            splitter = ',';
        }
    }

    while (!line.empty()) {
        string vertexOne;
        string vertexTwo;

        std::istringstream stream(line);
        std::getline(stream, vertexOne, splitter);
        stream >> vertexTwo;

        firstVertex = atoi(vertexOne.c_str());
        secondVertex = atoi(vertexTwo.c_str());

        std::vector<int> firstEdgeSet = graphStorageMap[firstVertex];
        std::vector<int> secondEdgeSet = graphStorageMap[secondVertex];

        std::vector<int> vertexEdgeSet = graphEdgeMap[firstVertex];

        if (firstEdgeSet.empty()) {
            vertexCount++;
            edgeCount++;
            firstEdgeSet.push_back(secondVertex);
            vertexEdgeSet.push_back(secondVertex);

        } else {
            if (std::find(firstEdgeSet.begin(),firstEdgeSet.end(),secondVertex) == firstEdgeSet.end()) {
                firstEdgeSet.push_back(secondVertex);
                vertexEdgeSet.push_back(secondVertex);
                edgeCount++;
            }
        }

        if (secondEdgeSet.empty()){
            vertexCount++;
            secondEdgeSet.push_back(firstVertex);

        } else {
            if (std::find(secondEdgeSet.begin(),secondEdgeSet.end(),firstVertex) == secondEdgeSet.end()) {
                secondEdgeSet.push_back(firstVertex);
            }
        }

        graphStorageMap[firstVertex] = firstEdgeSet;
        graphStorageMap[secondVertex] = secondEdgeSet;
        graphEdgeMap[firstVertex] = vertexEdgeSet;


        if (firstVertex > largestVertex) {
            largestVertex = firstVertex;
        }
        if (secondVertex > largestVertex) {
            largestVertex = secondVertex;
        }

        std::getline(dbFile, line);
        while(!line.empty() && line.find_first_not_of(splitter) == std::string::npos) {
            std::getline(dbFile, line);
        }
    }

}

void MetisPartitioner::constructMetisFormat() {
    int adjacencyIndex = 0;
    xadj.push_back(adjacencyIndex);
    for (int vertexNum = 0; vertexNum <= largestVertex; vertexNum++) {
        std::vector<int> vertexSet = graphStorageMap[vertexNum];
        std::sort(vertexSet.begin(),vertexSet.end());

        int edgeSize = vertexSet.size();
        if (edgeSize == 0) {
            xadj.push_back(adjacencyIndex);
        } else {
            std::copy(vertexSet.begin(),vertexSet.end(),std::back_inserter(adjncy));
            adjacencyIndex = adjacencyIndex+edgeSize;
            xadj.push_back(adjacencyIndex);
        }
    }

    /*for (std::vector<int>::const_iterator i = xadj.begin(); i != xadj.end(); ++i)
        std::cout << *i << ' ';

    std::cout << "\n";

    for (std::vector<int>::const_iterator i = adjncy.begin(); i != adjncy.end(); ++i)
        std::cout << *i << ' ';*/
}

void MetisPartitioner::partitionGraph() {
    idx_t part[vertexCount];

    idx_t xadjArray[xadj.size()];
    std::copy(xadj.begin(),xadj.end(),xadjArray);

    idx_t adjacencyArray[adjncy.size()];
    std::copy(adjncy.begin(),adjncy.end(),adjacencyArray);
    int ret = METIS_PartGraphKway(&vertexCount,&nWeights,xadjArray,adjacencyArray, NULL, NULL, NULL, &nParts, NULL,NULL, NULL, &objVal, part);

    /*for(unsigned part_i = 0; part_i < vertexCount; part_i++){
        std::cout << part_i << " " << part[part_i] << std::endl;
    }*/
}

void MetisPartitioner::createPartitionFiles(idx_t *part) {
    for (int vertex = 0;vertex<vertexCount;vertex++) {
        std::cout << vertex << " " << part[vertex] << std::endl;
        idx_t vertexPart = part[vertex];

        std::vector<int> partVertexSet = partVertexMap[vertexPart];

        partVertexSet.push_back(vertex);

        partVertexMap[vertexPart] = partVertexSet;
    }

    for (int vertex = 0;vertex<vertexCount;vertex++) {
        std::vector<int> vertexEdgeSet = graphEdgeMap[vertex];
        idx_t firstVertexPart = part[vertex];

        if (!vertexEdgeSet.empty()) {
            std::vector<int>::iterator it;
            for (it = vertexEdgeSet.begin(); it != vertexEdgeSet.end(); ++it) {
                int secondVertex = *it;
                int secondVertexPart = part[secondVertex];

                if (firstVertexPart == secondVertexPart) {
                    std::map<int,std::vector<int>> partEdgesSet = partitionedLocalGraphStorageMap[firstVertexPart];
                    std::vector<int> edgeSet = partEdgesSet[vertex];
                    edgeSet.push_back(secondVertex);
                    partEdgesSet[vertex] = edgeSet;
                    partitionedLocalGraphStorageMap[firstVertexPart] = partEdgesSet;
                } else {
                    std::map<int,std::vector<int>> partMasterEdgesSet = masterGraphStorageMap[firstVertexPart];
                    std::vector<int> edgeSet = partMasterEdgesSet[vertex];
                    edgeSet.push_back(secondVertex);
                    partMasterEdgesSet[vertex] = edgeSet;
                    masterGraphStorageMap[firstVertexPart] = partMasterEdgesSet;
                }
            }
        }

    }

    for (int part = 0;part<nParts;part++) {
        string outputFilePart = outputFilePath+std::to_string(part);
        string outputFilePartMaster = outputFilePath+std::to_string(part);

        std::map<int,std::vector<int>> partEdgeMap = partitionedLocalGraphStorageMap[part];
        std::map<int,std::vector<int>> partMasterEdgeMap = masterGraphStorageMap[part];

        if (!partEdgeMap.empty()) {
            std::ofstream localFile(outputFilePart);

            if (localFile.is_open()) {
                for (int vertex = 0; vertex < vertexCount;vertex++) {
                    std::vector<int> destinationSet = partEdgeMap[vertex];
                    if (!destinationSet.empty()) {
                        for (std::vector<int>::iterator itr = destinationSet.begin(); itr != destinationSet.end(); ++itr) {
                            string edge = std::to_string(vertex) + " " + std::to_string((*itr));
                            localFile<<edge;
                            localFile<<"\n";

                        }
                    }
                }
            }

            localFile.close();

        }

        if (!partEdgeMap.empty()) {
            std::ofstream masterFile(outputFilePartMaster);

            if (masterFile.is_open()) {
                for (int vertex = 0; vertex < vertexCount;vertex++) {
                    std::vector<int> destinationSet = partMasterEdgeMap[vertex];
                    if (!destinationSet.empty()) {
                        for (std::vector<int>::iterator itr = destinationSet.begin(); itr != destinationSet.end(); ++itr) {
                            string edge = vertex + " " + (*itr);
                            masterFile<<edge;
                            masterFile<<"\n";

                        }
                    }
                }
            }

            masterFile.close();

        }


    }
}
