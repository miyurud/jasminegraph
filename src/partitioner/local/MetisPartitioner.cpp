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

MetisPartitioner::MetisPartitioner(SQLiteDBInterface * sqlite) {
    this->sqlite = *sqlite;
}

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

        if (!zeroflag) {
            if (firstVertex == 0 || secondVertex == 0) {
                zeroflag = true;
                std::cout << "Graph have zero vertex." << std::endl;
            }
        }

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
    std::ofstream outputFile;
    outputFile.open("/tmp/grf");

    if (zeroflag){
        outputFile << (++vertexCount) << ' ' << (edgeCount) << std::endl;
    }else{
        outputFile << (vertexCount) << ' ' << (edgeCount) << std::endl;
    }


    xadj.push_back(adjacencyIndex);
    for (int vertexNum = 0; vertexNum <= largestVertex; vertexNum++) {
        std::vector<int> vertexSet = graphStorageMap[vertexNum];
        std::sort(vertexSet.begin(), vertexSet.end());

        for (std::vector<int>::const_iterator i = vertexSet.begin(); i != vertexSet.end(); ++i) {
            if (zeroflag) {
                outputFile << (*i + 1) << ' ';
            } else {
                outputFile << *i << ' ';
            }
        }

        outputFile << std::endl;
    }
}

void MetisPartitioner::partitioneWithGPMetis() {
    char buffer[128];
    std::string result = "";
    FILE *headerModify;
    FILE *input = popen("gpmetis /tmp/grf 4 2>&1", "r");
    if (input) {
        // read the input
        while (!feof(input)) {
            if (fgets(buffer, 128, input) != NULL) {
                result.append(buffer);
            }
        }
        pclose(input);
        if (!result.empty() && result.find("Premature") != std::string::npos) {
            vertexCount-=1;
            string newHeader = std::to_string(vertexCount)+ ' '+ std::to_string(edgeCount);
            string command = "sed -i \"1s/.*/" + newHeader +"/\" /tmp/grf";
            char * newHeaderChar = new char [command.length()+1];
            strcpy(newHeaderChar,command.c_str());
            headerModify = popen(newHeaderChar, "r");
            partitioneWithGPMetis();
        } else if (!result.empty() && result.find("out of bounds") != std::string::npos) {
            vertexCount+=1;
            string newHeader = std::to_string(vertexCount)+ ' '+ std::to_string(edgeCount);
            string command = "sed -i \"1s/.*/" + newHeader +"/\" /tmp/grf";
            char * newHeaderChar = new char [command.length()+1];
            strcpy(newHeaderChar,command.c_str());
            headerModify = popen(newHeaderChar, "r");
            partitioneWithGPMetis();
            //However, I only found
        } else if (!result.empty() && result.find("However, I only found") != std::string::npos) {
            string firstDelimiter = "I only found";
            string secondDelimite = "edges in the file";
            unsigned first = result.find(firstDelimiter);
            unsigned last = result.find(secondDelimite);
            string newEdgeSize = result.substr (first+firstDelimiter.length()+1,last-(first+firstDelimiter.length())-2);
            string newHeader = std::to_string(vertexCount)+ ' '+ newEdgeSize;
            string command = "sed -i \"1s/.*/" + newHeader +"/\" /tmp/grf";
            char * newHeaderChar = new char [command.length()+1];
            strcpy(newHeaderChar,command.c_str());
            headerModify = popen(newHeaderChar, "r");
            partitioneWithGPMetis();
            //However, I only found
        } else if (!result.empty() && result.find("Timing Information") != std::string::npos) {
            idx_t partIndex[vertexCount];
            std::string line;
            std::ifstream infile("/tmp/grf.part.4");
            int counter = 0;
            while (std::getline(infile, line)){
                std::istringstream iss(line);
                int a;
                if (!(iss >> a)) {
                    break;
                } else {
                    partIndex[counter] = a;
                    counter++;
                }
            }

            createPartitionFiles(partIndex);
        }



        perror("popen");
    } else {
        perror("popen");
        // handle error
    }
}

void MetisPartitioner::createPartitionFiles(idx_t *part) {
    Utils utils;
    std::vector<std::vector<std::pair<string,string>>> v = this->sqlite.runSelect("SELECT idgraph FROM graph ORDER BY idgraph LIMIT 1;");
    int newGraphID = atoi(v.at(0).at(0).second.c_str()) + 1;

    for (int vertex = 0;vertex<vertexCount;vertex++) {
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
        string outputFilePart = outputFilePath + "/" + std::to_string(newGraphID) + "_" + std::to_string(part);
        string outputFilePartMaster = outputFilePath + "/" + std::to_string(newGraphID) + "_centralstore_" + std::to_string(part);

        std::map<int, std::vector<int>> partEdgeMap = partitionedLocalGraphStorageMap[part];
        std::map<int, std::vector<int>> partMasterEdgeMap = masterGraphStorageMap[part];

        if (!partEdgeMap.empty()) {
            std::ofstream localFile(outputFilePart);

            if (localFile.is_open()) {
                for (int vertex = 0; vertex < vertexCount; vertex++) {
                    std::vector<int> destinationSet = partEdgeMap[vertex];
                    if (!destinationSet.empty()) {
                        for (std::vector<int>::iterator itr = destinationSet.begin();
                             itr != destinationSet.end(); ++itr) {
                            string edge = std::to_string(vertex) + " " + std::to_string((*itr));
                            localFile << edge;
                            localFile << "\n";
                        }
                    }
                }
            }

            localFile.flush();
            localFile.close();

            std::ofstream masterFile(outputFilePartMaster);

            if (masterFile.is_open()) {
                for (int vertex = 0; vertex < vertexCount; vertex++) {
                    std::vector<int> destinationSet = partMasterEdgeMap[vertex];
                    if (!destinationSet.empty()) {
                        for (std::vector<int>::iterator itr = destinationSet.begin();
                             itr != destinationSet.end(); ++itr) {
                            string edge = std::to_string(vertex) + " " + std::to_string((*itr));
                            masterFile << edge;
                            masterFile << "\n";
                        }
                    }
                }
            }

            masterFile.flush();
            masterFile.close();

        }

        //Compress part files
        utils.compressFile(outputFilePart);
        utils.compressFile(outputFilePartMaster);
    }
}