//
// Created by chinthaka on 11/11/18.
//

#include "MetisPartitioner.h"

void MetisPartitioner::loadDataSet(string filePath) {
    std::ifstream dbFile;
    dbFile.open(filePath,std::ios::binary | std::ios::in);

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

        std::set<int> firstEdgeSet = graphStorageMap[firstVertex];
        std::set<int> secondEdgeSet = graphStorageMap[secondVertex];

        if (firstEdgeSet.empty()) {
            vertexCount++;
            edgeCount++;
            firstEdgeSet.insert(secondVertex);

        } else {
            if (firstEdgeSet.find(secondVertex) == firstEdgeSet.end()) {
                firstEdgeSet.insert(secondVertex);
                edgeCount++;
            }
        }

        if (secondEdgeSet.empty()){
            vertexCount++;
            secondEdgeSet.insert(firstVertex);

        } else {
            if (secondEdgeSet.find(firstVertex) == secondEdgeSet.end()) {
                secondEdgeSet.insert(firstVertex);
            }
        }

        graphStorageMap[firstVertex] = firstEdgeSet;
        graphStorageMap[secondVertex] = secondEdgeSet;


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
        std::set<int> vertexSet = graphStorageMap[vertexNum];

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
