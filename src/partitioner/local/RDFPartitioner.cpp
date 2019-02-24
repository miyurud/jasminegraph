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

#include "RDFPartitioner.h"

using namespace std;
using namespace __gnu_cxx;


RDFPartitioner::RDFPartitioner(SQLiteDBInterface *sqlite) {
    this->sqlite = *sqlite;
}

void RDFPartitioner::convertWithoutDistribution(string graphName, int graphID, string inputFilePath,
                                                string outputFilePath, int nParts,
                                                bool isDistributedCentralPartitions, int nThreads, int nPlaces) {
    this->outputFilePath = outputFilePath;
    this->nParts = nParts;
    this->graphName = graphName;
    this->isDistributedCentralPartitions = isDistributedCentralPartitions;
    this->graphID = graphID;
    this->nThreads = nThreads;
    this->nPlaces = nPlaces;
}

void RDFPartitioner::distributeEdges() {
    //method implementation
}

void RDFPartitioner::loadDataSet(string inputFilePath, string outputFilePath, int graphID) {
    this->graphID = graphID;
    this->outputFilePath = outputFilePath;
    std::cout << "grapphId" << this->graphID << std::endl;
    this->utils.createDirectory("/tmp/" + std::to_string(this->graphID));
    std::ifstream dbFile;
    dbFile.open(inputFilePath, std::ios::binary | std::ios::in);


    string subject;
    string predicate;
    string object;
    char splitter = '\t';
    string line;

    while (std::getline(dbFile, line)) {

        string subject_str;
        string predicate_str;
        string object_str;

        std::istringstream stream(line);
        std::getline(stream, subject_str, splitter);
        stream >> predicate_str >> object_str;



        long firstVertex = addToStore(&nodes, subject_str);


        long relation = addToStore(&predicates, predicate_str);

        long secondVertex = addToStore(&nodes, object_str);


        std::string secondVertexStr = std::to_string(secondVertex);

        addToMap(&relationsMap, firstVertex, relation, secondVertexStr);


        std::set<long> vertexSet = graphStorage.find(firstVertex)->second;

        for (auto it = vertexSet.begin(); it != vertexSet.end(); ++it)

            if (vertexSet.empty()) {

                vertexSet = set<long>();
                vertexSet.insert(secondVertex);
                edgeCount++;
                graphStorage.insert({firstVertex, vertexSet});
            } else if (!vertexSet.empty()) {

                if (vertexSet.insert(secondVertex).second) {
                    edgeCount++;
                }
            }

    }
    writeRelationData();
}

long RDFPartitioner::addToStore(std::map<string, long> *map, string URI) {
    long id;
    id = map->size();

    auto search = map->find(URI);
    if (search != map->end()) {
        return search->second;
    } else {

    }


    if (*map == nodes) {
        nodesTemp.insert({id, URI});


    } else if (*map == predicates) {
        predicatesTemp.insert({id, URI});

    }
    map->insert({URI, id});
    return id;
}


void RDFPartitioner::addToMap(std::map<long, std::map<long, std::vector<string> >> *map, long vertex, long relation,
                              string value) {
    std::map<long, std::vector<string>> miniMap = map->find(vertex)->second;


    if (!miniMap.empty()) {

        std::vector<string> list = miniMap.find(0)->second;

        if (!list.empty()) {

            list.push_back(value);
        } else if (list.empty()) {

            list = std::vector<string>();
            list.push_back(value);
            miniMap.insert({relation, list});
        }

    } else if (miniMap.empty()) {

        std::vector<string> list;
        list.push_back(value);
        miniMap = std::map<long, vector<string>>();
        miniMap.insert({relation, list});
        map->insert({vertex, miniMap});

    }
}


void RDFPartitioner::convert(string graphName, int graphID, string inputFilePath, string outputFilePath, int nParts,
                             bool isDistributedCentralPartitions, int nThreads, int nPlaces) {
    convertWithoutDistribution(graphName, graphID, inputFilePath, outputFilePath, nParts,
                               isDistributedCentralPartitions, nThreads, nPlaces);

    distributeEdges();

}

void RDFPartitioner::writeRelationData() {
    ofstream file;
    file.open("RDFdata.txt");
    for (auto it = relationsMap.begin(); it != relationsMap.end(); ++it) {
        std::cout << it->first << " " << flush;
        file << it->first << " " << flush;
        std::map<long, std::vector<string>> miniMap = relationsMap.find(it->first)->second;
        for (auto itr = miniMap.begin(); itr != miniMap.end(); ++itr) {
            std::cout << itr->first << " " << flush;
            file << itr->first << " " << flush;

            std::vector<string> valuelist = miniMap.find(itr->first)->second;
            for (std::vector<string>::const_iterator i = valuelist.begin(); i != valuelist.end(); ++i) {
                std::cout << *i << std::endl;
                file << *i << "\n";
            }
        }
    }

    file.close();
    std::cout << "writing to the file 'RDFdata'" << std::endl;

}