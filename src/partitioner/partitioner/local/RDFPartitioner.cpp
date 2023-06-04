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

    std::ifstream dbFile;
    dbFile.open(inputFilePath, std::ios::binary | std::ios::in);
    std::cout << "File is loading..." << std::endl;


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


        long firstVertex = addToNodes(&nodes, subject_str);

        long relation = addToPredicates(&predicates, predicate_str);


        long secondVertex = addToNodes(&nodes, object_str);


        std::string secondVertexStr = std::to_string(secondVertex);

        addToMap(&relationsMap, firstVertex, relation, secondVertexStr);

        graphStorage[firstVertex].insert(secondVertex);
        edgeCount++;


    }
    writeRelationData();
}

long RDFPartitioner::addToNodes(std::map<string, long> *map, string URI) {
    long id;
    id = map->size();

    auto search = map->find(URI);
    if (search != map->end()) {
        return search->second;
    }


    nodesTemp.insert({id, URI});


    map->insert({URI, id});
    return id;
}

long RDFPartitioner::addToPredicates(std::map<string, long> *map, string URI) {
    long id;
    id = map->size();

    auto search = map->find(URI);
    if (search != map->end()) {
        return search->second;
    }


    predicatesTemp.insert({id, URI});


    map->insert({URI, id});
    return id;
}


void RDFPartitioner::addToMap(std::map<long, std::map<long, std::set<string>>> *map, long vertex, long relation,
                              string value) {
    auto it = map->find(vertex);
    if (it == map->end()) {
        relationsMap[vertex][relation].insert(value);

    } else {
        std::map<long, std::set<string>> miniMap = map->find(vertex)->second;


        auto it = miniMap.find(relation);
        if (it == miniMap.end()) {

            relationsMap[vertex][relation].insert(value);

        } else {

            relationsMap[vertex][relation].insert(value);


        }
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
    file.open("./tmp/RDF/" + std::to_string(this->graphID) + ".txt");

    for (const auto &subject : relationsMap) {
        for (const auto &relation : subject.second) {
            for (const auto &object : relation.second) {
                std::cout << subject.first << " " << flush;
                file << subject.first << " " << flush;

                std::cout << relation.first << " " << flush;
                file << relation.first << " " << flush;

                std::cout << object << "\n";
                file << object << "\n";

            }
        }
    }


    file.close();
    std::cout << "Data was written to the file path- /tmp/RDF/" << std::to_string(this->graphID) << ".txt" << std::endl;


    ofstream metisInputfile;
    metisInputfile.open("./tmp/RDF/" + std::to_string(this->graphID) + "_metisInput.txt");


    for( auto ii=graphStorage.begin(); ii!=graphStorage.end(); ++ii)
    {
        metisInputfile<< "Key: "<< ii->first << " value: ";

        for (auto it=ii->second.begin(); it!=ii->second.end(); ++it)
        {
            metisInputfile << *it << " ";
        }
        metisInputfile << endl;
    }
    metisInputfile.close();
    std::cout << "Data was written to the file path- /tmp/RDF/" << std::to_string(this->graphID) << "_metisInput.txt" << std::endl;


}