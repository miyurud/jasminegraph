//
// Created by chinthaka on 9/19/18.
//

#include "JasminGraphHashMapLocalStore.h"
#include <iostream>
#include <fstream>


JasminGraphHashMapLocalStore::JasminGraphHashMapLocalStore(int graphid, int partitionid) {
    graphId = graphid;
    partitionId = partitionid;
}

JasminGraphHashMapLocalStore::JasminGraphHashMapLocalStore(std::string folderLocation) {
    instanceDataFolderLocation = folderLocation;
}

bool JasminGraphHashMapLocalStore::loadGraph() {
    bool result = false;
    std::string edgeStorePath = instanceDataFolderLocation + getFileSeparator() + EDGE_STORE_NAME;

    std::ifstream dbFile;
    dbFile.open(edgeStorePath,std::ios::binary | std::ios::in);

    if (!dbFile.is_open()) {
        return result;
    }

    dbFile.seekg(0,std::ios::end);
    int length = dbFile.tellg();
    dbFile.seekg(0,std::ios::beg);
    char *data = new char[length];
    dbFile.read(data, length);
    dbFile.close();

    auto edgeStoreData = GetEdgeStore(data);

    toLocalSubGraphMap(edgeStoreData);

    result = true;

    vertexCount = localSubGraphMap.size();
    edgeCount = getEdgeCount();

    return result;
}

bool JasminGraphHashMapLocalStore::storeGraph() {
    bool result = false;
    flatbuffers::FlatBufferBuilder builder;
    std::vector<flatbuffers::Offset<EdgeStoreEntry>> edgeStoreEntriesVector;
    std::string edgeStorePath = instanceDataFolderLocation + getFileSeparator() + EDGE_STORE_NAME;

    std::map<long,std::unordered_set<long>>::iterator localSubGraphMapIterator;
    for (localSubGraphMapIterator = localSubGraphMap.begin() ; localSubGraphMapIterator != localSubGraphMap.end() ; localSubGraphMapIterator++) {
        long key = localSubGraphMapIterator->first;
        unordered_set<long> value = localSubGraphMapIterator->second;

        std::vector<long> valueVector(value.begin(),value.end());

        auto flatbufferVector = builder.CreateVector(valueVector);
        auto edgeStoreEntry = CreateEdgeStoreEntry(builder,key,flatbufferVector);
        edgeStoreEntriesVector.push_back(edgeStoreEntry);
    }

    auto flatBuffersEdgeStoreEntriesVector = builder.CreateVectorOfSortedTables(&edgeStoreEntriesVector);

    auto edgeStore = CreateEdgeStore(builder,flatBuffersEdgeStoreEntriesVector);

    builder.Finish(edgeStore);

    flatbuffers::SaveFile(edgeStorePath.c_str(),(const char *)builder.GetBufferPointer(),(size_t)builder.GetSize(),true);

    result = true;


    return result;
}

std::string JasminGraphHashMapLocalStore::getFileSeparator() {
    #ifdef _WIN32
        return "\\";
    #else
        return "/";
    #endif
}

void JasminGraphHashMapLocalStore::toLocalSubGraphMap(const EdgeStore *edgeStoreData) {
    auto allEntries = edgeStoreData->entries();
    int tableSize = allEntries->size();

    for (int i = 0; i < tableSize; i=i+1) {
        auto entry = allEntries->Get(i);
        long key = entry->key();
        auto value = entry->value();
        const flatbuffers::Vector<long>& vector = *value;
        unordered_set<long> valueSet(vector.begin(),vector.end());
        localSubGraphMap.insert(std::make_pair(key,valueSet));
    }
}

long JasminGraphHashMapLocalStore::getEdgeCount() {

    if (edgeCount == 0) {
        std::map<long,std::unordered_set<long>>::iterator localSubGraphMapIterator;
        long mapSize = localSubGraphMap.size();
        for (localSubGraphMapIterator = localSubGraphMap.begin() ; localSubGraphMapIterator != localSubGraphMap.end() ; localSubGraphMapIterator++) {
            edgeCount = edgeCount + localSubGraphMapIterator->second.size();
        }
    }

    return edgeCount;
}

unordered_set<long> JasminGraphHashMapLocalStore::getVertexSet() {
    unordered_set<long> vertexSet;

    for(map<long,unordered_set<long>>::iterator it = localSubGraphMap.begin(); it != localSubGraphMap.end(); ++it) {
        vertexSet.insert(it->first);
    }

    return vertexSet;
}

int* JasminGraphHashMapLocalStore::getOutDegreeDistribution() {
    int distributionArray[vertexCount];
    int counter = 0;

    for(map<long,unordered_set<long>>::iterator it = localSubGraphMap.begin(); it != localSubGraphMap.end(); ++it) {
        distributionArray[counter] = (it->second).size();
        counter++;
    }
    return distributionArray;
}

map<long, long> JasminGraphHashMapLocalStore::getOutDegreeDistributionHashMap() {
    map<long,long> distributionHashMap;

    for(map<long,unordered_set<long>>::iterator it = localSubGraphMap.begin(); it != localSubGraphMap.end(); ++it) {
        long distribution = (it->second).size();
        distributionHashMap.insert(std::make_pair(it->first,distribution));
    }
    return distributionHashMap;
}

long JasminGraphHashMapLocalStore::getVertexCount() {
    if (vertexCount == 0 ) {
        vertexCount = localSubGraphMap.size();
    }

    return vertexCount;
}

void JasminGraphHashMapLocalStore::addEdge(long startVid, long endVid) {
    map<long,unordered_set<long>>::iterator entryIterator = localSubGraphMap.find(startVid);
    if (entryIterator != localSubGraphMap.end()) {
        unordered_set<long> neighbours = entryIterator->second;
        neighbours.insert(endVid);
        entryIterator->second = neighbours;
    }
}