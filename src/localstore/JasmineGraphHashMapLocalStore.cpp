/**
Copyright 2019 JasminGraph Team
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

#include "JasmineGraphHashMapLocalStore.h"
#include <iostream>
#include <fstream>


JasmineGraphHashMapLocalStore::JasmineGraphHashMapLocalStore(int graphid, int partitionid) {
    graphId = graphid;
    partitionId = partitionid;
}

JasmineGraphHashMapLocalStore::JasmineGraphHashMapLocalStore(std::string folderLocation) {
    instanceDataFolderLocation = folderLocation;
}

JasmineGraphHashMapLocalStore::JasmineGraphHashMapLocalStore() {
}

bool JasmineGraphHashMapLocalStore::loadGraph() {
    bool result = false;
    std::string edgeStorePath = instanceDataFolderLocation + getFileSeparator() + EDGE_STORE_NAME;

    std::ifstream dbFile;
    dbFile.open(edgeStorePath, std::ios::binary | std::ios::in);

    if (!dbFile.is_open()) {
        return result;
    }

    dbFile.seekg(0, std::ios::end);
    int length = dbFile.tellg();
    dbFile.seekg(0, std::ios::beg);
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

bool JasmineGraphHashMapLocalStore::storeGraph() {
    bool result = false;
    flatbuffers::FlatBufferBuilder builder;
    std::vector<flatbuffers::Offset<EdgeStoreEntry>> edgeStoreEntriesVector;
    std::string edgeStorePath = instanceDataFolderLocation + getFileSeparator() + EDGE_STORE_NAME;

    std::map<long, std::unordered_set<long>>::iterator localSubGraphMapIterator;
    for (localSubGraphMapIterator = localSubGraphMap.begin();
         localSubGraphMapIterator != localSubGraphMap.end(); localSubGraphMapIterator++) {
        long key = localSubGraphMapIterator->first;
        unordered_set<long> value = localSubGraphMapIterator->second;

        std::vector<long> valueVector(value.begin(), value.end());

        auto flatbufferVector = builder.CreateVector(valueVector);
        auto edgeStoreEntry = CreateEdgeStoreEntry(builder, key, flatbufferVector);
        edgeStoreEntriesVector.push_back(edgeStoreEntry);
    }

    auto flatBuffersEdgeStoreEntriesVector = builder.CreateVectorOfSortedTables(&edgeStoreEntriesVector);

    auto edgeStore = CreateEdgeStore(builder, flatBuffersEdgeStoreEntriesVector);

    builder.Finish(edgeStore);

    flatbuffers::SaveFile(edgeStorePath.c_str(), (const char *) builder.GetBufferPointer(), (size_t) builder.GetSize(),
                          true);

    result = true;


    return result;
}

std::string JasmineGraphHashMapLocalStore::getFileSeparator() {
#ifdef _WIN32
    return "\\";
#else
    return "/";
#endif
}

void JasmineGraphHashMapLocalStore::toLocalSubGraphMap(const EdgeStore *edgeStoreData) {
    auto allEntries = edgeStoreData->entries();
    int tableSize = allEntries->size();

    for (int i = 0; i < tableSize; i = i + 1) {
        auto entry = allEntries->Get(i);
        long key = entry->key();
        auto value = entry->value();
        const flatbuffers::Vector<long> &vector = *value;
        unordered_set<long> valueSet(vector.begin(), vector.end());
        localSubGraphMap.insert(std::make_pair(key, valueSet));
    }
}

long JasmineGraphHashMapLocalStore::getEdgeCount() {

    if (edgeCount == 0) {
        std::map<long, std::unordered_set<long>>::iterator localSubGraphMapIterator;
        long mapSize = localSubGraphMap.size();
        for (localSubGraphMapIterator = localSubGraphMap.begin();
             localSubGraphMapIterator != localSubGraphMap.end(); localSubGraphMapIterator++) {
            edgeCount = edgeCount + localSubGraphMapIterator->second.size();
        }
    }

    return edgeCount;
}

unordered_set<long> JasmineGraphHashMapLocalStore::getVertexSet() {
    unordered_set<long> vertexSet;

    for (map<long, unordered_set<long>>::iterator it = localSubGraphMap.begin(); it != localSubGraphMap.end(); ++it) {
        vertexSet.insert(it->first);
    }

    return vertexSet;
}

int *JasmineGraphHashMapLocalStore::getOutDegreeDistribution() {
    distributionArray = new int[vertexCount];
    int counter = 0;

    for (map<long, unordered_set<long>>::iterator it = localSubGraphMap.begin(); it != localSubGraphMap.end(); ++it) {
        distributionArray[counter] = (it->second).size();
        counter++;
    }
    return distributionArray;
}

map<long, long> JasmineGraphHashMapLocalStore::getOutDegreeDistributionHashMap() {
    map<long, long> distributionHashMap;

    for (map<long, unordered_set<long>>::iterator it = localSubGraphMap.begin(); it != localSubGraphMap.end(); ++it) {
        long distribution = (it->second).size();
        distributionHashMap.insert(std::make_pair(it->first, distribution));
    }
    return distributionHashMap;
}

long JasmineGraphHashMapLocalStore::getVertexCount() {
    if (vertexCount == 0) {
        vertexCount = localSubGraphMap.size();
    }

    return vertexCount;
}

void JasmineGraphHashMapLocalStore::addEdge(long startVid, long endVid) {
    map<long, unordered_set<long>>::iterator entryIterator = localSubGraphMap.find(startVid);
    if (entryIterator != localSubGraphMap.end()) {
        unordered_set<long> neighbours = entryIterator->second;
        neighbours.insert(endVid);
        entryIterator->second = neighbours;
    }
}

map<long, unordered_set<long>> JasmineGraphHashMapLocalStore::getUnderlyingHashMap() {
    map<long, unordered_set<long>> result;
    return result;
}

void JasmineGraphHashMapLocalStore::initialize() {

}

void JasmineGraphHashMapLocalStore::addVertex(string *attributes) {

}

bool JasmineGraphHashMapLocalStore::storeAttributes(std::map<long, std::vector<string>> attributeMap, const string storePath) {
    this->localAttributeMap = attributeMap;
    bool result = false;
    flatbuffers::FlatBufferBuilder builder;
    std::vector<flatbuffers::Offset<AttributeStoreEntry>> attributeStoreEntriesVector;

    std::map<long, std::vector<std::string>>::iterator mapIterator;
    for (mapIterator = localAttributeMap.begin(); mapIterator != localAttributeMap.end(); mapIterator++) {
        long key = mapIterator->first;
        std::vector<std::string> attributeVector = mapIterator->second;

        auto flatbufferVector = builder.CreateVectorOfStrings(attributeVector);
        auto attributeStoreEntry = CreateAttributeStoreEntry(builder, key, flatbufferVector);
        attributeStoreEntriesVector.push_back(attributeStoreEntry);
    }

    auto flatBuffersAttributeStoreEntriesVector = builder.CreateVectorOfSortedTables(&attributeStoreEntriesVector);

    auto attributeStore = CreateAttributeStore(builder, flatBuffersAttributeStoreEntriesVector);

    builder.Finish(attributeStore);

    flatbuffers::SaveFile(storePath.c_str(), (const char *) builder.GetBufferPointer(), (size_t) builder.GetSize(), true);

    result = true;

    return result;
}

bool JasmineGraphHashMapLocalStore::loadAttributes() {
    bool result = false;
    std::string attributeStorePath = instanceDataFolderLocation + getFileSeparator() + ATTRIBUTE_STORE_NAME;
    std::ifstream dbFile;
    dbFile.open(attributeStorePath, std::ios::binary | std::ios::in);

    if (!dbFile.is_open()) {
        return result;
    }

    dbFile.seekg(0, std::ios::end);
    int length = dbFile.tellg();
    dbFile.seekg(0, std::ios::beg);
    char *data = new char[length];
    dbFile.read(data, length);
    dbFile.close();

    auto attributeStoreData = GetAttributeStore(data);

    toLocalAttributeMap(attributeStoreData);

    result = true;

    return result;
}

void JasmineGraphHashMapLocalStore::toLocalAttributeMap(const AttributeStore *attributeStoreData) {
    auto allEntries = attributeStoreData->entries();
    int tableSize = allEntries->size();

    for (int i = 0; i < tableSize; i = i + 1) {
        std::vector<std::string> attributeVector;
        auto entry = allEntries->Get(i);
        long key = entry->key();
        auto attributes = entry->value();
        auto attributesSize = attributes->Length();
        for (int j = 0; j < attributesSize; j = j + 1) {
            attributeVector.push_back(attributes->Get(j)->c_str());
        }
        localAttributeMap.insert(std::make_pair(key, attributeVector));
    }
}

map<long, std::vector<std::string>> JasmineGraphHashMapLocalStore::getAttributeHashMap() {
    loadAttributes();
    return this->localAttributeMap;
}

// The following 4 functions are used to serialize partition edge maps before uploading through workers.
// If this function can be done by existing methods we can remove these.

void JasmineGraphHashMapLocalStore::toLocalEdgeMap(const PartEdgeMapStore *edgeMapStoreData) {
    auto allEntries = edgeMapStoreData->entries();
    int tableSize = allEntries->size();

    for (int i = 0; i < tableSize; i = i + 1) {
        auto entry = allEntries->Get(i);
        int key = entry->key();
        auto value = entry->value();
        const flatbuffers::Vector<int> &vector = *value;
        std::vector<int> valueSet(vector.begin(), vector.end());
        edgeMap.insert(std::make_pair(key, valueSet));
    }
}

bool JasmineGraphHashMapLocalStore::loadPartEdgeMap(const std::string filePath) {
    bool result = false;

    std::ifstream dbFile;
    dbFile.open(filePath, std::ios::binary | std::ios::in);

    if (!dbFile.is_open()) {
        return result;
    }

    dbFile.seekg(0, std::ios::end);
    int length = dbFile.tellg();
    dbFile.seekg(0, std::ios::beg);
    char *data = new char[length];
    dbFile.read(data, length);
    dbFile.close();

    auto edgeMapStoreData = GetPartEdgeMapStore(data);

    toLocalEdgeMap(edgeMapStoreData);

    result = true;

    return result;
}

bool JasmineGraphHashMapLocalStore::storePartEdgeMap(std::map<int, std::vector<int>> edgeMap, const std::string savePath) {
    bool result = false;
    flatbuffers::FlatBufferBuilder builder;
    std::vector<flatbuffers::Offset<PartEdgeMapStoreEntry>> edgeStoreEntriesVector;

    std::map<int, std::vector<int>>::iterator mapIterator;
    for (mapIterator = edgeMap.begin(); mapIterator != edgeMap.end(); mapIterator++) {
        int key = mapIterator->first;
        std::vector<int> value = mapIterator->second;
        std::vector<int> valueVector(value.begin(), value.end());
        auto flatbufferVector = builder.CreateVector(valueVector);
        auto edgeStoreEntry = CreatePartEdgeMapStoreEntry(builder, key, flatbufferVector);
        edgeStoreEntriesVector.push_back(edgeStoreEntry);
    }

    auto flatBuffersEdgeStoreEntriesVector = builder.CreateVectorOfSortedTables(&edgeStoreEntriesVector);

    auto edgeStore = CreatePartEdgeMapStore(builder, flatBuffersEdgeStoreEntriesVector);

    builder.Finish(edgeStore);

    flatbuffers::SaveFile(savePath.c_str(), (const char *) builder.GetBufferPointer(), (size_t) builder.GetSize(), true);

    result = true;

    return result;
}

map<int, std::vector<int>> JasmineGraphHashMapLocalStore::getEdgeHashMap(const std::string filePath) {
    loadPartEdgeMap(filePath);
    return this->edgeMap;
}
