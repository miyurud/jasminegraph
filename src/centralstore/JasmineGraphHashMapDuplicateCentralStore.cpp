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

#include "JasmineGraphHashMapDuplicateCentralStore.h"

JasmineGraphHashMapDuplicateCentralStore::JasmineGraphHashMapDuplicateCentralStore() {}

JasmineGraphHashMapDuplicateCentralStore::JasmineGraphHashMapDuplicateCentralStore(std::string folderLocation) {
    this->instanceDataFolderLocation = folderLocation;
}

JasmineGraphHashMapDuplicateCentralStore::JasmineGraphHashMapDuplicateCentralStore(int graphId, int partitionId) {
    this->graphId = graphId;
    this->partitionId = partitionId;

    instanceDataFolderLocation = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
}

bool JasmineGraphHashMapDuplicateCentralStore::loadGraph() {
    bool result = false;
    std::string edgeStorePath = instanceDataFolderLocation + getFileSeparator() + std::to_string(graphId) +
                                "_centralstore_dp_" + std::to_string(partitionId);

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

    auto edgeStoreData = GetPartEdgeMapStore(data);

    toLocalSubGraphMap(edgeStoreData);

    result = true;

    vertexCount = centralDuplicateStoreSubgraphMap.size();
    edgeCount = getEdgeCount();

    return result;
}

bool JasmineGraphHashMapDuplicateCentralStore::loadGraph(std::string fileName) {
    bool result = false;
    std::string edgeStorePath = fileName;

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

    auto edgeStoreData = GetPartEdgeMapStore(data);

    toLocalSubGraphMap(edgeStoreData);

    result = true;

    vertexCount = centralDuplicateStoreSubgraphMap.size();
    edgeCount = getEdgeCount();

    return result;
}

bool JasmineGraphHashMapDuplicateCentralStore::storeGraph() {
    bool result = false;
    flatbuffers::FlatBufferBuilder builder;
    std::vector<flatbuffers::Offset<EdgeStoreEntry>> edgeStoreEntriesVector;
    std::string edgeStorePath = instanceDataFolderLocation + getFileSeparator() + getFileSeparator() +
                                std::to_string(graphId) + "_centralstore_dp_" + std::to_string(partitionId);

    std::map<long, std::unordered_set<long>>::iterator localSubGraphMapIterator;
    for (localSubGraphMapIterator = centralDuplicateStoreSubgraphMap.begin();
         localSubGraphMapIterator != centralDuplicateStoreSubgraphMap.end(); localSubGraphMapIterator++) {
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

    flatbuffers::SaveFile(edgeStorePath.c_str(), (const char *)builder.GetBufferPointer(), (size_t)builder.GetSize(),
                          true);

    result = true;

    return result;
}

map<long, unordered_set<long>> JasmineGraphHashMapDuplicateCentralStore::getUnderlyingHashMap() {
    return centralDuplicateStoreSubgraphMap;
}

map<long, long> JasmineGraphHashMapDuplicateCentralStore::getOutDegreeDistributionHashMap() {
    map<long, long> distributionHashMap;

    for (map<long, unordered_set<long>>::iterator it = centralDuplicateStoreSubgraphMap.begin();
         it != centralDuplicateStoreSubgraphMap.end(); ++it) {
        long distribution = (it->second).size();
        distributionHashMap[it->first] = distribution;
    }
    return distributionHashMap;
}

void JasmineGraphHashMapDuplicateCentralStore::addVertex(string *attributes) {}

void JasmineGraphHashMapDuplicateCentralStore::addEdge(long startVid, long endVid) {
    map<long, unordered_set<long>>::iterator entryIterator = centralDuplicateStoreSubgraphMap.find(startVid);
    if (entryIterator != centralDuplicateStoreSubgraphMap.end()) {
        unordered_set<long> neighbours = entryIterator->second;
        neighbours.insert(endVid);
        entryIterator->second = neighbours;
    }
}

long JasmineGraphHashMapDuplicateCentralStore::getVertexCount() {
    if (vertexCount == 0) {
        vertexCount = centralDuplicateStoreSubgraphMap.size();
    }

    return vertexCount;
}

long JasmineGraphHashMapDuplicateCentralStore::getEdgeCount() {
    if (edgeCount == 0) {
        std::map<long, std::unordered_set<long>>::iterator localSubGraphMapIterator;
        long mapSize = centralDuplicateStoreSubgraphMap.size();
        for (localSubGraphMapIterator = centralDuplicateStoreSubgraphMap.begin();
             localSubGraphMapIterator != centralDuplicateStoreSubgraphMap.end(); localSubGraphMapIterator++) {
            edgeCount = edgeCount + localSubGraphMapIterator->second.size();
        }
    }

    return edgeCount;
}

std::string JasmineGraphHashMapDuplicateCentralStore::getFileSeparator() {
#ifdef _WIN32
    return "\\";
#else
    return "/";
#endif
}

void JasmineGraphHashMapDuplicateCentralStore::toLocalSubGraphMap(const PartEdgeMapStore *edgeMapStoreData) {
    auto allEntries = edgeMapStoreData->entries();
    int tableSize = allEntries->size();

    for (int i = 0; i < tableSize; i = i + 1) {
        auto entry = allEntries->Get(i);
        long key = entry->key();
        auto value = entry->value();
        const flatbuffers::Vector<int> &vector = *value;
        unordered_set<long> valueSet(vector.begin(), vector.end());
        centralDuplicateStoreSubgraphMap[key] = valueSet;
    }
}

bool JasmineGraphHashMapDuplicateCentralStore::storePartEdgeMap(std::map<int, std::vector<int>> edgeMap,
                                                                const std::string savePath) {
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

    flatbuffers::SaveFile(savePath.c_str(), (const char *)builder.GetBufferPointer(), (size_t)builder.GetSize(), true);

    result = true;

    return result;
}
