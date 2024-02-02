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

#include "JasmineGraphHashMapCentralStore.h"

#include "../util/logger/Logger.h"
using namespace std;

const Logger hashmap_centralstore_logger;

JasmineGraphHashMapCentralStore::JasmineGraphHashMapCentralStore() {}

JasmineGraphHashMapCentralStore::JasmineGraphHashMapCentralStore(std::string folderLocation) {
    this->instanceDataFolderLocation = folderLocation;
}

JasmineGraphHashMapCentralStore::JasmineGraphHashMapCentralStore(int graphId, int partitionId) {
    this->graphId = graphId;
    this->partitionId = partitionId;

    instanceDataFolderLocation = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
}

bool JasmineGraphHashMapCentralStore::loadGraph() {
    bool result = false;
    std::string edgeStorePath = instanceDataFolderLocation + getFileSeparator() + std::to_string(graphId) +
                                "_centralstore_" + std::to_string(partitionId);

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

    vertexCount = centralSubgraphMap.size();
    edgeCount = getEdgeCount();

    return result;
}

bool JasmineGraphHashMapCentralStore::loadGraph(std::string fileName) {
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

    vertexCount = centralSubgraphMap.size();
    edgeCount = getEdgeCount();

    return result;
}

bool JasmineGraphHashMapCentralStore::storeGraph() {
    bool result = false;
    flatbuffers::FlatBufferBuilder builder;
    std::vector<flatbuffers::Offset<EdgeStoreEntry>> edgeStoreEntriesVector;
    std::string edgeStorePath = instanceDataFolderLocation + getFileSeparator() + getFileSeparator() +
                                std::to_string(graphId) + "_centralstore_" + std::to_string(partitionId);

    std::map<long, std::unordered_set<long>>::iterator localSubGraphMapIterator;
    for (localSubGraphMapIterator = centralSubgraphMap.begin(); localSubGraphMapIterator != centralSubgraphMap.end();
         localSubGraphMapIterator++) {
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

map<long, unordered_set<long>> JasmineGraphHashMapCentralStore::getUnderlyingHashMap() { return centralSubgraphMap; }

map<long, long> JasmineGraphHashMapCentralStore::getOutDegreeDistributionHashMap() {
    map<long, long> distributionHashMap;

    for (map<long, unordered_set<long>>::iterator it = centralSubgraphMap.begin(); it != centralSubgraphMap.end();
         ++it) {
        long distribution = (it->second).size();
        auto key = it->first;
        auto nodes = it->second;
        unordered_set<long> neighboursOfNeighbour = nodes;
        distributionHashMap[it->first] = distribution;
    }
    return distributionHashMap;
}

map<long, long> JasmineGraphHashMapCentralStore::getInDegreeDistributionHashMap() {
    map<long, long> distributionHashMap;

    for (map<long, unordered_set<long>>::iterator it = centralSubgraphMap.begin(); it != centralSubgraphMap.end();
         ++it) {
        unordered_set<long> distribution = it->second;

        for (auto itr = distribution.begin(); itr != distribution.end(); ++itr) {
            std::map<long, long>::iterator distMapItr = distributionHashMap.find(*itr);
            if (distMapItr != distributionHashMap.end()) {
                long previousValue = distMapItr->second;
                distMapItr->second = previousValue + 1;
            } else {
                distributionHashMap[*itr] = 1;
            }
        }
    }
    return distributionHashMap;
}

void JasmineGraphHashMapCentralStore::addVertex(string *attributes) {}

void JasmineGraphHashMapCentralStore::addEdge(long startVid, long endVid) {
    map<long, unordered_set<long>>::iterator entryIterator = centralSubgraphMap.find(startVid);
    if (entryIterator != centralSubgraphMap.end()) {
        unordered_set<long> neighbours = entryIterator->second;
        neighbours.insert(endVid);
        entryIterator->second = neighbours;
    }
}

long JasmineGraphHashMapCentralStore::getVertexCount() {
    if (vertexCount == 0) {
        vertexCount = centralSubgraphMap.size();
    }

    return vertexCount;
}

long JasmineGraphHashMapCentralStore::getEdgeCount() {
    if (edgeCount == 0) {
        std::map<long, std::unordered_set<long>>::iterator localSubGraphMapIterator;
        long mapSize = centralSubgraphMap.size();
        for (localSubGraphMapIterator = centralSubgraphMap.begin();
             localSubGraphMapIterator != centralSubgraphMap.end(); localSubGraphMapIterator++) {
            edgeCount = edgeCount + localSubGraphMapIterator->second.size();
        }
    }

    return edgeCount;
}

std::string JasmineGraphHashMapCentralStore::getFileSeparator() {
#ifdef _WIN32
    return "\\";
#else
    return "/";
#endif
}

void JasmineGraphHashMapCentralStore::toLocalSubGraphMap(const PartEdgeMapStore *edgeMapStoreData) {
    auto allEntries = edgeMapStoreData->entries();
    int tableSize = allEntries->size();

    for (int i = 0; i < tableSize; i = i + 1) {
        auto entry = allEntries->Get(i);
        long key = entry->key();
        auto value = entry->value();
        const flatbuffers::Vector<int> &vector = *value;
        unordered_set<long> valueSet(vector.begin(), vector.end());
        centralSubgraphMap[key] = valueSet;
    }
}

bool JasmineGraphHashMapCentralStore::storePartEdgeMap(std::map<int, std::vector<int>> edgeMap,
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
