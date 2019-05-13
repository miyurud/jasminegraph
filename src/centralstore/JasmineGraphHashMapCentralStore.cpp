//
// Created by chinthaka on 5/11/19.
//

#include "JasmineGraphHashMapCentralStore.h"

JasmineGraphHashMapCentralStore::JasmineGraphHashMapCentralStore() {

}

JasmineGraphHashMapCentralStore::JasmineGraphHashMapCentralStore(std::string folderLocation) {
    this->instanceDataFolderLocation = folderLocation;
}

JasmineGraphHashMapCentralStore::JasmineGraphHashMapCentralStore(int graphId, int partitionId) {
    this->graphId = graphId;
    this->partitionId = partitionId;

    Utils utils;

    string dataFolder = utils.getJasmineGraphProperty("org.jasminegraph.server.runtime.location");
    string centralStoreFolder = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");

    string graphIdentifier = std::to_string(graphId) + "_" + std::to_string(partitionId);

    instanceDataFolderLocation = dataFolder + "/" + std::to_string(graphId) + "_centralStore" + graphIdentifier;
}

bool JasmineGraphHashMapCentralStore::loadGraph() {
    bool result = false;
    std::string edgeStorePath = instanceDataFolderLocation + getFileSeparator() + CENTRAL_STORE_NAME;

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

    vertexCount = localSubgraphMap.size();
    edgeCount = getEdgeCount();

    return result;
}

bool JasmineGraphHashMapCentralStore::storeGraph() {
    bool result = false;
    flatbuffers::FlatBufferBuilder builder;
    std::vector<flatbuffers::Offset<EdgeStoreEntry>> edgeStoreEntriesVector;
    std::string edgeStorePath = instanceDataFolderLocation + getFileSeparator() + CENTRAL_STORE_NAME;

    std::map<long, std::unordered_set<long>>::iterator localSubGraphMapIterator;
    for (localSubGraphMapIterator = localSubgraphMap.begin();
         localSubGraphMapIterator != localSubgraphMap.end(); localSubGraphMapIterator++) {
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

map<long, unordered_set<long>> JasmineGraphHashMapCentralStore::getUnderlyingHashMap() {
    return localSubgraphMap;
}

map<long, long> JasmineGraphHashMapCentralStore::getOutDegreeDistributionHashMap() {
    map<long, long> distributionHashMap;

    for (map<long, unordered_set<long>>::iterator it = localSubgraphMap.begin(); it != localSubgraphMap.end(); ++it) {
        long distribution = (it->second).size();
        distributionHashMap.insert(std::make_pair(it->first, distribution));
    }
    return distributionHashMap;
}

void JasmineGraphHashMapCentralStore::addVertex(string *attributes) {

}

void JasmineGraphHashMapCentralStore::addEdge(long startVid, long endVid) {
    map<long, unordered_set<long>>::iterator entryIterator = localSubgraphMap.find(startVid);
    if (entryIterator != localSubgraphMap.end()) {
        unordered_set<long> neighbours = entryIterator->second;
        neighbours.insert(endVid);
        entryIterator->second = neighbours;
    }
}

long JasmineGraphHashMapCentralStore::getVertexCount() {
    if (vertexCount == 0) {
        vertexCount = localSubgraphMap.size();
    }

    return vertexCount;
}

long JasmineGraphHashMapCentralStore::getEdgeCount() {
    if (edgeCount == 0) {
        std::map<long, std::unordered_set<long>>::iterator localSubGraphMapIterator;
        long mapSize = localSubgraphMap.size();
        for (localSubGraphMapIterator = localSubgraphMap.begin();
             localSubGraphMapIterator != localSubgraphMap.end(); localSubGraphMapIterator++) {
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

void JasmineGraphHashMapCentralStore::toLocalSubGraphMap(const EdgeStore *edgeStoreData) {
    auto allEntries = edgeStoreData->entries();
    int tableSize = allEntries->size();

    for (int i = 0; i < tableSize; i = i + 1) {
        auto entry = allEntries->Get(i);
        long key = entry->key();
        auto value = entry->value();
        const flatbuffers::Vector<long> &vector = *value;
        unordered_set<long> valueSet(vector.begin(), vector.end());
        localSubgraphMap.insert(std::make_pair(key, valueSet));
    }
}