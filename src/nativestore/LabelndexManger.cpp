#include "LabelIndexManager.h"
#include <algorithm>
#include <iostream>

LabelIndexManager::LabelIndexManager(const std::string& labelMapFile,
                                     const std::string& bitmapFile)
    : labelMapFilename(labelMapFile),
      bitmapFilename(bitmapFile),
      nextLabelId(0)
{
    loadLabelMappings();
    loadOrInitBitmapFile();
}

LabelIndexManager::~LabelIndexManager() {
    if (bitmapFileStream.is_open()) {
        bitmapFileStream.close();
    }
}

uint32_t LabelIndexManager::getOrCreateLabelID(const std::string& label) {
    auto it = labelToId.find(label);
    if (it != labelToId.end()) {
        return it->second;
    }

    uint32_t id = nextLabelId++;
    labelToId[label] = id;
    if (idToLabel.size() <= id) idToLabel.resize(id + 1);
    idToLabel[id] = label;

    saveLabel(label, id);
    return id;
}

std::string LabelIndexManager::getLabelName(uint32_t id) const {
    if (id < idToLabel.size()) {
        return idToLabel[id];
    }
    return "";
}

void LabelIndexManager::setLabel(uint32_t labelID, size_t nodeID) {
    if (labelID >= MAX_LABELS || nodeID >= MAX_NODES) {
        std::cerr << "Invalid labelID or nodeID in setLabel\n";
        return;
    }
    size_t offset = labelID * BYTES_PER_LABEL + nodeID / 8;
    bitmapFileStream.seekg(offset);
    uint8_t byte = 0;
    bitmapFileStream.read(reinterpret_cast<char*>(&byte), 1);
    byte |= (1 << (nodeID % 8));
    bitmapFileStream.seekp(offset);
    bitmapFileStream.write(reinterpret_cast<char*>(&byte), 1);
    bitmapFileStream.flush();
}

std::vector<size_t> LabelIndexManager::getNodesWithLabel(uint32_t labelID) {
    std::vector<size_t> result;
    if (labelID >= MAX_LABELS) return result;

    std::vector<uint8_t> bitmap(BYTES_PER_LABEL);
    size_t offset = labelID * BYTES_PER_LABEL;
    bitmapFileStream.seekg(offset);
    bitmapFileStream.read(reinterpret_cast<char*>(bitmap.data()), BYTES_PER_LABEL);

    // Optimized 64-bit chunk scanning with bit tricks
    const size_t numChunks = BYTES_PER_LABEL / sizeof(uint64_t);
    const uint64_t* chunks = reinterpret_cast<const uint64_t*>(bitmap.data());

    for (size_t chunkIdx = 0; chunkIdx < numChunks; ++chunkIdx) {
        uint64_t bits = chunks[chunkIdx];
        size_t baseBitIndex = chunkIdx * 64;

        while (bits != 0) {
            unsigned long bitPos;
#if defined(_MSC_VER)
            _BitScanForward64(&bitPos, bits);
#else
            bitPos = __builtin_ctzll(bits);
#endif
            result.push_back(baseBitIndex + bitPos);
            bits &= bits - 1;  // clear lowest set bit
        }
    }

    // Handle leftover bytes
    size_t remainderBytes = BYTES_PER_LABEL % sizeof(uint64_t);
    size_t remainderStart = numChunks * sizeof(uint64_t);
    for (size_t i = 0; i < remainderBytes; ++i) {
        uint8_t byte = bitmap[remainderStart + i];
        for (int bit = 0; bit < 8; ++bit) {
            if (byte & (1 << bit)) {
                size_t nodeId = remainderStart * 8 + i * 8 + bit;
                if (nodeId < MAX_NODES) result.push_back(nodeId);
            }
        }
    }

    return result;
}

void LabelIndexManager::loadLabelMappings() {
    std::ifstream in(labelMapFilename, std::ios::binary);
    if (!in.good()) {
        // No file, start fresh
        return;
    }

    while (in.peek() != EOF) {
        uint32_t id;
        uint8_t len;
        char buf[256];

        in.read(reinterpret_cast<char*>(&id), sizeof(id));
        in.read(reinterpret_cast<char*>(&len), sizeof(len));
        in.read(buf, len);
        std::string label(buf, len);

        labelToId[label] = id;
        if (idToLabel.size() <= id) idToLabel.resize(id + 1);
        idToLabel[id] = label;
        nextLabelId = std::max(nextLabelId, id + 1);
    }
    in.close();
}

void LabelIndexManager::saveLabel(const std::string& label, uint32_t id) {
    std::ofstream out(labelMapFilename, std::ios::binary | std::ios::app);
    uint8_t len = label.length();
    out.write(reinterpret_cast<char*>(&id), sizeof(id));
    out.write(reinterpret_cast<char*>(&len), sizeof(len));
    out.write(label.data(), len);
    out.close();
}

void LabelIndexManager::loadOrInitBitmapFile() {
    std::ifstream test(bitmapFilename);
    if (!test.good()) {
        std::ofstream out(bitmapFilename, std::ios::binary);
        std::vector<uint8_t> zeros(MAX_LABELS * BYTES_PER_LABEL, 0);
        out.write(reinterpret_cast<char*>(zeros.data()), zeros.size());
        out.close();
    }

    bitmapFileStream.open(bitmapFilename, std::ios::in | std::ios::out | std::ios::binary);
    if (!bitmapFileStream.is_open()) {
        std::cerr << "Failed to open bitmap file: " << bitmapFilename << "\n";
    }
}

inline void LabelIndexManager::setBit(std::vector<uint8_t>& bitmap, size_t pos) {
    bitmap[pos / 8] |= (1 << (pos % 8));
}

inline bool LabelIndexManager::getBit(const std::vector<uint8_t>& bitmap, size_t pos) {
    return bitmap[pos / 8] & (1 << (pos % 8));
}

#include "LabelIndexManager.h"
#include <iostream>

// int main() {
//     LabelIndexManager* labelIndex =  new LabelIndexManager("/var/tmp/jasminegraph-localstore/label-mapping.db", "/var/tmp/jasminegraph-localstore/label-scan-store.db");
//
//     uint32_t personLabelId = labelIndex->getOrCreateLabelID("Person");
//     // uint32_t movieLabelId = labelIndex->getOrCreateLabelID("Movie");
//
//     // labelIndex->setLabel(personLabelId, 123);
//     // labelIndex->setLabel(personLabelId, 456);
//     // labelIndex->setLabel(movieLabelId, 789);
//     // labelIndex->setLabel(movieLabelId, 4400);
//
//     auto nodesWithPersonLabel = labelIndex->getNodesWithLabel(personLabelId);
//     std::cout << "Nodes with Person label: ";
//     for (auto node : nodesWithPersonLabel) {
//         std::cout << node << " ";
//     }
//     std::cout << "\n";
//
//     auto nodesWithMovieLabel = labelIndex->getNodesWithLabel(movieLabelId);
//     std::cout << "Nodes with Movie label: ";
//     for (auto node : nodesWithMovieLabel) {
//         std::cout << node << " ";
//     }
//     std::cout << "\n";
//
//     delete labelIndex;
//     return 0;
// }
