#include "LabelIndexManager.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <vector>

namespace fs = std::filesystem;

LabelIndexManager::LabelIndexManager(const std::string& labelMapFilePrefix,
                                     const std::string& bitmapPrefix)
    : labelMapFilePrefix(labelMapFilePrefix), bitmapPrefix(bitmapPrefix), nextLabelId(0)
{
    createDirectoriesIfNeeded();
    loadLabelMappings();
}

LabelIndexManager::~LabelIndexManager() {

    saveAllBitMaps();


}
void LabelIndexManager::saveAllBitMaps()
{
    std::lock_guard<std::mutex> lock(mtx);
    // Save all bitmaps
    for (auto& pair : labelBitmaps) {
        saveBitmap(pair.first);
        roaring_bitmap_free(pair.second);
    }
}

uint32_t LabelIndexManager::getOrCreateLabelID(const std::string& label) {
    if (label.size() > 255) {
        throw std::runtime_error("Label length exceeds 255 characters");
    }
    std::lock_guard<std::mutex> lock(mtx);

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
    std::lock_guard<std::mutex> lock(mtx);
    if (id < idToLabel.size()) {
        return idToLabel[id];
    }
    return "";
}

void LabelIndexManager::setLabel(uint32_t labelID, size_t nodeID) {
    // if (nodeID >= MAX_NODES) {
    //     std::cerr << "Invalid nodeID in setLabel\n";
    //     return;
    // }
    std::lock_guard<std::mutex> lock(mtx);
    // std::cout << "[INFO] Setting labelID " << labelID << " for nodeID " << nodeID << std::endl;
    loadOrInitBitmap(labelID);
    roaring_bitmap_add(labelBitmaps[labelID], static_cast<uint32_t>(nodeID));
    // std::cout << "[INFO] Label set successfully for nodeID " << nodeID << std::endl;
}

std::vector<size_t> LabelIndexManager::getNodesWithLabel(uint32_t labelID) {
    std::lock_guard<std::mutex> lock(mtx);
    std::vector<size_t> result;
    loadOrInitBitmap(labelID);

    roaring_uint32_iterator_t it;
    roaring_init_iterator(labelBitmaps[labelID], &it);
    while (it.has_value) {
        result.push_back(it.current_value);
        roaring_advance_uint32_iterator(&it);
    }
    return result;
}

void LabelIndexManager::loadLabelMappings() {
    std::ifstream in(labelMapFilePrefix + "_label_map.db", std::ios::binary);
    if (!in.good()) return;

    while (in.peek() != EOF) {
        uint32_t id;
        uint8_t len;
        char buf[256] = {0};
        in.read(reinterpret_cast<char*>(&id), sizeof(id));
        in.read(reinterpret_cast<char*>(&len), sizeof(len));
        if (len == 0 || len > 255) {
            std::cerr << "Corrupted label map file: invalid label length\n";
            break;
        }
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
    // Write atomically to temp then rename
    std::string filePath = labelMapFilePrefix + "_label_map.db";
    std::string tmpPath = filePath + ".tmp";

    // Read existing contents (except appending)
    std::ifstream existing(filePath, std::ios::binary);
    std::vector<char> existingData;
    if (existing.good()) {
        existing.seekg(0, std::ios::end);
        size_t size = existing.tellg();
        existing.seekg(0, std::ios::beg);
        existingData.resize(size);
        existing.read(existingData.data(), size);
        existing.close();
    }

    std::ofstream out(tmpPath, std::ios::binary);
    if (!out.good()) {
        std::cerr << "Failed to open label map file for writing\n";
        return;
    }
    // Write previous data
    if (!existingData.empty()) {
        out.write(existingData.data(), existingData.size());
    }
    // Write new label entry
    uint8_t len = static_cast<uint8_t>(label.length());
    out.write(reinterpret_cast<const char*>(&id), sizeof(id));
    out.write(reinterpret_cast<const char*>(&len), sizeof(len));
    out.write(label.data(), len);
    out.close();

    fs::rename(tmpPath, filePath);
}

void LabelIndexManager::loadOrInitBitmap(uint32_t labelID) {
    if (labelBitmaps.count(labelID)) return;

    std::string filePath = getBitmapFilePath(labelID);
    std::ifstream in(filePath, std::ios::binary);
    if (in.good()) {
        in.seekg(0, std::ios::end);
        size_t size = in.tellg();
        in.seekg(0, std::ios::beg);
        if (size == 0) {
            labelBitmaps[labelID] = roaring_bitmap_create();
            return;
        }
        std::vector<char> buffer(size);
        in.read(buffer.data(), size);
        in.close();

        labelBitmaps[labelID] = roaring_bitmap_portable_deserialize(buffer.data());
        if (!labelBitmaps[labelID]) {
            std::cerr << "Failed to deserialize bitmap for labelID " << labelID << ", creating new bitmap\n";
            labelBitmaps[labelID] = roaring_bitmap_create();
        }
    } else {
        labelBitmaps[labelID] = roaring_bitmap_create();
    }
}

void LabelIndexManager::saveBitmap(uint32_t labelID) {
    auto it = labelBitmaps.find(labelID);
    if (it == labelBitmaps.end()) return;

    roaring_bitmap_t* bmp = it->second;
    roaring_bitmap_run_optimize(bmp);

    std::string filePath = getBitmapFilePath(labelID);
    std::string tmpPath = filePath + ".tmp";

    size_t size = roaring_bitmap_portable_size_in_bytes(bmp);
    std::vector<char> buffer(size);
    roaring_bitmap_portable_serialize(bmp, buffer.data());

    std::ofstream out(tmpPath, std::ios::binary);
    if (!out.good()) {
        std::cerr << "Failed to open bitmap file for writing: " << tmpPath << "\n";
        return;
    }
    out.write(buffer.data(), size);
    out.close();

    fs::rename(tmpPath, filePath);
}

std::string LabelIndexManager::getBitmapFilePath(uint32_t labelID) const {
    return bitmapPrefix + "_label_" + std::to_string(labelID) + ".bitmap";
}

void LabelIndexManager::createDirectoriesIfNeeded() const {
    try {
        auto labelMapDir = fs::path(labelMapFilePrefix).parent_path();
        if (!labelMapDir.empty() && !fs::exists(labelMapDir)) {
            fs::create_directories(labelMapDir);
        }

        auto bitmapDir = fs::path(bitmapPrefix).parent_path();
        if (!bitmapDir.empty() && !fs::exists(bitmapDir)) {
            fs::create_directories(bitmapDir);
        }
    } catch (const fs::filesystem_error& e) {
        std::cerr << "Failed to create directories: " << e.what() << "\n";
    }
}
