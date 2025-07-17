#pragma once
#include <string>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <roaring/roaring.h>

constexpr size_t MAX_NODES = 1'000'000;  // adjust as needed

class LabelIndexManager {
public:
    LabelIndexManager(const std::string& labelMapFilePrefix,
                      const std::string& bitmapPrefix);
    ~LabelIndexManager();
    void saveAllBitMaps();

    uint32_t getOrCreateLabelID(const std::string& label);
    std::string getLabelName(uint32_t id) const;

    void setLabel(uint32_t labelID, size_t nodeID);
    std::vector<size_t> getNodesWithLabel(uint32_t labelID);

private:
    std::string labelMapFilePrefix;
    std::string bitmapPrefix;
    uint32_t nextLabelId;

    std::unordered_map<std::string, uint32_t> labelToId;
    std::vector<std::string> idToLabel;

    std::unordered_map<uint32_t, roaring_bitmap_t*> labelBitmaps;

    mutable std::mutex mtx;  // for thread safety

    void loadLabelMappings();
    void saveLabel(const std::string& label, uint32_t id);
    void loadOrInitBitmap(uint32_t labelID);
    void saveBitmap(uint32_t labelID);

    std::string getBitmapFilePath(uint32_t labelID) const;

    void createDirectoriesIfNeeded() const;
};
