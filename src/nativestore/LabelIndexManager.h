#ifndef LABEL_INDEX_MANAGER_H
#define LABEL_INDEX_MANAGER_H

#include <string>
#include <unordered_map>
#include <vector>
#include <fstream>
#include <cstdint>

class LabelIndexManager {
public:
    // Configurable max nodes and labels
    static constexpr size_t MAX_NODES = 1'000'000;
    static constexpr size_t MAX_LABELS = 1024;
    static constexpr size_t BYTES_PER_LABEL = (MAX_NODES + 7) / 8;

    LabelIndexManager(const std::string& labelMapFile = "label-mapping.db",
                      const std::string& bitmapFile = "label-scan-store.db");
    ~LabelIndexManager();

    // Get label ID, create if missing
    uint32_t getOrCreateLabelID(const std::string& label);

    // Get label name by ID
    std::string getLabelName(uint32_t id) const;

    // Set label for a node
    void setLabel(uint32_t labelID, size_t nodeID);

    // Get all node IDs with this label
    std::vector<size_t> getNodesWithLabel(uint32_t labelID);

private:
    // Internal helpers
    void loadLabelMappings();
    void saveLabel(const std::string& label, uint32_t id);
    void loadOrInitBitmapFile();

    static inline void setBit(std::vector<uint8_t>& bitmap, size_t pos);
    static inline bool getBit(const std::vector<uint8_t>& bitmap, size_t pos);

private:
    std::unordered_map<std::string, uint32_t> labelToId;
    std::vector<std::string> idToLabel;
    uint32_t nextLabelId;

    std::string labelMapFilename;
    std::string bitmapFilename;

    std::fstream bitmapFileStream;
};

#endif // LABEL_INDEX_MANAGER_H
