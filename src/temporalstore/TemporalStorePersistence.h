/**
Copyright 2026 JasminGraph Team
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

#ifndef TEMPORAL_STORE_PERSISTENCE_H
#define TEMPORAL_STORE_PERSISTENCE_H

#include <string>
#include <fstream>
#include <vector>
#include <map>
#include <cstring>
#include "EdgeLifespanBitmap.h"
#include "PropertyIntervalDictionary.h"
#include "BitmapCompression.h"

/**
 * TemporalStorePersistence - Save/Load temporal data to/from disk
 * 
 * File Format:
 * ============
 * [Header: 64 bytes]
 *   - Magic number: "JGTSTORE" (8 bytes)
 *   - Version: uint32_t (4 bytes)
 *   - Graph ID: uint32_t (4 bytes)
 *   - Partition ID: uint32_t (4 bytes)
 *   - Snapshot ID: uint32_t (4 bytes)
 *   - Edge count: uint64_t (8 bytes)
 *   - Timestamp: uint64_t (8 bytes)
 *   - Flags: uint32_t (4 bytes)
 *     * Bit 0: Compressed (1=yes, 0=no)
 *     * Bits 1-31: Reserved
 *   - Reserved: 20 bytes
 * 
 * [Edge Section]
 *   - Edge count: uint64_t (8 bytes)
 *   - For each edge:
 *     * Source ID length: uint32_t (4 bytes)
 *     * Source ID: variable
 *     * Dest ID length: uint32_t (4 bytes)
 *     * Dest ID: variable
 *     * Bitmap total bits: uint32_t (4 bytes)
 *     * Bitmap data length: uint32_t (4 bytes)
 *     * Bitmap data: variable (compressed or raw)
 * 
 * [Property Section]
 *   - Property count: uint64_t (8 bytes)
 *   - For each property:
 *     * Node/Edge ID length: uint32_t
 *     * Node/Edge ID: variable
 *     * Property data length: uint32_t
 *     * Property data: variable (JSON format)
 * 
 * Example:
 *   TemporalStorePersistence::saveSnapshot("graph1_part0_snap42.tgs", 
 *                                          graphId, partitionId, 42,
 *                                          edgeBitmaps, nodeProps, edgeProps);
 */
class TemporalStorePersistence {
private:
    static const char* MAGIC_NUMBER;
    static const uint32_t VERSION = 1;
    static const uint32_t FLAG_COMPRESSED = 0x00000001;
    
    struct FileHeader {
        char magic[8];
        uint32_t version;
        uint32_t graphId;
        uint32_t partitionId;
        uint32_t snapshotId;
        uint64_t edgeCount;
        uint64_t timestamp;
        uint32_t flags;
        char reserved[20];
    };
    
    /**
     * Write string to file with length prefix
     */
    static void writeString(std::ofstream& file, const std::string& str) {
        uint32_t length = str.length();
        file.write(reinterpret_cast<const char*>(&length), sizeof(uint32_t));
        file.write(str.data(), length);
    }
    
    /**
     * Read string from file with length prefix
     */
    static std::string readString(std::ifstream& file) {
        uint32_t length;
        file.read(reinterpret_cast<char*>(&length), sizeof(uint32_t));
        
        std::string str(length, '\0');
        file.read(&str[0], length);
        return str;
    }

public:
    /**
     * Save snapshot to file
     */
    template<typename EdgeKey>
    static bool saveSnapshot(
        const std::string& filePath,
        uint32_t graphId,
        uint32_t partitionId,
        uint32_t snapshotId,
        const std::map<EdgeKey, EdgeLifespanBitmap>& edgeBitmaps,
        const std::map<std::string, PropertyIntervalDictionary>& nodeProperties,
        const std::map<EdgeKey, PropertyIntervalDictionary>& edgeProperties,
        bool compress = true) {
        
        std::ofstream file(filePath, std::ios::binary);
        if (!file.is_open()) {
            return false;
        }
        
        // Write header
        FileHeader header;
        std::memcpy(header.magic, MAGIC_NUMBER, 8);
        header.version = VERSION;
        header.graphId = graphId;
        header.partitionId = partitionId;
        header.snapshotId = snapshotId;
        header.edgeCount = edgeBitmaps.size();
        header.timestamp = std::chrono::system_clock::now().time_since_epoch().count();
        header.flags = compress ? FLAG_COMPRESSED : 0;
        std::memset(header.reserved, 0, 20);
        
        file.write(reinterpret_cast<const char*>(&header), sizeof(FileHeader));
        
        // Write edge section
        uint64_t edgeCount = edgeBitmaps.size();
        file.write(reinterpret_cast<const char*>(&edgeCount), sizeof(uint64_t));
        
        for (const auto& pair : edgeBitmaps) {
            const EdgeKey& key = pair.first;
            const EdgeLifespanBitmap& bitmap = pair.second;
            
            // Write edge IDs
            writeString(file, key.sourceId);
            writeString(file, key.destId);
            
            // Serialize Roaring bitmap (already compressed!)
            std::string bitmapData = bitmap.serialize();
            
            // Roaring format is already highly compressed, no need for additional compression
            // Just write the size and data
            uint32_t dataSize = bitmapData.size();
            file.write(reinterpret_cast<const char*>(&dataSize), sizeof(uint32_t));
            file.write(bitmapData.data(), dataSize);
        }
        
        // Write node properties section
        uint64_t nodePropCount = nodeProperties.size();
        file.write(reinterpret_cast<const char*>(&nodePropCount), sizeof(uint64_t));
        
        for (const auto& pair : nodeProperties) {
            const std::string& nodeId = pair.first;
            const PropertyIntervalDictionary& propDict = pair.second;
            
            writeString(file, nodeId);
            std::string propData = propDict.serialize();
            writeString(file, propData);
        }
        
        // Write edge properties section
        uint64_t edgePropCount = edgeProperties.size();
        file.write(reinterpret_cast<const char*>(&edgePropCount), sizeof(uint64_t));
        
        for (const auto& pair : edgeProperties) {
            const EdgeKey& key = pair.first;
            const PropertyIntervalDictionary& propDict = pair.second;
            
            std::string edgeId = key.toString();
            writeString(file, edgeId);
            std::string propData = propDict.serialize();
            writeString(file, propData);
        }
        
        file.close();
        return true;
    }
    
    /**
     * Load snapshot from file
     */
    template<typename EdgeKey>
    static bool loadSnapshot(
        const std::string& filePath,
        uint32_t& graphId,
        uint32_t& partitionId,
        uint32_t& snapshotId,
        std::map<EdgeKey, EdgeLifespanBitmap>& edgeBitmaps,
        std::map<std::string, PropertyIntervalDictionary>& nodeProperties,
        std::map<EdgeKey, PropertyIntervalDictionary>& edgeProperties) {
        
        std::ifstream file(filePath, std::ios::binary);
        if (!file.is_open()) {
            return false;
        }
        
        // Read header
        FileHeader header;
        file.read(reinterpret_cast<char*>(&header), sizeof(FileHeader));
        
        // Verify magic number
        if (std::memcmp(header.magic, MAGIC_NUMBER, 8) != 0) {
            return false;
        }
        
        // Check version
        if (header.version != VERSION) {
            return false;
        }
        
        graphId = header.graphId;
        partitionId = header.partitionId;
        snapshotId = header.snapshotId;
        bool compressed = (header.flags & FLAG_COMPRESSED) != 0;
        
        // Read edge section
        uint64_t edgeCount;
        file.read(reinterpret_cast<char*>(&edgeCount), sizeof(uint64_t));
        
        for (uint64_t i = 0; i < edgeCount; ++i) {
            std::string sourceId = readString(file);
            std::string destId = readString(file);
            
            // Read Roaring bitmap data
            uint32_t dataSize;
            file.read(reinterpret_cast<char*>(&dataSize), sizeof(uint32_t));
            
            std::string bitmapData(dataSize, '\0');
            file.read(&bitmapData[0], dataSize);
            
            // Deserialize Roaring bitmap
            EdgeLifespanBitmap bitmap = EdgeLifespanBitmap::deserialize(bitmapData);
            EdgeKey key(sourceId, destId);
            edgeBitmaps[key] = bitmap;
        }
        
        // Read node properties
        uint64_t nodePropCount;
        file.read(reinterpret_cast<char*>(&nodePropCount), sizeof(uint64_t));
        
        for (uint64_t i = 0; i < nodePropCount; ++i) {
            std::string nodeId = readString(file);
            std::string propData = readString(file);
            
            // Deserialize property dictionary (simplified)
            PropertyIntervalDictionary propDict;
            // Note: Would need to implement deserialize() for PropertyIntervalDictionary
            nodeProperties[nodeId] = propDict;
        }
        
        // Read edge properties
        uint64_t edgePropCount;
        file.read(reinterpret_cast<char*>(&edgePropCount), sizeof(uint64_t));
        
        for (uint64_t i = 0; i < edgePropCount; ++i) {
            std::string edgeId = readString(file);
            std::string propData = readString(file);
            
            // Parse edge ID
            size_t arrowPos = edgeId.find("->");
            if (arrowPos != std::string::npos) {
                std::string src = edgeId.substr(0, arrowPos);
                std::string dst = edgeId.substr(arrowPos + 2);
                EdgeKey key(src, dst);
                
                PropertyIntervalDictionary propDict;
                edgeProperties[key] = propDict;
            }
        }
        
        file.close();
        return true;
    }
    
    /**
     * Get file info without loading entire file
     */
    static bool getFileInfo(const std::string& filePath,
                           uint32_t& graphId,
                           uint32_t& partitionId,
                           uint32_t& snapshotId,
                           uint64_t& edgeCount,
                           uint64_t& timestamp) {
        std::ifstream file(filePath, std::ios::binary);
        if (!file.is_open()) {
            return false;
        }
        
        FileHeader header;
        file.read(reinterpret_cast<char*>(&header), sizeof(FileHeader));
        
        if (std::memcmp(header.magic, MAGIC_NUMBER, 8) != 0) {
            return false;
        }
        
        graphId = header.graphId;
        partitionId = header.partitionId;
        snapshotId = header.snapshotId;
        edgeCount = header.edgeCount;
        timestamp = header.timestamp;
        
        file.close();
        return true;
    }
    
    /**
     * Generate snapshot file path
     */
    static std::string generateFilePath(const std::string& baseDir,
                                        uint32_t graphId,
                                        uint32_t partitionId,
                                        uint32_t snapshotId) {
        return baseDir + "/graph" + std::to_string(graphId) + 
               "_part" + std::to_string(partitionId) +
               "_snap" + std::to_string(snapshotId) + ".tgs";
    }
};

// Initialize static member
inline const char* TemporalStorePersistence::MAGIC_NUMBER = "JGTSTORE";

#endif // TEMPORAL_STORE_PERSISTENCE_H
