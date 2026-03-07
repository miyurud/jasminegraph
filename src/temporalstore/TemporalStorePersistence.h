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
#include <chrono>
#include <dirent.h>
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

    // ── Bitmap index file header (64 bytes) ─────────────────────────────────
    // File: graph{G}_part{P}_bitmaps.ebm
    // A single, overwritten-in-place file holding ALL edge bitmaps.
    // Replaces the per-snapshot fan of _snapN.tgs files in the hot path.
    struct BitmapFileHeader {
        char     magic[8];           // "JGBINDEX"
        uint32_t version;            // 1
        uint32_t graphId;
        uint32_t partitionId;
        uint32_t latestSnapshotId;   // highest snapshot ID stored in bitmaps
        uint64_t edgeCount;          // number of edge records
        uint64_t timestamp;          // write time (ns since epoch)
        uint32_t flags;              // bit0 = compressed (reserved, always 0 now)
        char     reserved[20];
    };  // 64 bytes

    // ── Snapshot metadata file ───────────────────────────────────────────────
    // File: graph{G}_part{P}_snapmeta.bin
    // Append-only, 32-byte header + one 32-byte record per closed snapshot.
    struct MetaFileHeader {
        char     magic[8];      // "JGMETA00"
        uint32_t version;       // 1
        uint32_t graphId;
        uint32_t partitionId;
        char     reserved[12];
    };  // 32 bytes

    struct SnapshotMetaRecord {
        uint32_t snapshotId;
        uint32_t _pad;
        uint64_t totalEdges;    // total edges in bitmap index after this snapshot
        uint64_t newEdges;      // edges newly added in this snapshot
        uint64_t timestamp;     // close time (ns since epoch)
    };  // 32 bytes
    
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
    template<typename EdgeBitmapMap, typename EdgePropMap>
    static bool saveSnapshot(
        const std::string& filePath,
        uint32_t graphId,
        uint32_t partitionId,
        uint32_t snapshotId,
        const EdgeBitmapMap& edgeBitmaps,
        const std::map<std::string, PropertyIntervalDictionary>& nodeProperties,
        const EdgePropMap& edgeProperties,
        bool compress = true) {
        using EdgeKey = typename EdgeBitmapMap::key_type;
        
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
    template<typename EdgeBitmapMap, typename EdgePropMap>
    static bool loadSnapshot(
        const std::string& filePath,
        uint32_t& graphId,
        uint32_t& partitionId,
        uint32_t& snapshotId,
        EdgeBitmapMap& edgeBitmaps,
        std::map<std::string, PropertyIntervalDictionary>& nodeProperties,
        EdgePropMap& edgeProperties) {
        using EdgeKey = typename EdgeBitmapMap::key_type;
        
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

    /**
     * Path for the single persistent bitmap index file.
     * graph{G}_part{P}_bitmaps.ebm
     * This file is rewritten in full each time a snapshot closes.
     */
    static std::string generateBitmapFilePath(const std::string& baseDir,
                                              uint32_t graphId,
                                              uint32_t partitionId) {
        return baseDir + "/graph" + std::to_string(graphId) +
               "_part" + std::to_string(partitionId) + "_bitmaps.ebm";
    }

    /**
     * Path for the snapshot metadata file.
     * graph{G}_part{P}_snapmeta.bin
     * Append-only: one 32-byte MetaFileHeader followed by one SnapshotMetaRecord
     * per closed snapshot.
     */
    static std::string generateMetaFilePath(const std::string& baseDir,
                                            uint32_t graphId,
                                            uint32_t partitionId) {
        return baseDir + "/graph" + std::to_string(graphId) +
               "_part" + std::to_string(partitionId) + "_snapmeta.bin";
    }

    /**
     * Save the full edge bitmap index to disk (compressed).
     * Rewrites graph{G}_part{P}_bitmaps.ebm in full.
     * Called once per snapshot-close instead of saveSnapshot().
     */
    template<typename EdgeBitmapMap>
    static bool saveBitmapIndex(
        const std::string& filePath,
        uint32_t graphId,
        uint32_t partitionId,
        uint32_t latestSnapshotId,
        const EdgeBitmapMap& edgeBitmaps) {

        std::ofstream file(filePath, std::ios::binary | std::ios::trunc);
        if (!file.is_open()) return false;

        // Write header
        BitmapFileHeader header;
        std::memcpy(header.magic, "JGBINDEX", 8);
        header.version          = 1;
        header.graphId          = graphId;
        header.partitionId      = partitionId;
        header.latestSnapshotId = latestSnapshotId;
        header.edgeCount        = edgeBitmaps.size();
        header.timestamp        = static_cast<uint64_t>(
            std::chrono::system_clock::now().time_since_epoch().count());
        header.flags            = 0;   // Roaring is self-compressed; no extra layer
        std::memset(header.reserved, 0, 20);
        file.write(reinterpret_cast<const char*>(&header), sizeof(BitmapFileHeader));

        // Write edge count
        uint64_t edgeCount = edgeBitmaps.size();
        file.write(reinterpret_cast<const char*>(&edgeCount), sizeof(uint64_t));

        // Write each edge: src, dst, serialized Roaring bitmap
        for (const auto& pair : edgeBitmaps) {
            const auto& key    = pair.first;
            const auto& bitmap = pair.second;

            writeString(file, key.sourceId);
            writeString(file, key.destId);

            std::string bitmapData = bitmap.serialize();
            uint32_t dataSize = static_cast<uint32_t>(bitmapData.size());
            file.write(reinterpret_cast<const char*>(&dataSize), sizeof(uint32_t));
            file.write(bitmapData.data(), dataSize);
        }

        file.close();
        return file.good() || true;  // flush errors caught by close
    }

    /**
     * Load the bitmap index from disk.
     * Populates edgeBitmaps and returns the latestSnapshotId stored in the header.
     */
    template<typename EdgeBitmapMap>
    static bool loadBitmapIndex(
        const std::string& filePath,
        uint32_t& graphId,
        uint32_t& partitionId,
        uint32_t& latestSnapshotId,
        EdgeBitmapMap& edgeBitmaps) {
        using EdgeKey = typename EdgeBitmapMap::key_type;

        std::ifstream file(filePath, std::ios::binary);
        if (!file.is_open()) return false;

        BitmapFileHeader header;
        file.read(reinterpret_cast<char*>(&header), sizeof(BitmapFileHeader));

        if (std::memcmp(header.magic, "JGBINDEX", 8) != 0) return false;
        if (header.version != 1) return false;

        graphId          = header.graphId;
        partitionId      = header.partitionId;
        latestSnapshotId = header.latestSnapshotId;

        uint64_t edgeCount;
        file.read(reinterpret_cast<char*>(&edgeCount), sizeof(uint64_t));

        edgeBitmaps.clear();
        for (uint64_t i = 0; i < edgeCount; ++i) {
            std::string sourceId = readString(file);
            std::string destId   = readString(file);

            uint32_t dataSize;
            file.read(reinterpret_cast<char*>(&dataSize), sizeof(uint32_t));
            std::string bitmapData(dataSize, '\0');
            file.read(&bitmapData[0], dataSize);

            EdgeKey key(sourceId, destId);
            edgeBitmaps[key] = EdgeLifespanBitmap::deserialize(bitmapData);
        }

        file.close();
        return true;
    }

    /**
     * Append a 32-byte record to the snapshot metadata file.
     * Creates the file (with 32-byte header) on first call.
     */
    static bool appendSnapshotMeta(
        const std::string& metaFilePath,
        uint32_t graphId,
        uint32_t partitionId,
        uint32_t snapshotId,
        uint64_t totalEdges,
        uint64_t newEdges) {

        // Create header on first write
        bool needsHeader = false;
        {
            std::ifstream check(metaFilePath, std::ios::binary);
            needsHeader = !check.is_open();
        }

        std::ofstream file(metaFilePath, std::ios::binary | std::ios::app);
        if (!file.is_open()) return false;

        if (needsHeader) {
            char hdr[32] = {};
            std::memcpy(hdr, "JGMETA00", 8);
            uint32_t ver = 1;
            std::memcpy(hdr + 8,  &ver,         4);
            std::memcpy(hdr + 12, &graphId,      4);
            std::memcpy(hdr + 16, &partitionId,  4);
            file.write(hdr, 32);
        }

        SnapshotMetaRecord rec;
        rec.snapshotId = snapshotId;
        rec._pad       = 0;
        rec.totalEdges = totalEdges;
        rec.newEdges   = newEdges;
        rec.timestamp  = static_cast<uint64_t>(
            std::chrono::system_clock::now().time_since_epoch().count());

        file.write(reinterpret_cast<const char*>(&rec), sizeof(SnapshotMetaRecord));
        file.close();
        return true;
    }

    /**
     * Read ALL snapshot meta records from graph{G}_part{P}_snapmeta.bin.
     * Records are sorted in append order (oldest first).
     * Returns empty vector if the file does not exist.
     */
    static std::vector<SnapshotMetaRecord> readAllSnapmeta(const std::string& metaFilePath) {
        std::vector<SnapshotMetaRecord> records;
        std::ifstream file(metaFilePath, std::ios::binary | std::ios::ate);
        if (!file.is_open()) return records;

        std::streamsize fileSize = file.tellg();
        const std::streamsize HEADER_SIZE = static_cast<std::streamsize>(sizeof(MetaFileHeader));
        const std::streamsize RECORD_SIZE = static_cast<std::streamsize>(sizeof(SnapshotMetaRecord));

        if (fileSize < HEADER_SIZE) return records;

        std::streamsize dataSize = fileSize - HEADER_SIZE;
        std::streamsize nRecords = dataSize / RECORD_SIZE;
        if (nRecords <= 0) return records;

        file.seekg(HEADER_SIZE);
        records.resize(nRecords);
        file.read(reinterpret_cast<char*>(records.data()),
                  nRecords * RECORD_SIZE);
        file.close();
        return records;
    }

    /**
     * Read the latest snapshot ID from graph{G}_part{P}_snapmeta.bin.
     * Returns UINT32_MAX if the file does not exist or has no records.
     */
    static uint32_t readLatestSnapshotId(const std::string& metaFilePath) {
        std::ifstream file(metaFilePath, std::ios::binary | std::ios::ate);
        if (!file.is_open()) return UINT32_MAX;

        std::streamsize fileSize = file.tellg();
        const std::streamsize HEADER_SIZE = static_cast<std::streamsize>(sizeof(MetaFileHeader));
        const std::streamsize RECORD_SIZE = static_cast<std::streamsize>(sizeof(SnapshotMetaRecord));

        if (fileSize < HEADER_SIZE + RECORD_SIZE) return UINT32_MAX;

        file.seekg(fileSize - RECORD_SIZE);
        SnapshotMetaRecord rec;
        file.read(reinterpret_cast<char*>(&rec), RECORD_SIZE);
        file.close();
        return rec.snapshotId;
    }

    /**
     * Find the highest snapshot ID for a given graph and partition
     * Returns 0 if no snapshots exist
     * 
     * This scans the snapshot directory and finds the maximum snapshot ID
     * for the specified graph/partition combination. Used when restarting
     * to continue from the last saved snapshot.
     */
    static uint32_t findHighestSnapshotId(const std::string& baseDir,
                                          uint32_t graphId,
                                          uint32_t partitionId) {
        uint32_t maxSnapshotId = 0;
        
        // Generate prefix pattern for this graph/partition
        std::string prefix = "graph" + std::to_string(graphId) + 
                           "_part" + std::to_string(partitionId) + 
                           "_snap";
        
        // Try to open directory
        DIR* dir = opendir(baseDir.c_str());
        if (!dir) {
            return 0;
        }
        
        struct dirent* entry;
        while ((entry = readdir(dir)) != nullptr) {
            std::string filename(entry->d_name);
            
            // Check if this file matches our pattern
            if (filename.find(prefix) == 0 && filename.find(".tgs") != std::string::npos) {
                // Extract snapshot ID from filename
                // Format: graph{ID}_part{ID}_snap{ID}.tgs
                size_t snapPos = filename.find("_snap") + 5;  // Position after "_snap"
                size_t dotPos = filename.find(".tgs");
                
                if (snapPos != std::string::npos && dotPos != std::string::npos) {
                    std::string snapIdStr = filename.substr(snapPos, dotPos - snapPos);
                    try {
                        uint32_t snapshotId = std::stoul(snapIdStr);
                        if (snapshotId > maxSnapshotId) {
                            maxSnapshotId = snapshotId;
                        }
                    } catch (...) {
                        // Skip invalid filenames
                    }
                }
            }
        }
        
        closedir(dir);
        return maxSnapshotId;
    }
};

// Initialize static members
inline const char* TemporalStorePersistence::MAGIC_NUMBER = "JGTSTORE";

#endif // TEMPORAL_STORE_PERSISTENCE_H
