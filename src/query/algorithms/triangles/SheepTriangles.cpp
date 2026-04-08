/**
Copyright 2024 JasminGraph Team
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

#include "SheepTriangles.h"
#include "../../../util/logger/Logger.h"
#include "../../../util/telemetry/OpenTelemetryUtil.h"
#include <chrono>
#include <algorithm>

Logger sheep_triangle_logger;

long SheepTriangles::run(JasmineGraphHashMapLocalStore &graphDB,
                         JasmineGraphHashMapCentralStore &centralStore,
                         JasmineGraphHashMapDuplicateCentralStore &duplicateCentralStore,
                         std::string graphId,
                         std::string partitionId) {
    OTEL_TRACE_FUNCTION();
    
    std::string workerInfo = "worker_" + graphId + "_partition_" + partitionId;
    sheep_triangle_logger.info("###SHEEP-TRIANGLE### " + workerInfo + " Starting optimized sheep triangle counting");
    
    auto startTime = std::chrono::high_resolution_clock::now();
    
    // Extract data from stores
    std::map<long, std::unordered_set<long>> localMap;
    std::map<long, std::unordered_set<long>> centralMap;
    std::map<long, std::unordered_set<long>> duplicateMap;
    
    {
        ScopedTracer data_extract("sheep_extract_data");
        localMap = graphDB.getUnderlyingHashMap();
        centralMap = centralStore.getUnderlyingHashMap();
        duplicateMap = duplicateCentralStore.getUnderlyingHashMap();
    }
    
    // Merge stores efficiently
    {
        ScopedTracer merge_phase("sheep_merge_stores");
        mergeStores(localMap, centralMap, duplicateMap);
    }
    
    auto mergeEnd = std::chrono::high_resolution_clock::now();
    auto mergeDuration = std::chrono::duration_cast<std::chrono::milliseconds>(mergeEnd - startTime).count();
    sheep_triangle_logger.info("Merge time: " + std::to_string(mergeDuration) + " ms");
    
    // Count triangles with optimized algorithm
    SheepTriangleResult result;
    {
        ScopedTracer count_phase("sheep_count_triangles");
        result = countTriangles(localMap, false);
    }
    
    auto endTime = std::chrono::high_resolution_clock::now();
    auto totalDuration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime).count();
    sheep_triangle_logger.info("###SHEEP-TRIANGLE### " + workerInfo + " Complete. Count: " + 
                              std::to_string(result.count) + " Time: " + std::to_string(totalDuration) + " ms");
    
    return result.count;
}

void SheepTriangles::mergeStores(
    std::map<long, std::unordered_set<long>> &localMap,
    std::map<long, std::unordered_set<long>> &centralMap,
    std::map<long, std::unordered_set<long>> &duplicateMap) {
    
    // Merge duplicate central into central (avoiding duplicates)
    for (const auto &entry : duplicateMap) {
        long vertex = entry.first;
        const auto &edges = entry.second;
        centralMap[vertex].insert(edges.begin(), edges.end());
    }
    
    // Merge central into local
    for (const auto &entry : centralMap) {
        long vertex = entry.first;
        const auto &edges = entry.second;
        localMap[vertex].insert(edges.begin(), edges.end());
    }
}

SheepTriangleResult SheepTriangles::countTriangles(
    std::map<long, std::unordered_set<long>> &edgeMap,
    bool returnTriangles) {
    
    SheepTriangleResult result;
    result.count = 0;
    
    std::ostringstream triangleStream;
    std::unordered_set<std::string> seenTriangles;  // Prevent duplicates
    
    // For each vertex u
    for (const auto &u_entry : edgeMap) {
        long u = u_entry.first;
        const auto &u_neighbors = u_entry.second;
        
        // For each neighbor v of u where v > u (ordering prevents duplicates)
        for (long v : u_neighbors) {
            if (v <= u) continue;  // Enforce u < v
            
            auto v_it = edgeMap.find(v);
            if (v_it == edgeMap.end()) continue;
            const auto &v_neighbors = v_it->second;
            
            // For each neighbor w of u where w > v (enforce u < v < w)
            for (long w : u_neighbors) {
                if (w <= v) continue;  // Enforce v < w
                
                // Check if v is connected to w (completing the triangle)
                if (v_neighbors.find(w) != v_neighbors.end()) {
                    // Found triangle (u, v, w) with u < v < w
                    result.count++;
                    
                    if (returnTriangles) {
                        triangleStream << u << "," << v << "," << w << ":";
                    }
                }
            }
        }
    }
    
    if (returnTriangles) {
        std::string triangles = triangleStream.str();
        if (triangles.empty()) {
            result.triangles = "NILL";
        } else {
            triangles.pop_back();  // Remove trailing ':'
            result.triangles = std::move(triangles);
        }
    }
    
    return result;
}
