/**
Copyright 2025 JasmineGraph Team
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**/

#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace jasminegraph {

// Basic temporal identifiers used across the temporal storage module
using SnapshotID = std::uint64_t;
using EdgeID = std::uint64_t;
using VertexID = std::uint64_t;

// Stream operation semantics
enum class StreamOp { Insert, Update, Delete };

struct StreamVertex {
    std::string id; // external string identifier
    std::uint32_t pid = 0; // partition id if provided
    std::unordered_map<std::string, std::string> properties; // temporal property payload
};

struct StreamEdgeRecord {
    StreamVertex source;
    StreamVertex destination;
    std::unordered_map<std::string, std::string> properties; // edge property payload
    std::string eventTimeISO8601; // empty if not provided; ingestion time will be used
    StreamOp op = StreamOp::Insert;
};

struct EdgeRecordOnDisk {
    EdgeID edgeId;
    VertexID srcId;
    VertexID dstId;
    std::uint64_t propertiesPtr; // opaque pointer/offset into EP log
};

struct TemporalEdgeRef {
    EdgeID edgeId;
    std::uint64_t bitmapOffset; // offset/handle maintained by BitmapManager
};

} // namespace jasminegraph


