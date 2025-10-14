/**
Append-only temporal property stores for edges and vertices (EP/VP logs).
*/

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

#include "TemporalTypes.h"

namespace jasminegraph {

class EdgePropertyStore {
public:
    struct Paths { std::string baseDir; };
    explicit EdgePropertyStore(const Paths& paths);
    ~EdgePropertyStore();

    // Append a property interval for an edge. EndSnapshot is inclusive; use max to denote open interval.
    void append(EdgeID edgeId, const std::string& key, const std::string& value,
                SnapshotID startSnapshot, SnapshotID endSnapshot);

    // Read value valid at snapshot if any.
    std::optional<std::string> get(EdgeID edgeId, SnapshotID snapshot, const std::string& key) const;

private:
    struct Impl; std::unique_ptr<Impl> impl;
};

class VertexPropertyStore {
public:
    struct Paths { std::string baseDir; };
    explicit VertexPropertyStore(const Paths& paths);
    ~VertexPropertyStore();

    void append(VertexID vertexId, const std::string& key, const std::string& value,
                SnapshotID startSnapshot, SnapshotID endSnapshot);

    std::optional<std::string> get(VertexID vertexId, SnapshotID snapshot, const std::string& key) const;

private:
    struct Impl; std::unique_ptr<Impl> impl;
};

} // namespace jasminegraph


