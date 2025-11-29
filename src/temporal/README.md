# Temporal Graph Streaming Integration

This document describes the integration of TgStore-like temporal graph storage with JasmineGraph's streaming system.

## Architecture Overview

The integration provides a complete temporal streaming pipeline that processes graph operations (ADD/EDIT/DELETE) within time-based snapshots (Δt intervals), following the TgStore approach.

### Key Components

1. **Frontend Integration** (`JasmineGraphFrontEnd.cpp`)
   - Initializes `TemporalIntegration` when temporal streaming is enabled
   - Configures snapshot intervals (Δt) globally

2. **Stream Processing** (`StreamHandler.cpp`)
   - Routes edges to workers with operation types automatically detected from JSON
   - Maintains existing partitioning logic while adding temporal metadata

3. **Worker-Side Processing** (`JasmineGraphIncrementalLocalStore.cpp`)
   - Creates `TemporalPartitionManager` per partition when temporal mode is active
   - Buffers stream operations within Δt windows
   - Auto-flushes to create snapshots

4. **Temporal Storage** (`src/temporal/`)
   - `SnapshotManager`: Orchestrates snapshot creation and compaction
   - `BitmapManager`: Tracks edge activity across snapshots
   - `EdgeStorage`: Manages FEDB/NEDB structure
   - `PropertyStores`: Handle temporal properties

## Configuration

### Enable Temporal Streaming

Add these properties to `jasminegraph-server.properties`:

```properties
# Enable temporal streaming mode
org.jasminegraph.temporal.enabled=true

# Snapshot interval in seconds (Δt)
org.jasminegraph.temporal.snapshot.interval=60

# Base directory for temporal storage
org.jasminegraph.server.instance.datafolder=/path/to/data

# Auto-flush threshold (optional)
org.jasminegraph.temporal.flush.threshold=1000
```

### Kafka Message Format

Stream messages should include operation types:

```json
{
  "source": {
    "id": "user1",
    "properties": {
      "name": "Alice",
      "type": "person"
    }
  },
  "destination": {
    "id": "user2", 
    "properties": {
      "name": "Bob",
      "type": "person"
    }
  },
  "properties": {
    "relationship": "friend",
    "weight": 0.8,
    "event_timestamp": "1698537600000"
  },
  "operation_type": "ADD"
}
```

**Supported Operation Types:**
- `ADD`: Insert new edge
- `EDIT`: Update existing edge properties  
- `DELETE`: Remove edge (marks as inactive in bitmap)

## Usage Flow

### 1. Frontend Command
```bash
# Start temporal streaming
addstreaming
# Select temporal mode when prompted
# Specify graph ID and partition count
# Provide Kafka topic details
```

### 2. Automatic Processing
- Frontend initializes `TemporalIntegration`
- `StreamHandler` detects operation types from JSON
- Workers create `TemporalPartitionManager` instances
- Stream operations are buffered within Δt intervals

### 3. Snapshot Creation
- Auto-flush triggers every Δt seconds
- Manual flush available via high record count threshold
- Each snapshot gets unique `SnapshotID`
- Bitmaps updated based on operation types:
  - ADD/EDIT: Set active bit
  - DELETE: Clear active bit

### 4. Query Interface
```cpp
// Access temporal facade
jasminegraph::TemporalFacade* facade = jasminegraph::TemporalIntegration::instance();

// Query active edges at specific snapshot
std::vector<EdgeRecordOnDisk> edges = facade->getActiveEdgesFor(vertexId, snapshotId);

// Get temporal properties
std::optional<std::string> prop = facade->getEdgeProperty(edgeId, snapshotId, "relationship");
```

## File Structure

```
temporal/
├── TemporalTypes.h              # Basic types and structures
├── TemporalFacade.{h,cpp}       # Query interface
├── TemporalIntegration.{h,cpp}  # Global integration singleton
├── SnapshotManager.{h,cpp}      # Snapshot orchestration
├── BitmapManager.{h,cpp}        # Activity bitmap management
├── EdgeStorage.{h,cpp}          # FEDB/NEDB management
├── PropertyStores.{h,cpp}       # Temporal property logs
├── TemporalPartitionManager.{h,cpp}  # Worker-side partition management
├── TemporalStreamIngestor.{h,cpp}    # Kafka stream integration
└── TemporalIntegrationDemo.cpp  # Integration example
```

## Storage Layout

```
/data/temporal/
└── g{graphId}/
    └── p{partitionId}/
        ├── edges/          # FEDB/NEDB files
        ├── bitmaps/        # Compressed activity bitmaps
        ├── ep/             # Edge property logs
        └── vp/             # Vertex property logs
```

## Performance Considerations

### Snapshot Intervals (Δt)
- **Shorter intervals**: More frequent snapshots, higher overhead, fresher data
- **Longer intervals**: Fewer snapshots, better throughput, higher latency

### Buffer Management
- Auto-flush threshold prevents memory overflow
- Manual flush available for time-critical applications
- Configurable per partition

### Bitmap Compression
- Uses compressed bitmaps (e.g., Roaring) for space efficiency
- Activity tracking scales with edge count
- Compaction available for historical data
