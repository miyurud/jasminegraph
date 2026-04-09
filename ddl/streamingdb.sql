create table central_store
(
    triangles integer,
    graph_id  integer
        constraint central_store_pk
            primary key
);

create table streaming_partition
(
    partition_id  integer,
    local_edges   integer,
    triangles     integer,
    central_edges integer,
    graph_id      integer,
    constraint streaming_partition_pk
        primary key (graph_id, partition_id)
);

-- ============================================================================
-- TEMPORAL STORAGE TABLES (Added for temporal graph support)
-- ============================================================================

-- Table 1: temporal_snapshots
-- Purpose: Metadata about each snapshot (when created, size, status)
-- Example: snapshot_id=42, timestamp=1735574400, edge_count=10000, is_closed=1
create table if not exists temporal_snapshots
(
    snapshot_id   integer primary key,
    graph_id      integer not null,
    partition_id  integer not null,
    timestamp     integer not null,  -- Unix timestamp (seconds since epoch)
    edge_count    integer default 0,
    vertex_count  integer default 0,
    is_closed     integer default 0, -- 0 = currently being filled, 1 = closed/finalized
    created_at    integer default (strftime('%s', 'now'))
);

-- Table 2: temporal_edges (New Edge Database - NEDB)
-- Purpose: Store each unique edge ONCE with bitmap tracking its snapshot presence
-- Example: edge (Alice→Bob) stored once, bitmap shows it exists in snapshots 1,2,3,6,7,8
create table if not exists temporal_edges
(
    edge_id              integer primary key autoincrement,
    graph_id             integer not null,
    partition_id         integer not null,
    source_id            text not null,      -- Source vertex ID
    destination_id       text not null,      -- Destination vertex ID
    edge_type            text default 'UNKNOWN',
    first_seen_snapshot  integer not null,   -- First snapshot this edge appeared
    last_seen_snapshot   integer,            -- Last snapshot (NULL = still active)
    lifespan_bitmap      blob,               -- Serialized EdgeLifespanBitmap
    unique(graph_id, partition_id, source_id, destination_id)
);

-- Table 3: temporal_properties
-- Purpose: Store property value changes as intervals
-- Example: node Alice's "city" property: [("NYC",1,10), ("LA",11,20), ("SF",21,∞)]
create table if not exists temporal_properties
(
    property_id       integer primary key autoincrement,
    edge_id           integer,              -- FK to temporal_edges (NULL for node properties)
    node_id           text,                 -- Node ID (NULL for edge properties)
    property_key      text not null,        -- Property name (e.g., "age", "color")
    property_intervals text,                -- Serialized PropertyIntervalDictionary (JSON)
    foreign key(edge_id) references temporal_edges(edge_id)
);

-- Performance indexes
create index if not exists idx_temporal_edges_graph 
    on temporal_edges(graph_id, partition_id);

create index if not exists idx_temporal_edges_source 
    on temporal_edges(source_id);

create index if not exists idx_temporal_edges_destination
    on temporal_edges(destination_id);

create index if not exists idx_temporal_properties_edge 
    on temporal_properties(edge_id);

create index if not exists idx_temporal_properties_node 
    on temporal_properties(node_id);

create index if not exists idx_temporal_properties_key
    on temporal_properties(property_key);

create index if not exists idx_temporal_snapshots_graph 
    on temporal_snapshots(graph_id, partition_id);

create index if not exists idx_temporal_snapshots_time
    on temporal_snapshots(timestamp);