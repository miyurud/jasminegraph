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