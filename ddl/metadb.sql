create table graph
(
    idgraph                     INTEGER      not null
        primary key,
    name                        VARCHAR(25)  not null,
    upload_path                 VARCHAR(300) not null,
    upload_start_time           TIME         not null,
    upload_end_time             TIME         not null,
    graph_status_idgraph_status INTEGER      not null,
    vertexcount                 BIGINT,
    centralpartitioncount       INTEGER,
    edgecount                   INTEGER,
    upload_time                 VARCHAR(8),
    train_status                VARCHAR(20),
    feature_count               INTEGER(100),
    is_directed                 boolean default false,
    feature_type                VARCHAR(10)
);

create index index_idgraph
    on graph (idgraph);

create table graph_status
(
    idgraph_status INTEGER not null
        primary key,
    description    VARCHAR(200)
);

create table host
(
    idhost    INTEGER not null
        primary key,
    name      VARCHAR(200),
    ip        VARCHAR(200),
    is_public BOOLEAN default false
);

create index index_idhost
    on host (idhost);

create table model
(
    idmodel                     INTEGER      not null
        primary key,
    name                        VARCHAR(25)  not null,
    upload_path                 VARCHAR(300) not null,
    upload_time                 TIME         not null,
    model_status_idmodel_status VARCHAR(10)  not null
);

create table partition
(
    idpartition                 INTEGER not null,
    graph_idgraph               INTEGER,
    vertexcount                 INTEGER,
    central_vertexcount         INTEGER,
    edgecount                   INTEGER,
    central_edgecount           INTEGER,
    central_edgecount_with_dups INTEGER
);

create table worker
(
    host_idhost      INTEGER      not null,
    server_port      VARCHAR(200) not null,
    server_data_port VARCHAR(200),
    user             REAL,
    name             VARCHAR(200),
    ip               VARCHAR(200),
    is_public        BOOLEAN,
    idworker         INTEGER      not null
        primary key,
    status           VARCHAR(200)
);

create table worker_has_partition
(
    partition_idpartition   VARCHAR,
    partition_graph_idgraph VARCHAR,
    worker_idworker         INTEGER
);

INSERT INTO graph_status (idgraph_status, description) VALUES (1, 'LOADING');
INSERT INTO graph_status (idgraph_status, description) VALUES (2, 'OPERATIONAL');
INSERT INTO graph_status (idgraph_status, description) VALUES (3, 'DELETED');
INSERT INTO graph_status (idgraph_status, description) VALUES (4, 'NONOPERATIONAL');