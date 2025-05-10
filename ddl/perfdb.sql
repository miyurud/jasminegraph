create table graph_performance_data
(
    id             INTEGER not null
        primary key,
    graph_name     TEXT,
    date_time      TEXT,
    worker_count   INTEGER,
    execution_time NUMERIC
);

create table graph_place_sla_performance
(
    id           INTEGER not null
        primary key,
    graph_sla_id INTEGER not null,
    place_id     INTEGER not null,
    memory_usage NUMERIC,
    load_average NUMERIC,
    elapsed_time INTEGER
);

create table graph_sla
(
    id              INTEGER not null
        primary key,
    id_sla_category INTEGER,
    graph_id        TEXT,
    partition_count INTEGER,
    sla_value       INTEGER,
    attempt         INTEGER
);

create table host
(
    idhost          INTEGER not null
        primary key,
    name            VARCHAR(200),
    ip              VARCHAR(200),
    is_public       BOOLEAN,
    total_cpu_cores INTEGER,
    total_memory    TEXT
);

create table host_performance_data
(
    id           INTEGER not null
        primary key,
    date_time    TEXT,
    memory_usage NUMERIC,
    cpu_usage    NUMERIC,
    idhost       INTEGER not null
);

create table place
(
    idplace          INTEGER not null
        primary key,
    host_idhost      INTEGER not null,
    server_port      VARCHAR(200),
    ip               VARCHAR(200),
    user             VARCHAR(200),
    is_master        BOOLEAN,
    is_host_reporter BOOLEAN
);

create table place_performance_data
(
    id           INTEGER not null
        primary key,
    idplace      INTEGER,
    memory_usage NUMERIC,
    cpu_usage    NUMERIC,
    date_time    TEXT
);

create table sla_category
(
    id       INTEGER not null
        primary key,
    command  TEXT    not null,
    category TEXT    not null
);

insert into sla_category (id, command, category)
values (1, 'trian', 'latency');

insert into sla_category (id, command, category)
values (2, 'pgrnk', 'latency');

insert into sla_category (id, command, category)
values (3, 'cypher', 'latency');
