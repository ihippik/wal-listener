CREATE USER replicator WITH REPLICATION ENCRYPTED PASSWORD 'replicator_password';

CREATE TABLE IF NOT EXISTS test_table (
    id BIGSERIAL PRIMARY KEY,
    name varchar(255) NOT NULL,
    source varchar(50) NOT NULL DEFAULT 'local',
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE test_table REPLICA IDENTITY FULL;

CREATE PUBLICATION test_pub FOR TABLE test_table;

GRANT SELECT ON test_table TO replicator;
GRANT USAGE ON SCHEMA public TO replicator;
