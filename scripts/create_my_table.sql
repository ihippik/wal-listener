CREATE TABLE IF NOT EXISTS my_table (
    id BIGSERIAL PRIMARY KEY,
    name varchar(255) NOT NULL,
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);