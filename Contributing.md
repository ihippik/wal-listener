## Development environment

We use nix for the dev environment. Run `direnv allow` to let it set everything up.

## Running a postgres

You can use these scripts to setup a local database

```sh
scripts/start-postgres.sh
```

then in another tab
```sh
scripts/setup-postgres.sh
```

## Running the listener

You can run with the stdout publisher via:

```sh
go run ./cmd/wal-listener/ --config ./config_stdout_example.yml
```

You can then connect to the postgres at `postgres://wal_development:wal_development@127.0.0.1:5432/wal_development` and run some queries to see them on stdout. Here are some examples:

```sql
-- Create a sample users table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL UNIQUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT true
);

-- Insert a sample record
INSERT INTO users (username, email)
SELECT 
    'user_' || md5(random()::text)::varchar(10), -- generates a random username with 'user_' prefix
    md5(random()::text) || '@example.com' -- generates a random email
;

```

#### Publishing a new docker container

Run:

```shell
docker buildx build --platform linux/amd64 --push -t us-central1-docker.pkg.dev/gadget-core-production/core-production/wal-listener:sha-$(git rev-parse HEAD) .
```
