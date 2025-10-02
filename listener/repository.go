package listener

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// RepositoryImpl service repository.
type RepositoryImpl struct {
	conn *pgx.Conn
}

// NewRepository returns a new instance of the repository.
func NewRepository(conn *pgx.Conn) *RepositoryImpl {
	return &RepositoryImpl{conn: conn}
}

// GetSlotLSN returns the value of the last offset for a specific slot.
func (r RepositoryImpl) GetSlotLSN(ctx context.Context, slotName string) (*string, error) {
	var restartLSNStr *string

	err := r.conn.QueryRow(ctx, "SELECT restart_lsn FROM pg_replication_slots WHERE slot_name=$1;", slotName).
		Scan(&restartLSNStr)

	return restartLSNStr, err
}

// GetSlotRetainedWALBytes returns the retained bytes of the replication slot.
func (r RepositoryImpl) GetSlotRetainedWALBytes(ctx context.Context, slotName string) (*int64, error) {
	var retainedWALBytes *int64

	err := r.conn.QueryRow(ctx, "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS retained_wal_bytes FROM pg_replication_slots WHERE slot_name=$1;", slotName).
		Scan(&retainedWALBytes)

	return retainedWALBytes, err
}

// CreatePublication create publication fo all.
func (r RepositoryImpl) CreatePublication(ctx context.Context, name string) error {
	if _, err := r.conn.Exec(ctx, `CREATE PUBLICATION "`+name+`" FOR ALL TABLES`); err != nil && !strings.Contains("already exists", err.Error()) {
		return fmt.Errorf("exec: %w", err)
	}

	return nil
}

// IsAlive check database connection problems.
func (r RepositoryImpl) IsAlive() bool {
	return !r.conn.IsClosed()
}

// Close database connection.
func (r RepositoryImpl) Close(ctx context.Context) error {
	return r.conn.Close(ctx)
}
