package listener

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx"
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
func (r RepositoryImpl) GetSlotLSN(ctx context.Context, slotName string) (string, error) {
	var restartLSNStr string

	err := r.conn.QueryRowEx(ctx, "SELECT restart_lsn FROM pg_replication_slots WHERE slot_name=$1;", nil, slotName).
		Scan(&restartLSNStr)

	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}

	return restartLSNStr, err
}

// CreatePublication create publication fo all.
func (r RepositoryImpl) CreatePublication(ctx context.Context, name string) error {
	if _, err := r.conn.ExecEx(ctx, `CREATE PUBLICATION "`+name+`" FOR ALL TABLES`, nil); err != nil {
		return fmt.Errorf("exec: %w", err)
	}

	return nil
}

// NewStandbyStatus return standby status with WAL positions.
func (r RepositoryImpl) NewStandbyStatus(walPositions ...uint64) (status *pgx.StandbyStatus, err error) {
	return pgx.NewStandbyStatus(walPositions...)
}

// IsAlive check database connection problems.
func (r RepositoryImpl) IsAlive() bool {
	return r.conn.IsAlive()
}

// Close database connection.
func (r RepositoryImpl) Close() error {
	return r.conn.Close()
}

// IsReplicationActive returns true if the replication slot is already active, false otherwise.
func (r RepositoryImpl) IsReplicationActive(ctx context.Context, slotName string) (bool, error) {
	var activePID int

	err := r.conn.QueryRowEx(ctx, "SELECT active_pid FROM pg_replication_slots WHERE slot_name=$1 AND active=true;", nil, slotName).
		Scan(&activePID)

	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}

	return true, err
}
