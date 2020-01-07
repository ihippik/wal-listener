package listener

import "github.com/jackc/pgx"

type repositoryImpl struct {
	conn *pgx.Conn
}

func NewRepository(conn *pgx.Conn) *repositoryImpl {
	return &repositoryImpl{conn: conn}
}

func (r repositoryImpl) GetSlotLSN(slotName string) (string, error) {
	var restartLSNStr string
	err := r.conn.QueryRow(
		"SELECT restart_lsn FROM pg_replication_slots WHERE slot_name=$1;",
		slotName,
	).Scan(&restartLSNStr)
	return restartLSNStr, err
}

func (r repositoryImpl) IsAlive() bool {
	return r.conn.IsAlive()
}

func (r repositoryImpl) Close() error {
	return r.conn.Close()
}
