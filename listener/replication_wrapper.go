package listener

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type ReplicationWrapper struct {
	conn *pgconn.PgConn
	log  *slog.Logger
}

func NewReplicationWrapper(conn *pgconn.PgConn, log *slog.Logger) *ReplicationWrapper {
	return &ReplicationWrapper{conn: conn, log: log}
}

func (r *ReplicationWrapper) IdentifySystem() (pglogrepl.IdentifySystemResult, error) {
	mrr := r.conn.Exec(context.Background(), "IDENTIFY_SYSTEM")

	var isr pglogrepl.IdentifySystemResult
	results, err := mrr.ReadAll()
	if err != nil {
		return isr, err
	}

	if len(results) != 1 {
		return isr, fmt.Errorf("expected 1 result set, got %d", len(results))
	}

	result := results[0]
	if len(result.Rows) != 1 {
		return isr, fmt.Errorf("expected 1 result row, got %d", len(result.Rows))
	}

	row := result.Rows[0]
	if len(row) < 4 {
		return isr, fmt.Errorf("expected at least 4 result columns, got %d", len(row))
	}

	isr.SystemID = string(row[0])
	timeline, err := strconv.ParseInt(string(row[1]), 10, 32)
	if err != nil {
		return isr, fmt.Errorf("failed to parse timeline: %w", err)
	}
	isr.Timeline = int32(timeline)

	isr.XLogPos, err = pglogrepl.ParseLSN(string(row[2]))
	if err != nil {
		return isr, fmt.Errorf("failed to parse xlogpos as LSN: %w", err)
	}

	isr.DBName = string(row[3])

	return isr, nil
}

func (r *ReplicationWrapper) CreateReplicationSlotEx(slotName, outputPlugin string) error {
	_, err := pglogrepl.CreateReplicationSlot(context.Background(), r.conn, slotName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{Temporary: false})
	if err != nil {
		return fmt.Errorf("CreateReplicationSlot failed: %w", err)
	}
	r.log.Info("created temporary replication slot:", slog.String(slotName, ""))

	return nil
}

func (r *ReplicationWrapper) StartReplication(slotName string, startLsn pglogrepl.LSN, pluginArguments ...string) (err error) {
	err = pglogrepl.StartReplication(context.Background(), r.conn, slotName, startLsn, pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	if err != nil {
		return fmt.Errorf("StartReplication failed: %w", err)
	}
	return nil
}

func (r *ReplicationWrapper) SendStandbyStatus(ctx context.Context, lsn pglogrepl.LSN, withReply bool) error {
	err := pglogrepl.SendStandbyStatusUpdate(ctx, r.conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: lsn,
		ReplyRequested:   withReply,
	})
	if err != nil {
		return fmt.Errorf("send standby status update: %w", err)
	}
	return nil
}

func (r *ReplicationWrapper) WaitForReplicationMessage(ctx context.Context) (*pgproto3.CopyData, error) {
	rawMsg, err := r.conn.ReceiveMessage(ctx)
	if err != nil {
		return nil, fmt.Errorf("receive message: %w", err)
	}

	if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
		return nil, fmt.Errorf("received Postgres WAL error: %+v", errMsg)
	}

	msg, ok := rawMsg.(*pgproto3.CopyData)
	if !ok {
		return nil, fmt.Errorf("received unexpected message: %T", rawMsg)
	}

	// we run into pointer / data overwrite issues if we don't copy the data
	msgCopy := make([]byte, len(msg.Data))
	copy(msgCopy, msg.Data)

	return &pgproto3.CopyData{Data: msgCopy}, nil
}

func (r *ReplicationWrapper) IsAlive() bool {
	return !r.conn.IsClosed()
}

func (r *ReplicationWrapper) DropReplicationSlot(slotName string) error {
	result := r.conn.Exec(context.Background(), fmt.Sprintf("DROP PUBLICATION IF EXISTS %s;", slotName))
	_, err := result.ReadAll()
	if err != nil {
		return fmt.Errorf("cannot drop publication if exists: %w", err)
	}
	return nil
}

func (r *ReplicationWrapper) Close(ctx context.Context) error {
	if err := r.conn.Close(ctx); err != nil {
		return fmt.Errorf("cannot close replication connection: %w", err)
	}
	return nil
}
