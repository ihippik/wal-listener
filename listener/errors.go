package listener

import "errors"

// Constants with error text message
const (
	ErrPostgresConnection    = "db connection error"
	ErrReplicationConnection = "replication connection error"
	ErrNatsConnection        = "nats connection error"
	ErrMarshalMsg            = "marshal wal message error"
	ErrUnmarshalMsg          = "unmarshal wal message error"
	ErrAckWalMessage         = "acknowledge wal message error"
	ErrValidateMessage       = "message validate error"
	ErrSendStandbyStatus     = "send standby status error"
)

// Variable with connection errors.
var (
	errReplConnectionIsLost = errors.New("replication connection to postgres is lost")
	errConnectionIsLost     = errors.New("db connection to postgres is lost")
	ErrMessageLost          = errors.New("messages are lost")
)

type serviceErr struct {
	Caller string
	Err    error
}

func newListenerError(caller string, err error) *serviceErr {
	return &serviceErr{Caller: caller, Err: err}
}

func (e *serviceErr) Error() string {
	return e.Caller + ": " + e.Err.Error()
}
