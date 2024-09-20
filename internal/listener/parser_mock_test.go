package listener

import (
	"time"

	"github.com/stretchr/testify/mock"

	trx "github.com/ihippik/wal-listener/v2/internal/listener/transaction"
)

type parserMock struct {
	mock.Mock
}

func (p *parserMock) ParseWalMessage(msg []byte, tx *trx.WAL) error {
	args := p.Called(msg, tx)
	now := time.Now()

	tx.BeginTime = &now
	tx.CommitTime = &now
	tx.Actions = []trx.ActionData{
		{
			Schema: "public",
			Table:  "users",
			Kind:   "INSERT",
			NewColumns: []trx.Column{
				trx.InitColumn(nil, "id", 1, 23, true),
			},
		},
	}

	return args.Error(0)
}
