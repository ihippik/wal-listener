package transaction

import (
	"time"

	"github.com/stretchr/testify/mock"
)

type parserMock struct {
	mock.Mock
}

func (p *parserMock) ParseWalMessage(msg []byte, tx *WAL) error {
	args := p.Called(msg, tx)
	now := time.Now()

	tx.BeginTime = &now
	tx.CommitTime = &now
	tx.Actions = []ActionData{
		{
			Schema: "public",
			Table:  "users",
			Kind:   "INSERT",
			NewColumns: []Column{
				{
					name:      "id",
					value:     1,
					valueType: 23,
					isKey:     true,
				},
			},
		},
	}

	return args.Error(0)
}
