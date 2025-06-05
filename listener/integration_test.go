package listener

import (
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/ihippik/wal-listener/v2/config"
	"github.com/ihippik/wal-listener/v2/publisher"
	"github.com/stretchr/testify/assert"
)

func TestOriginFilteringIntegration(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	
	pool := &sync.Pool{
		New: func() interface{} {
			return &publisher.Event{}
		},
	}

	t.Run("full pipeline with origin filtering enabled", func(t *testing.T) {
		tx := NewWalTransaction(logger, pool, new(monitorMock), nil, config.ExcludeStruct{}, map[string]string{}, true)
		
		tx.RelationStore[1] = RelationData{
			Schema: "public",
			Table:  "test_table",
			Columns: []Column{
				{
					log:       logger,
					name:      "id",
					valueType: TextOID,
				},
			},
		}

		parser := NewBinaryParser(logger, binary.BigEndian)

		originMsg := []byte{byte(OriginMsgType), 102, 111, 114, 101, 105, 103, 110, 95, 100, 98, 0} // "foreign_db\0"
		err := parser.ParseWalMessage(originMsg, tx)
		assert.NoError(t, err)
		assert.Equal(t, "foreign_db", tx.origin)

		insertMsg := []byte{
			byte(InsertMsgType),
			0, 0, 0, 1, // relation id
			78,   // N flag
			0, 1, // 1 column
			116,        // text type
			0, 0, 0, 4, // 4 bytes
			116, 101, 115, 116, // "test"
		}
		err = parser.ParseWalMessage(insertMsg, tx)
		assert.NoError(t, err)

		assert.Equal(t, 0, len(tx.Actions))

		events := tx.CreateEventsWithFilter(context.Background())
		assert.Equal(t, 0, len(events))
	})

	t.Run("full pipeline with local transaction", func(t *testing.T) {
		tx := NewWalTransaction(logger, pool, new(monitorMock), nil, config.ExcludeStruct{}, map[string]string{}, true)
		
		tx.RelationStore[1] = RelationData{
			Schema: "public",
			Table:  "test_table",
			Columns: []Column{
				{
					log:       logger,
					name:      "id",
					valueType: TextOID,
				},
			},
		}

		parser := NewBinaryParser(logger, binary.BigEndian)

		insertMsg := []byte{
			byte(InsertMsgType),
			0, 0, 0, 1, // relation id
			78,   // N flag
			0, 1, // 1 column
			116,        // text type
			0, 0, 0, 4, // 4 bytes
			116, 101, 115, 116, // "test"
		}
		err := parser.ParseWalMessage(insertMsg, tx)
		assert.NoError(t, err)

		assert.Equal(t, 1, len(tx.Actions))

		events := tx.CreateEventsWithFilter(context.Background())
		assert.Equal(t, 1, len(events))
	})

	t.Run("mixed transaction scenario", func(t *testing.T) {
		tx := NewWalTransaction(logger, pool, new(monitorMock), nil, config.ExcludeStruct{}, map[string]string{}, true)
		
		tx.RelationStore[1] = RelationData{
			Schema: "public",
			Table:  "test_table",
			Columns: []Column{
				{
					log:       logger,
					name:      "id",
					valueType: TextOID,
				},
			},
		}

		parser := NewBinaryParser(logger, binary.BigEndian)

		originMsg := []byte{byte(OriginMsgType), 114, 101, 112, 108, 105, 99, 97, 0} // "replica\0"
		err := parser.ParseWalMessage(originMsg, tx)
		assert.NoError(t, err)

		insertMsg := []byte{
			byte(InsertMsgType),
			0, 0, 0, 1, // relation id
			78,   // N flag
			0, 1, // 1 column
			116,        // text type
			0, 0, 0, 4, // 4 bytes
			116, 101, 115, 116, // "test"
		}
		err = parser.ParseWalMessage(insertMsg, tx)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(tx.Actions))

		tx.Clear()

		insertMsg2 := []byte{
			byte(InsertMsgType),
			0, 0, 0, 1, // relation id
			78,   // N flag
			0, 1, // 1 column
			116,        // text type
			0, 0, 0, 5, // 5 bytes
			108, 111, 99, 97, 108, // "local"
		}
		err = parser.ParseWalMessage(insertMsg2, tx)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(tx.Actions))

		events := tx.CreateEventsWithFilter(context.Background())
		assert.Equal(t, 1, len(events))
	})

	t.Run("skip transaction buffering simulation", func(t *testing.T) {
		
		tx := NewWalTransaction(logger, pool, new(monitorMock), nil, config.ExcludeStruct{}, map[string]string{}, true)
		
		tx.RelationStore[1] = RelationData{
			Schema: "public",
			Table:  "test_table",
			Columns: []Column{
				{
					log:       logger,
					name:      "id",
					valueType: TextOID,
				},
			},
		}

		parser := NewBinaryParser(logger, binary.BigEndian)

		originMsg := []byte{byte(OriginMsgType), 114, 101, 109, 111, 116, 101, 0} // "remote\0"
		err := parser.ParseWalMessage(originMsg, tx)
		assert.NoError(t, err)
		assert.Equal(t, "remote", tx.origin)

		insertMsg := []byte{
			byte(InsertMsgType),
			0, 0, 0, 1, // relation id
			78,   // N flag
			0, 1, // 1 column
			116,        // text type
			0, 0, 0, 4, // 4 bytes
			116, 101, 115, 116, // "test"
		}
		err = parser.ParseWalMessage(insertMsg, tx)
		assert.NoError(t, err)

		events := tx.CreateEventsWithFilter(context.Background())
		assert.Equal(t, 0, len(events)) // Should be empty due to filtering

		tx.ClearActions()

		insertMsg2 := []byte{
			byte(InsertMsgType),
			0, 0, 0, 1, // relation id
			78,   // N flag
			0, 1, // 1 column
			116,        // text type
			0, 0, 0, 5, // 5 bytes
			116, 101, 115, 116, 50, // "test2"
		}
		err = parser.ParseWalMessage(insertMsg2, tx)
		assert.NoError(t, err)

		events = tx.CreateEventsWithFilter(context.Background())
		assert.Equal(t, 0, len(events)) // Should still be empty due to filtering
	})
}
