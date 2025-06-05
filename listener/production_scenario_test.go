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

func TestProductionScenarios(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	
	pool := &sync.Pool{
		New: func() interface{} {
			return &publisher.Event{}
		},
	}

	t.Run("high volume logical replication with foreign origins", func(t *testing.T) {
		tx := NewWalTransaction(logger, pool, new(monitorMock), nil, config.ExcludeStruct{}, map[string]string{}, true)
		
		tx.RelationStore[1] = RelationData{
			Schema: "public",
			Table:  "users",
			Columns: []Column{
				{log: logger, name: "id", valueType: TextOID},
				{log: logger, name: "name", valueType: TextOID},
			},
		}

		parser := NewBinaryParser(logger, binary.BigEndian)

		originMsg := []byte{byte(OriginMsgType), 114, 101, 112, 108, 105, 99, 97, 95, 100, 98, 0} // "replica_db\0"
		err := parser.ParseWalMessage(originMsg, tx)
		assert.NoError(t, err)
		assert.Equal(t, "replica_db", tx.origin)

		for i := 0; i < 1000; i++ {
			insertMsg := []byte{
				byte(InsertMsgType),
				0, 0, 0, 1, // relation id
				78,   // N flag
				0, 2, // 2 columns
				116,        // text type
				0, 0, 0, 4, // 4 bytes
				116, 101, 115, 116, // "test"
				116,        // text type
				0, 0, 0, 4, // 4 bytes
				117, 115, 101, 114, // "user"
			}
			err = parser.ParseWalMessage(insertMsg, tx)
			assert.NoError(t, err)
		}

		assert.Equal(t, 0, len(tx.Actions))

		events := tx.CreateEventsWithFilter(context.Background())
		assert.Equal(t, 0, len(events))
	})

	t.Run("cascaded replication setup", func(t *testing.T) {
		tx := NewWalTransaction(logger, pool, new(monitorMock), nil, config.ExcludeStruct{}, map[string]string{}, true)
		
		tx.RelationStore[1] = RelationData{
			Schema: "public",
			Table:  "orders",
			Columns: []Column{
				{log: logger, name: "id", valueType: TextOID},
			},
		}

		parser := NewBinaryParser(logger, binary.BigEndian)

		originMsg := []byte{byte(OriginMsgType), 117, 112, 115, 116, 114, 101, 97, 109, 95, 100, 98, 0} // "upstream_db\0"
		err := parser.ParseWalMessage(originMsg, tx)
		assert.NoError(t, err)

		insertMsg := []byte{
			byte(InsertMsgType),
			0, 0, 0, 1, // relation id
			78,   // N flag
			0, 1, // 1 column
			116,        // text type
			0, 0, 0, 5, // 5 bytes
			111, 114, 100, 101, 114, // "order"
		}
		err = parser.ParseWalMessage(insertMsg, tx)
		assert.NoError(t, err)

		assert.Equal(t, 0, len(tx.Actions))

		events := tx.CreateEventsWithFilter(context.Background())
		assert.Equal(t, 0, len(events))
	})

	t.Run("mixed local and replicated transactions", func(t *testing.T) {
		tx := NewWalTransaction(logger, pool, new(monitorMock), nil, config.ExcludeStruct{}, map[string]string{}, true)
		
		tx.RelationStore[1] = RelationData{
			Schema: "public",
			Table:  "products",
			Columns: []Column{
				{log: logger, name: "id", valueType: TextOID},
			},
		}

		parser := NewBinaryParser(logger, binary.BigEndian)

		originMsg := []byte{byte(OriginMsgType), 114, 101, 109, 111, 116, 101, 0} // "remote\0"
		err := parser.ParseWalMessage(originMsg, tx)
		assert.NoError(t, err)

		insertMsg1 := []byte{
			byte(InsertMsgType),
			0, 0, 0, 1, // relation id
			78,   // N flag
			0, 1, // 1 column
			116,        // text type
			0, 0, 0, 8, // 8 bytes
			114, 101, 109, 111, 116, 101, 95, 49, // "remote_1"
		}
		err = parser.ParseWalMessage(insertMsg1, tx)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(tx.Actions))

		tx.Clear()

		insertMsg2 := []byte{
			byte(InsertMsgType),
			0, 0, 0, 1, // relation id
			78,   // N flag
			0, 1, // 1 column
			116,        // text type
			0, 0, 0, 7, // 7 bytes
			108, 111, 99, 97, 108, 95, 49, // "local_1"
		}
		err = parser.ParseWalMessage(insertMsg2, tx)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(tx.Actions))

		events := tx.CreateEventsWithFilter(context.Background())
		assert.Equal(t, 1, len(events))
	})

	t.Run("configuration disabled in production", func(t *testing.T) {
		tx := NewWalTransaction(logger, pool, new(monitorMock), nil, config.ExcludeStruct{}, map[string]string{}, false)
		
		tx.RelationStore[1] = RelationData{
			Schema: "public",
			Table:  "test_table",
			Columns: []Column{
				{log: logger, name: "id", valueType: TextOID},
			},
		}

		parser := NewBinaryParser(logger, binary.BigEndian)

		originMsg := []byte{byte(OriginMsgType), 102, 111, 114, 101, 105, 103, 110, 0} // "foreign\0"
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

		assert.Equal(t, 1, len(tx.Actions))

		events := tx.CreateEventsWithFilter(context.Background())
		assert.Equal(t, 1, len(events))
	})

	t.Run("transaction without origin message when filtering enabled", func(t *testing.T) {
		tx := NewWalTransaction(logger, pool, new(monitorMock), nil, config.ExcludeStruct{}, map[string]string{}, true)
		
		tx.RelationStore[1] = RelationData{
			Schema: "public",
			Table:  "local_table",
			Columns: []Column{
				{log: logger, name: "id", valueType: TextOID},
			},
		}

		parser := NewBinaryParser(logger, binary.BigEndian)

		insertMsg := []byte{
			byte(InsertMsgType),
			0, 0, 0, 1, // relation id
			78,   // N flag
			0, 1, // 1 column
			116,        // text type
			0, 0, 0, 5, // 5 bytes
			108, 111, 99, 97, 108, // "local"
		}
		err := parser.ParseWalMessage(insertMsg, tx)
		assert.NoError(t, err)

		assert.Equal(t, 1, len(tx.Actions))

		events := tx.CreateEventsWithFilter(context.Background())
		assert.Equal(t, 1, len(events))
	})

	t.Run("skip transaction buffering with foreign origin", func(t *testing.T) {
		tx := NewWalTransaction(logger, pool, new(monitorMock), nil, config.ExcludeStruct{}, map[string]string{}, true)
		
		tx.RelationStore[1] = RelationData{
			Schema: "public",
			Table:  "buffered_table",
			Columns: []Column{
				{log: logger, name: "id", valueType: TextOID},
			},
		}

		parser := NewBinaryParser(logger, binary.BigEndian)

		originMsg := []byte{byte(OriginMsgType), 98, 117, 102, 102, 101, 114, 101, 100, 0} // "buffered\0"
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

		events := tx.CreateEventsWithFilter(context.Background())
		assert.Equal(t, 0, len(events))

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
		assert.Equal(t, 0, len(events))
	})
}
