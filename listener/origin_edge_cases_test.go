package listener

import (
	"encoding/binary"
	"io"
	"log/slog"
	"testing"

	"github.com/ihippik/wal-listener/v2/config"
	"github.com/stretchr/testify/assert"
)

func TestOriginFilteringEdgeCases(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	metrics := new(monitorMock)

	t.Run("origin message with empty string", func(t *testing.T) {
		tx := NewWalTransaction(logger, nil, metrics, nil, config.ExcludeStruct{}, map[string]string{}, true)
		tx.RelationStore[1] = RelationData{
			Schema: "public",
			Table:  "test_table",
			Columns: []Column{{log: logger, name: "id", valueType: TextOID}},
		}

		parser := NewBinaryParser(logger, binary.BigEndian)

		originMsg := []byte{byte(OriginMsgType), 0} // Empty string with null terminator
		err := parser.ParseWalMessage(originMsg, tx)
		assert.NoError(t, err)
		assert.Equal(t, "", tx.origin)

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
	})

	t.Run("multiple origin messages in same transaction", func(t *testing.T) {
		tx := NewWalTransaction(logger, nil, metrics, nil, config.ExcludeStruct{}, map[string]string{}, true)
		tx.RelationStore[1] = RelationData{
			Schema: "public",
			Table:  "test_table",
			Columns: []Column{{log: logger, name: "id", valueType: TextOID}},
		}

		parser := NewBinaryParser(logger, binary.BigEndian)

		originMsg1 := []byte{byte(OriginMsgType), 111, 114, 105, 103, 105, 110, 49, 0} // "origin1\0"
		err := parser.ParseWalMessage(originMsg1, tx)
		assert.NoError(t, err)
		assert.Equal(t, "origin1", tx.origin)

		originMsg2 := []byte{byte(OriginMsgType), 111, 114, 105, 103, 105, 110, 50, 0} // "origin2\0"
		err = parser.ParseWalMessage(originMsg2, tx)
		assert.NoError(t, err)
		assert.Equal(t, "origin2", tx.origin)

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
		assert.Equal(t, 0, len(tx.Actions)) // Should be dropped
	})

	t.Run("very long origin name", func(t *testing.T) {
		tx := NewWalTransaction(logger, nil, metrics, nil, config.ExcludeStruct{}, map[string]string{}, true)
		tx.RelationStore[1] = RelationData{
			Schema: "public",
			Table:  "test_table",
			Columns: []Column{{log: logger, name: "id", valueType: TextOID}},
		}

		parser := NewBinaryParser(logger, binary.BigEndian)

		longOrigin := "very_long_origin_name_that_might_cause_issues_in_some_systems_if_not_handled_properly"
		originBytes := append([]byte{byte(OriginMsgType)}, append([]byte(longOrigin), 0)...)
		
		err := parser.ParseWalMessage(originBytes, tx)
		assert.NoError(t, err)
		assert.Equal(t, longOrigin, tx.origin)

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
	})

	t.Run("origin with special characters", func(t *testing.T) {
		tx := NewWalTransaction(logger, nil, metrics, nil, config.ExcludeStruct{}, map[string]string{}, true)
		tx.RelationStore[1] = RelationData{
			Schema: "public",
			Table:  "test_table",
			Columns: []Column{{log: logger, name: "id", valueType: TextOID}},
		}

		parser := NewBinaryParser(logger, binary.BigEndian)

		specialOrigin := "origin-with_special.chars@domain"
		originBytes := append([]byte{byte(OriginMsgType)}, append([]byte(specialOrigin), 0)...)
		
		err := parser.ParseWalMessage(originBytes, tx)
		assert.NoError(t, err)
		assert.Equal(t, specialOrigin, tx.origin)

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
	})

	t.Run("transaction state after commit with origin", func(t *testing.T) {
		tx := NewWalTransaction(logger, nil, metrics, nil, config.ExcludeStruct{}, map[string]string{}, true)
		
		tx.SetOrigin("foreign_origin", true)
		assert.Equal(t, true, tx.ShouldDropMessage())

		tx.Clear()
		
		assert.Equal(t, "", tx.origin)
		assert.Equal(t, false, tx.ShouldDropMessage())
	})

	t.Run("dropForeignOrigin configuration changes", func(t *testing.T) {
		tx := NewWalTransaction(logger, nil, metrics, nil, config.ExcludeStruct{}, map[string]string{}, false)
		
		tx.SetOrigin("foreign_origin", false)
		assert.Equal(t, false, tx.ShouldDropMessage())

		tx.SetOrigin("foreign_origin", true)
		assert.Equal(t, true, tx.ShouldDropMessage())

		tx.SetOrigin("foreign_origin", false)
		assert.Equal(t, false, tx.ShouldDropMessage())
	})
}
