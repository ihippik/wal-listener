package transaction

import (
	"log/slog"
	"strconv"
	"time"

	"github.com/goccy/go-json"
	"github.com/google/uuid"
)

// ActionKind kind of action on WAL message.
type ActionKind string

// kind of WAL message.
const (
	ActionKindInsert ActionKind = "INSERT"
	ActionKindUpdate ActionKind = "UPDATE"
	ActionKindDelete ActionKind = "DELETE"
)

func (k ActionKind) string() string {
	return string(k)
}

// RelationData kind of WAL message data.
type RelationData struct {
	Schema  string
	Table   string
	Columns []Column
}

// ActionData kind of WAL message data.
type ActionData struct {
	Schema     string
	Table      string
	Kind       ActionKind
	OldColumns []Column
	NewColumns []Column
}

// Column of the table with which changes occur.
type Column struct {
	log       *slog.Logger
	name      string
	value     any
	valueType int
	isKey     bool
}

// InitColumn create new Column instance with data.s
func InitColumn(log *slog.Logger, name string, value any, valueType int, isKey bool) Column {
	return Column{log: log, name: name, value: value, valueType: valueType, isKey: isKey}
}

// AssertValue converts bytes to a specific type depending
// on the type of this data in the database table.
func (c *Column) AssertValue(src []byte) {
	var (
		val any
		err error
	)

	if src == nil {
		c.value = nil
		return
	}

	strSrc := string(src)

	const (
		timestampLayout       = "2006-01-02 15:04:05"
		timestampWithTZLayout = "2006-01-02 15:04:05.999999999-07"
	)

	switch c.valueType {
	case BoolOID:
		val, err = strconv.ParseBool(strSrc)
	case Int2OID, Int4OID:
		val, err = strconv.Atoi(strSrc)
	case Int8OID:
		val, err = strconv.ParseInt(strSrc, 10, 64)
	case TextOID, VarcharOID:
		val = strSrc
	case TimestampOID:
		val, err = time.Parse(timestampLayout, strSrc)
	case TimestamptzOID:
		val, err = time.ParseInLocation(timestampWithTZLayout, strSrc, time.UTC)
	case DateOID, TimeOID:
		val = strSrc
	case UUIDOID:
		val, err = uuid.Parse(strSrc)
	case JSONBOID:
		var m any

		if src[0] == '[' {
			m = make([]any, 0)
		} else {
			m = make(map[string]any)
		}

		err = json.Unmarshal(src, &m)
		val = m
	default:
		c.log.Debug(
			"unknown oid type",
			slog.Int("pg_type", c.valueType),
			slog.String("column_name", c.name),
		)

		val = strSrc
	}

	if err != nil {
		c.log.Error(
			"column data parse error",
			slog.String("err", err.Error()),
			slog.Int("pg_type", c.valueType),
			slog.String("column_name", c.name),
		)
	}

	c.value = val
}
