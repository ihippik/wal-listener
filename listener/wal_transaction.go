package listener

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-json"
	"github.com/google/uuid"

	"github.com/ihippik/wal-listener/v2/publisher"
)

// ActionKind kind of action on WAL message.
type ActionKind string

// kind of WAL message.
const (
	ActionKindInsert ActionKind = "INSERT"
	ActionKindUpdate ActionKind = "UPDATE"
	ActionKindDelete ActionKind = "DELETE"
)

type transactionMonitor interface {
	IncFilterSkippedEvents(table string)
}

// WalTransaction transaction specified WAL message.
type WalTransaction struct {
	log           *slog.Logger
	monitor       transactionMonitor
	LSN           int64
	BeginTime     *time.Time
	CommitTime    *time.Time
	RelationStore map[int32]RelationData
	Actions       []ActionData
	pool          *sync.Pool
}

// NewWalTransaction create and initialize new WAL transaction.
func NewWalTransaction(log *slog.Logger, pool *sync.Pool, monitor transactionMonitor) *WalTransaction {
	const aproxData = 300

	return &WalTransaction{
		pool:          pool,
		log:           log,
		monitor:       monitor,
		RelationStore: make(map[int32]RelationData),
		Actions:       make([]ActionData, 0, aproxData),
	}
}

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
		c.log.Warn(
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

// Clear transaction data.
func (w *WalTransaction) Clear() {
	w.CommitTime = nil
	w.BeginTime = nil
	w.Actions = nil
}

// CreateActionData create action  from WAL message data.
func (w *WalTransaction) CreateActionData(
	relationID int32,
	oldRows []TupleData,
	newRows []TupleData,
	kind ActionKind,
) (a ActionData, err error) {
	rel, ok := w.RelationStore[relationID]
	if !ok {
		return a, errRelationNotFound
	}

	a = ActionData{
		Schema: rel.Schema,
		Table:  rel.Table,
		Kind:   kind,
	}

	oldColumns := make([]Column, 0, len(oldRows))

	for num, row := range oldRows {
		column := Column{
			log:       w.log,
			name:      rel.Columns[num].name,
			valueType: rel.Columns[num].valueType,
			isKey:     rel.Columns[num].isKey,
		}

		column.AssertValue(row.Value)
		oldColumns = append(oldColumns, column)
	}

	a.OldColumns = oldColumns

	newColumns := make([]Column, 0, len(newRows))

	for num, row := range newRows {
		column := Column{
			log:       w.log,
			name:      rel.Columns[num].name,
			valueType: rel.Columns[num].valueType,
			isKey:     rel.Columns[num].isKey,
		}
		column.AssertValue(row.Value)
		newColumns = append(newColumns, column)
	}

	a.NewColumns = newColumns

	return a, nil
}

// CreateEventsWithFilter filter WAL message by table,
// action and create events for each value.
func (w *WalTransaction) CreateEventsWithFilter(ctx context.Context, tableMap map[string][]string) []*publisher.Event {
	var events []*publisher.Event

	for _, item := range w.Actions {
		if err := ctx.Err(); err != nil {
			w.log.Debug("create events with filter: context canceled")
			break
		}

		dataOld := make(map[string]any, len(item.OldColumns))

		for _, val := range item.OldColumns {
			dataOld[val.name] = val.value
		}

		data := make(map[string]any, len(item.NewColumns))

		for _, val := range item.NewColumns {
			data[val.name] = val.value
		}

		event := w.pool.Get().(*publisher.Event)
		event.ID = uuid.New()
		event.Schema = item.Schema
		event.Table = item.Table
		event.Action = item.Kind.string()
		event.Data = data
		event.DataOld = dataOld
		event.EventTime = *w.CommitTime

		actions, validTable := tableMap[item.Table]

		validAction := inArray(actions, item.Kind.string())
		if validTable && validAction {
			events = append(events, event)
			continue
		}

		w.monitor.IncFilterSkippedEvents(item.Table)

		w.log.Debug(
			"wal-message was skipped by filter",
			slog.String("schema", item.Schema),
			slog.String("table", item.Table),
			slog.String("action", string(item.Kind)),
		)
	}

	return events
}

// inArray checks whether the value is in an array.
func inArray(arr []string, value string) bool {
	for _, v := range arr {
		if strings.EqualFold(v, value) {
			return true
		}
	}

	return false
}
