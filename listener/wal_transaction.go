package listener

import (
	"fmt"
	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"strconv"
	"strings"
	"time"
)

// ActionKind kind of action on WAL message.
type ActionKind string

// kind of WAL message.
const (
	ActionKindInsert ActionKind = "INSERT"
	ActionKindUpdate ActionKind = "UPDATE"
	ActionKindDelete ActionKind = "DELETE"
)

// WalTransaction transaction specified WAL message.
type WalTransaction struct {
	LSN           int64
	BeginTime     *time.Time
	CommitTime    *time.Time
	RelationStore map[int32]RelationData
	Actions       []ActionData
}

// NewWalTransaction create and initialize new WAL transaction.
func NewWalTransaction() *WalTransaction {
	return &WalTransaction{
		RelationStore: make(map[int32]RelationData),
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
		logrus.WithFields(logrus.Fields{"pgtype": c.valueType, "column_name": c.name}).Warnln("unknown oid type")
		val = strSrc
	}

	if err != nil {
		logrus.WithError(err).WithFields(logrus.Fields{"pgtype": c.valueType, "column_name": c.name}).
			Errorln("column data parse error")
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
func (w *WalTransaction) CreateActionData(relationID int32, oldRows []TupleData, newRows []TupleData, kind ActionKind) (a ActionData, err error) {
	rel, ok := w.RelationStore[relationID]
	if !ok {
		return a, errRelationNotFound
	}

	a = ActionData{
		Schema: rel.Schema,
		Table:  rel.Table,
		Kind:   kind,
	}

	var oldColumns []Column

	for num, row := range oldRows {
		column := Column{
			name:      rel.Columns[num].name,
			valueType: rel.Columns[num].valueType,
			isKey:     rel.Columns[num].isKey,
		}
		column.AssertValue(row.Value)
		oldColumns = append(oldColumns, column)
	}

	a.OldColumns = oldColumns

	var newColumns []Column
	for num, row := range newRows {
		column := Column{
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
func (w *WalTransaction) CreateEventsWithFilter(tableMap map[string][]string) []Event {
	var events []Event

	for _, item := range w.Actions {
		dataOld := make(map[string]any)
		for _, val := range item.OldColumns {
			dataOld[val.name] = val.value
		}

		data := make(map[string]any)
		for _, val := range item.NewColumns {
			data[val.name] = val.value
		}

		event := Event{
			ID:        uuid.New(),
			Schema:    item.Schema,
			Table:     item.Table,
			Action:    item.Kind.string(),
			DataOld:   dataOld,
			Data:      data,
			EventTime: *w.CommitTime,
		}

		actions, validTable := tableMap[item.Table]

		validAction := inArray(actions, item.Kind.string())
		if validTable && validAction {
			events = append(events, event)
			continue
		}

		filterSkippedEvents.With(prometheus.Labels{"table": item.Table}).Inc()
		bs, _ := json.Marshal(event.Data)
		fmt.Println(string(bs))

		logrus.WithFields(
			logrus.Fields{
				"schema": item.Schema,
				"table":  item.Table,
				"action": item.Kind,
			}).
			Infoln("wal-message was skipped by filter")
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
