package listener

import (
	"errors"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

type ActionKind string

// kind of wall message.
const (
	ActionKindInsert ActionKind = "INSERT"
	ActionKindUpdate ActionKind = "UPDATE"
	ActionKindDelete ActionKind = "DELETE"
)

type WalTransaction struct {
	LSN           int64
	BeginTime     time.Time
	CommitTime    time.Time
	RelationStore map[int32]RelationData
	Actions       []ActionData
}

func NewWalTransaction() *WalTransaction {
	return &WalTransaction{
		RelationStore: make(map[int32]RelationData),
	}
}

func (k ActionKind) string() string {
	return string(k)
}

type RelationData struct {
	Schema  string
	Table   string
	Columns []Column
}

type ActionData struct {
	Schema  string
	Table   string
	Kind    ActionKind
	Columns []Column
}

type Column struct {
	Name  string
	Value interface{}
	Type  string
	IsKey bool
}

func (w WalTransaction) CreateActionData(
	relationID int32,
	rows []TupleData,
	kind ActionKind,
) (a ActionData, err error) {
	rel, ok := w.RelationStore[relationID]
	if !ok {
		return a, errors.New("relation not found")
	}
	a = ActionData{
		Schema: rel.Schema,
		Table:  rel.Table,
		Kind:   kind,
	}
	var columns []Column
	for num, row := range rows {
		columns = append(columns, Column{
			Name: rel.Columns[num].Name,
			// TODO type assert!
			Value: string(row.Value),
			Type:  rel.Columns[num].Type,
			IsKey: rel.Columns[num].IsKey,
		})
	}
	a.Columns = columns
	return a, nil
}

// CreateEventsWithFilter filter wal message by table, action and create events for each value.
func (w *WalTransaction) CreateEventsWithFilter(
	tableMap map[string][]string) []Event {
	var events []Event

	for _, item := range w.Actions {
		data := make(map[string]interface{})
		for _, val := range item.Columns {
			data[val.Name] = val.Value
		}
		event := Event{
			Schema: item.Schema,
			Table:  item.Table,
			Action: item.Kind.string(),
			Data:   data,
		}

		actions, validTable := tableMap[item.Table]
		validAction := inArray(actions, item.Kind.string())
		if validTable && validAction {
			events = append(events, event)
		} else {
			logrus.WithFields(
				logrus.Fields{
					"schema": item.Schema,
					"table":  item.Table,
					"action": item.Kind,
				}).
				Infoln("wal message skip by filter")
		}
	}
	return events
}

// inArray checks whether the value is in an array
func inArray(arr []string, value string) bool {
	for _, v := range arr {
		if strings.ToLower(v) == strings.ToLower(value) {
			return true
		}
	}
	return false
}
