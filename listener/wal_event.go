package listener

import (
	"errors"

	"github.com/sirupsen/logrus"
)

//go:generate  easyjson wal_event.go

// Constant with kind fo wal message.
const ActionDelete = "delete"

// Error message.
const NotValidMessage = "not valid WAL message"

// WalEvent incoming message structure.
//easyjson:json
type WalEvent struct {
	NextLSN string `json:"nextlsn"`
	Change  []struct {
		Kind         string        `json:"kind"`
		Schema       string        `json:"schema"`
		Table        string        `json:"table"`
		ColumnNames  []string      `json:"columnnames"`
		ColumnTypes  []string      `json:"columntypes"`
		ColumnValues []interface{} `json:"columnvalues"`
		OldKeys      struct {
			KeyNames  []string      `json:"keynames"`
			KeyTypes  []string      `json:"keytypes"`
			KeyValues []interface{} `json:"keyvalues"`
		} `json:"oldkeys"`
	} `json:"change"`
}

// Validate simple message checking for integrity.
func (w *WalEvent) Validate() error {
	for _, val := range w.Change {
		if len(val.ColumnValues) != len(val.ColumnNames) {
			return errors.New(NotValidMessage)
		}
	}
	return nil
}

// CreateEventsWithFilter filter wal message by table, action and create events for each value.
func (w *WalEvent) CreateEventsWithFilter(tableMap map[string][]string) []Event {
	var events []Event

	for _, item := range w.Change {
		data := make(map[string]interface{})
		switch item.Kind {
		case ActionDelete:
			for i, val := range item.OldKeys.KeyNames {
				data[val] = item.OldKeys.KeyValues[i]
			}
		default:
			for i, val := range item.ColumnNames {
				data[val] = item.ColumnValues[i]
			}

		}

		event := Event{TableName: item.Table, Action: item.Kind, Data: data}

		actions, validTable := tableMap[item.Table]
		validAction := inArray(actions, item.Kind)
		if validTable && validAction {
			events = append(events, event)
		} else {
			logrus.WithFields(logrus.Fields{"table": item.Table, "action": item.Kind}).Infoln("wal message skip by filter")
		}
	}
	return events
}

// inArray checks whether the value is in an array
func inArray(arr []string, value string) bool {
	for _, v := range arr {
		if v == value {
			return true
		}
	}
	return false
}
