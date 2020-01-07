package listener

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestWalEvent_Validate(t *testing.T) {
	type fields struct {
		NextLSN string
		Change  []ChangeItem
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr error
	}{
		{
			name: "success",
			fields: fields{
				NextLSN: "12345",
				Change: []ChangeItem{
					{
						ColumnNames:  []string{"first", "second"},
						ColumnValues: []interface{}{"first", "second"},
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "invalid count",
			fields: fields{
				NextLSN: "12345",
				Change: []ChangeItem{
					{
						ColumnNames:  []string{"first"},
						ColumnValues: []interface{}{"first", "second"},
					},
				},
			},
			wantErr: errors.New("not valid WAL message"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &WalEvent{
				NextLSN: tt.fields.NextLSN,
				Change:  tt.fields.Change,
			}
			err := w.Validate()
			if err == nil {
				assert.Nil(t, tt.wantErr)
			} else {
				assert.EqualError(t, tt.wantErr, err.Error())
			}
		})
	}
}

func TestWalEvent_CreateEventsWithFilter(t *testing.T) {
	type fields struct {
		NextLSN string
		Change  []ChangeItem
	}
	type args struct {
		tableMap map[string][]string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []Event
	}{
		{
			name: "success",
			fields: fields{
				NextLSN: "12345",
				Change: []ChangeItem{
					{
						Kind:         "insert",
						Schema:       "public",
						Table:        "users",
						ColumnTypes:  []string{"string", "string"},
						ColumnNames:  []string{"first", "second"},
						ColumnValues: []interface{}{"v1", "v2"},
						OldKeys: struct {
							KeyNames  []string      `json:"keynames"`
							KeyTypes  []string      `json:"keytypes"`
							KeyValues []interface{} `json:"keyvalues"`
						}{
							KeyNames:  nil,
							KeyTypes:  nil,
							KeyValues: nil,
						},
					},
				},
			},
			args: args{
				tableMap: map[string][]string{"users": {"insert", "update"}},
			},
			want: []Event{
				{
					TableName: "users",
					Action:    "insert",
					Data:      map[string]interface{}{"first": "v1", "second": "v2"},
				},
			},
		},
		{
			name: "success with delete",
			fields: fields{
				NextLSN: "12345",
				Change: []ChangeItem{
					{
						Kind:   "delete",
						Schema: "public",
						Table:  "users",
						OldKeys: struct {
							KeyNames  []string      `json:"keynames"`
							KeyTypes  []string      `json:"keytypes"`
							KeyValues []interface{} `json:"keyvalues"`
						}{
							KeyNames:  []string{"id"},
							KeyTypes:  nil,
							KeyValues: []interface{}{1},
						},
					},
				},
			},
			args: args{
				tableMap: map[string][]string{"users": {"insert", "update", "delete"}},
			},
			want: []Event{
				{
					TableName: "users",
					Action:    "delete",
					Data:      map[string]interface{}{"id": 1},
				},
			},
		},
		{
			name: "success",
			fields: fields{
				NextLSN: "12345",
				Change: []ChangeItem{
					{
						Kind:         "insert",
						Schema:       "public",
						Table:        "users",
						ColumnTypes:  []string{"string", "string"},
						ColumnNames:  []string{"first", "second"},
						ColumnValues: []interface{}{"v1", "v2"},
						OldKeys: struct {
							KeyNames  []string      `json:"keynames"`
							KeyTypes  []string      `json:"keytypes"`
							KeyValues []interface{} `json:"keyvalues"`
						}{
							KeyNames:  nil,
							KeyTypes:  nil,
							KeyValues: nil,
						},
					},
				},
			},
			args: args{
				tableMap: map[string][]string{"users": {"insert", "update"}},
			},
			want: []Event{
				{
					TableName: "users",
					Action:    "insert",
					Data:      map[string]interface{}{"first": "v1", "second": "v2"},
				},
			},
		},
		{
			name: "success with two event",
			fields: fields{
				NextLSN: "12345",
				Change: []ChangeItem{
					{
						Kind:         "insert",
						Schema:       "public",
						Table:        "users",
						ColumnTypes:  []string{"string", "string"},
						ColumnNames:  []string{"first", "second"},
						ColumnValues: []interface{}{"v1", "v2"},
						OldKeys: struct {
							KeyNames  []string      `json:"keynames"`
							KeyTypes  []string      `json:"keytypes"`
							KeyValues []interface{} `json:"keyvalues"`
						}{
							KeyNames:  nil,
							KeyTypes:  nil,
							KeyValues: nil,
						},
					},
					{
						Kind:         "update",
						Schema:       "public",
						Table:        "pets",
						ColumnTypes:  []string{"string", "string"},
						ColumnNames:  []string{"k1", "k2"},
						ColumnValues: []interface{}{"v1", "v2"},
						OldKeys: struct {
							KeyNames  []string      `json:"keynames"`
							KeyTypes  []string      `json:"keytypes"`
							KeyValues []interface{} `json:"keyvalues"`
						}{
							KeyNames:  nil,
							KeyTypes:  nil,
							KeyValues: nil,
						},
					},
				},
			},
			args: args{
				tableMap: map[string][]string{
					"users": {"insert", "update"},
					"pets":  {"insert", "update"},
				},
			},
			want: []Event{
				{
					TableName: "users",
					Action:    "insert",
					Data:      map[string]interface{}{"first": "v1", "second": "v2"},
				},
				{
					TableName: "pets",
					Action:    "update",
					Data:      map[string]interface{}{"k1": "v1", "k2": "v2"},
				},
			},
		},
		{
			name: "filtered unknown table",
			fields: fields{
				NextLSN: "12345",
				Change: []ChangeItem{
					{
						Kind:         "insert",
						Schema:       "public",
						Table:        "users",
						ColumnTypes:  []string{"string", "string"},
						ColumnNames:  []string{"first", "second"},
						ColumnValues: []interface{}{"v1", "v2"},
						OldKeys: struct {
							KeyNames  []string      `json:"keynames"`
							KeyTypes  []string      `json:"keytypes"`
							KeyValues []interface{} `json:"keyvalues"`
						}{
							KeyNames:  nil,
							KeyTypes:  nil,
							KeyValues: nil,
						},
					},
					{
						Kind:         "insert",
						Schema:       "public",
						Table:        "pets",
						ColumnTypes:  []string{"string", "string"},
						ColumnNames:  []string{"first", "second"},
						ColumnValues: []interface{}{"v1", "v2"},
						OldKeys: struct {
							KeyNames  []string      `json:"keynames"`
							KeyTypes  []string      `json:"keytypes"`
							KeyValues []interface{} `json:"keyvalues"`
						}{
							KeyNames:  nil,
							KeyTypes:  nil,
							KeyValues: nil,
						},
					},
				},
			},
			args: args{
				tableMap: map[string][]string{"users": {"insert", "update"}},
			},
			want: []Event{
				{
					TableName: "users",
					Action:    "insert",
					Data:      map[string]interface{}{"first": "v1", "second": "v2"},
				},
			},
		},
		{
			name: "success filtered unknown action",
			fields: fields{
				NextLSN: "12345",
				Change: []ChangeItem{
					{
						Kind:         "insert",
						Schema:       "public",
						Table:        "users",
						ColumnTypes:  []string{"string", "string"},
						ColumnNames:  []string{"first", "second"},
						ColumnValues: []interface{}{"v1", "v2"},
						OldKeys: struct {
							KeyNames  []string      `json:"keynames"`
							KeyTypes  []string      `json:"keytypes"`
							KeyValues []interface{} `json:"keyvalues"`
						}{
							KeyNames:  nil,
							KeyTypes:  nil,
							KeyValues: nil,
						},
					},
					{
						Kind:         "delete",
						Schema:       "public",
						Table:        "users",
						ColumnTypes:  []string{"integer"},
						ColumnNames:  []string{"id"},
						ColumnValues: []interface{}{1},
						OldKeys: struct {
							KeyNames  []string      `json:"keynames"`
							KeyTypes  []string      `json:"keytypes"`
							KeyValues []interface{} `json:"keyvalues"`
						}{
							KeyNames:  nil,
							KeyTypes:  nil,
							KeyValues: nil,
						},
					},
				},
			},
			args: args{
				tableMap: map[string][]string{"users": {"insert", "update"}},
			},
			want: []Event{
				{
					TableName: "users",
					Action:    "insert",
					Data:      map[string]interface{}{"first": "v1", "second": "v2"},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &WalEvent{
				NextLSN: tt.fields.NextLSN,
				Change:  tt.fields.Change,
			}
			if got := w.CreateEventsWithFilter(tt.args.tableMap); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CreateEventsWithFilter() = %v, want %v", got, tt.want)
			}
		})
	}
}
