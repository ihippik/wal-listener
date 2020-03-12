package listener

import (
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/pgtype"
	"github.com/magiconair/properties/assert"
)

func TestWalTransaction_CreateActionData(t *testing.T) {
	type fields struct {
		LSN           int64
		BeginTime     *time.Time
		CommitTime    *time.Time
		RelationStore map[int32]RelationData
		Actions       []ActionData
	}
	type args struct {
		relationID int32
		rows       []TupleData
		kind       ActionKind
	}
	now := time.Now()
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantA   ActionData
		wantErr bool
	}{
		{
			name: "success",
			fields: fields{
				LSN:        10,
				BeginTime:  &now,
				CommitTime: &now,
				RelationStore: map[int32]RelationData{
					10: {
						Schema: "public",
						Table:  "users",
						Columns: []Column{
							{
								name:      "id",
								value:     5,
								valueType: pgtype.Int4OID,
								isKey:     true,
							},
						},
					},
				},
				Actions: nil,
			},
			args: args{
				relationID: 10,
				rows: []TupleData{
					{
						Value: []byte{49, 49},
					},
				},
				kind: ActionKindUpdate,
			},
			wantA: ActionData{
				Schema: "public",
				Table:  "users",
				Kind:   ActionKindUpdate,
				Columns: []Column{
					{
						name:      "id",
						value:     11,
						valueType: pgtype.Int4OID,
						isKey:     true,
					},
				},
			},
			wantErr: false,
		},
		{
			name: "relation not exists",
			fields: fields{
				LSN:        10,
				BeginTime:  &now,
				CommitTime: &now,
				RelationStore: map[int32]RelationData{
					11: {
						Schema: "public",
						Table:  "users",
						Columns: []Column{
							{
								name:      "id",
								value:     5,
								valueType: pgtype.Int4OID,
								isKey:     true,
							},
						},
					},
				},
				Actions: nil,
			},
			args: args{
				relationID: 10,
				rows:       nil,
				kind:       ActionKindUpdate,
			},
			wantA:   ActionData{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := WalTransaction{
				LSN:           tt.fields.LSN,
				BeginTime:     tt.fields.BeginTime,
				CommitTime:    tt.fields.CommitTime,
				RelationStore: tt.fields.RelationStore,
				Actions:       tt.fields.Actions,
			}
			gotA, err := w.CreateActionData(tt.args.relationID, tt.args.rows, tt.args.kind)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateActionData() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotA, tt.wantA) {
				t.Errorf("CreateActionData() gotA = %v, want %v", gotA, tt.wantA)
			}
		})
	}
}

func TestColumn_AssertValue(t *testing.T) {
	type fields struct {
		name      string
		valueType int
		isKey     bool
	}
	type args struct {
		src []byte
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Column
	}{
		{
			name: "bool",
			fields: fields{
				name:      "isBool",
				valueType: pgtype.BoolOID,
				isKey:     false,
			},
			args: args{
				src: []byte{116},
			},
			want: &Column{
				name:      "isBool",
				value:     true,
				valueType: 16,
				isKey:     false,
			},
		},
		{
			name: "text",
			fields: fields{
				name:      "name",
				valueType: pgtype.TextOID,
				isKey:     false,
			},
			args: args{
				src: []byte{104, 101, 108, 108, 111},
			},
			want: &Column{
				name:      "name",
				value:     "hello",
				valueType: 25,
				isKey:     false,
			},
		},
		{
			name: "timestamp",
			fields: fields{
				name:      "created",
				valueType: pgtype.TimestampOID,
				isKey:     false,
			},
			args: args{
				src: []byte{50, 48, 50, 48, 45, 49, 48, 45, 49, 50},
			},
			want: &Column{
				name:      "created",
				value:     "2020-10-12",
				valueType: 1114,
				isKey:     false,
			},
		},
		{
			name: "unknown",
			fields: fields{
				name:      "created",
				valueType: pgtype.Float4ArrayOID,
				isKey:     false,
			},
			args: args{
				src: []byte{50, 48, 50, 48, 45, 49, 48, 45, 49, 50},
			},
			want: &Column{
				name:      "created",
				value:     "2020-10-12",
				valueType: 1021,
				isKey:     false,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Column{
				name:      tt.fields.name,
				valueType: tt.fields.valueType,
				isKey:     tt.fields.isKey,
			}
			c.AssertValue(tt.args.src)
			assert.Equal(t, c, tt.want)
		})
	}
}
