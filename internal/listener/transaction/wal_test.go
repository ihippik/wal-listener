package transaction

import (
	"io"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

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
		oldRows    []TupleData
		newRows    []TupleData
		kind       ActionKind
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
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
								log:       logger,
								name:      "id",
								value:     5,
								valueType: Int4OID,
								isKey:     true,
							},
						},
					},
				},
				Actions: nil,
			},
			args: args{
				relationID: 10,
				oldRows: []TupleData{
					{
						Value: []byte{56, 48},
					},
				},
				newRows: []TupleData{
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
				OldColumns: []Column{
					{
						log:       logger,
						name:      "id",
						value:     80,
						valueType: Int4OID,
						isKey:     true,
					},
				},
				NewColumns: []Column{
					{
						log:       logger,
						name:      "id",
						value:     11,
						valueType: Int4OID,
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
								log:       logger,
								name:      "id",
								value:     5,
								valueType: Int4OID,
								isKey:     true,
							},
						},
					},
				},
				Actions: nil,
			},
			args: args{
				relationID: 10,
				oldRows:    nil,
				newRows:    nil,
				kind:       ActionKindUpdate,
			},
			wantA:   ActionData{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := WAL{
				log:           logger,
				LSN:           tt.fields.LSN,
				BeginTime:     tt.fields.BeginTime,
				CommitTime:    tt.fields.CommitTime,
				RelationStore: tt.fields.RelationStore,
				Actions:       tt.fields.Actions,
			}

			gotA, err := w.CreateActionData(tt.args.relationID, tt.args.oldRows, tt.args.newRows, tt.args.kind)
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

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

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
				valueType: BoolOID,
				isKey:     false,
			},
			args: args{
				src: []byte{116},
			},
			want: &Column{
				log:       logger,
				name:      "isBool",
				value:     true,
				valueType: 16,
				isKey:     false,
			},
		},
		{
			name: "int",
			fields: fields{
				name:      "name",
				valueType: Int2OID,
				isKey:     false,
			},
			args: args{
				src: []byte("555"),
			},
			want: &Column{
				log:       logger,
				name:      "name",
				value:     555,
				valueType: 21,
				isKey:     false,
			},
		},
		{
			name: "int8",
			fields: fields{
				name:      "name",
				valueType: Int8OID,
				isKey:     false,
			},
			args: args{
				src: []byte("555"),
			},
			want: &Column{
				log:       logger,
				name:      "name",
				value:     int64(555),
				valueType: 20,
				isKey:     false,
			},
		},
		{
			name: "text",
			fields: fields{
				name:      "name",
				valueType: TextOID,
				isKey:     false,
			},
			args: args{
				src: []byte{104, 101, 108, 108, 111},
			},
			want: &Column{
				log:       logger,
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
				valueType: TimestampOID,
				isKey:     false,
			},
			args: args{
				src: []byte("2022-08-27 17:44:01"),
			},
			want: &Column{
				log:       logger,
				name:      "created",
				value:     time.Date(2022, 8, 27, 17, 44, 1, 0, time.UTC),
				valueType: 1114,
				isKey:     false,
			},
		},
		{
			name: "timestamp with tz",
			fields: fields{
				name:      "created",
				valueType: TimestamptzOID,
				isKey:     false,
			},
			args: args{
				src: []byte("2022-08-27 17:44:58.083316+00"),
			},
			want: &Column{
				log:       logger,
				name:      "created",
				value:     time.Date(2022, 8, 27, 17, 44, 58, 83316000, time.UTC),
				valueType: 1184,
				isKey:     false,
			},
		},
		{
			name: "uuid",
			fields: fields{
				name:      "uuid",
				valueType: UUIDOID,
				isKey:     false,
			},
			args: args{
				src: []byte("600f37ed-1d88-4262-8be4-c3360e833f50"),
			},
			want: &Column{
				log:       logger,
				name:      "uuid",
				value:     uuid.MustParse("600f37ed-1d88-4262-8be4-c3360e833f50"),
				valueType: 2950,
				isKey:     false,
			},
		},
		{
			name: "jsonb",
			fields: fields{
				name:      "jsonb",
				valueType: JSONBOID,
				isKey:     false,
			},
			args: args{
				src: []byte(`{"name":"jsonb"}`),
			},
			want: &Column{
				log:       logger,
				name:      "jsonb",
				value:     map[string]any{"name": "jsonb"},
				valueType: 3802,
				isKey:     false,
			},
		},
		{
			name: "jsonb array string",
			fields: fields{
				name:      "jsonb",
				valueType: JSONBOID,
				isKey:     false,
			},
			args: args{
				src: []byte(`["tag1", "tag2"]`),
			},
			want: &Column{
				log:       logger,
				name:      "jsonb",
				value:     []any{"tag1", "tag2"},
				valueType: 3802,
				isKey:     false,
			},
		},
		{
			name: "jsonb nil",
			fields: fields{
				name:      "jsonb",
				valueType: JSONBOID,
				isKey:     false,
			},
			args: args{
				src: nil,
			},
			want: &Column{
				log:       logger,
				name:      "jsonb",
				value:     nil,
				valueType: 3802,
				isKey:     false,
			},
		},
		{
			name: "jsonb array empty",
			fields: fields{
				name:      "jsonb",
				valueType: JSONBOID,
				isKey:     false,
			},
			args: args{
				src: []byte(`[]`),
			},
			want: &Column{
				log:       logger,
				name:      "jsonb",
				value:     []any{},
				valueType: 3802,
				isKey:     false,
			},
		},
		{
			name: "date",
			fields: fields{
				name:      "date",
				valueType: DateOID,
				isKey:     false,
			},
			args: args{
				src: []byte(`1980-03-19`),
			},
			want: &Column{
				log:       logger,
				name:      "date",
				value:     "1980-03-19",
				valueType: 1082,
				isKey:     false,
			},
		},
		{
			name: "unknown",
			fields: fields{
				name:      "created",
				valueType: 1,
				isKey:     false,
			},
			args: args{
				src: []byte{50, 48, 50, 48, 45, 49, 48, 45, 49, 50},
			},
			want: &Column{
				log:       logger,
				name:      "created",
				value:     "2020-10-12",
				valueType: 1,
				isKey:     false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Column{
				log:       logger,
				name:      tt.fields.name,
				valueType: tt.fields.valueType,
				isKey:     tt.fields.isKey,
			}

			c.AssertValue(tt.args.src)

			assert.Equal(t, c, tt.want)
		})
	}
}
