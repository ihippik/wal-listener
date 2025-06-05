package listener

import (
	"bytes"
	"encoding/binary"
	"io"
	"log/slog"
	"reflect"
	"testing"

	"github.com/ihippik/wal-listener/v2/config"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
)

func TestBinaryParser_readTupleData(t *testing.T) {
	type fields struct {
		buffer *bytes.Buffer
	}

	tests := []struct {
		name   string
		fields fields
		want   []TupleData
	}{
		{
			name: "success",
			fields: fields{
				// 0,1 - 1(int16) BigEndian
				// 116 - t(type, text)
				// 0,0,0,1 - 1(int32) BigEndian
				// 116 - t(value, text)
				buffer: bytes.NewBuffer([]byte{
					0, 1,
					116,
					0, 0, 0, 1,
					116,
				}),
			},
			want: []TupleData{
				{
					Value: []byte{116},
				},
			},
		},
		{
			name: "null value",
			fields: fields{
				buffer: bytes.NewBuffer([]byte{0, 1, 110, 0, 0, 0, 1, 116}),
			},
			want: []TupleData{
				{},
			},
		},
		{
			name: "toast value",
			fields: fields{
				buffer: bytes.NewBuffer([]byte{0, 1, 117, 0, 0, 0, 1, 116}),
			},
			want: []TupleData{
				{IsUnchangedToastedValue: true},
			},
		},
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &BinaryParser{
				log:       logger,
				byteOrder: binary.BigEndian,
				buffer:    tt.fields.buffer,
			}
			if got := p.readTupleData(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("readTupleData() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBinaryParser_readColumns(t *testing.T) {
	type fields struct {
		buffer *bytes.Buffer
	}
	tests := []struct {
		name   string
		fields fields
		want   []RelationColumn
	}{
		{
			name: "success",
			fields: fields{
				// 0,1 - 1(count rows, int16)
				// 1 - 0 (isKey bool,int8)
				// 105,100 - id(field name, text)
				// 0 - end of string
				// 0,0,0,25 - 25(pgtype text, int32)
				// 0,0,0,1 - 1 (modifier, int32)
				buffer: bytes.NewBuffer([]byte{
					0, 1,
					1,
					105, 100, 0,
					0, 0, 0, 25,
					0, 0, 0, 1,
				}),
			},
			want: []RelationColumn{
				{
					Key:          true,
					Name:         "id",
					TypeID:       pgtype.TextOID,
					ModifierType: 1,
				},
			},
		},
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &BinaryParser{
				log:       logger,
				byteOrder: binary.BigEndian,
				buffer:    tt.fields.buffer,
			}
			if got := p.readColumns(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("readColumns() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBinaryParser_getRelationMsg(t *testing.T) {
	type fields struct {
		src []byte
	}
	tests := []struct {
		name   string
		fields fields
		want   Relation
	}{
		{
			name: "get relation",
			// 0,0,0,1 = 1 (relation id int32)
			// 112, 117, 98, 108, 105, 99 = public (namespace, text)
			// 0 = end of string
			// 117, 115, 101, 114, 115 = users (table name, text)
			// 0 = end of string
			// 1 = int8, replica
			// 0 = zero columns
			fields: fields{
				src: []byte{
					0, 0, 0, 1,
					112, 117, 98, 108, 105, 99, 0,
					117, 115, 101, 114, 115, 0,
					1,
					0,
				},
			},
			want: Relation{
				ID:        1,
				Namespace: "public",
				Name:      "users",
				Replica:   1,
				Columns:   []RelationColumn{},
			},
		},
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &BinaryParser{
				log:       logger,
				byteOrder: binary.BigEndian,
				buffer:    bytes.NewBuffer(tt.fields.src),
			}
			if got := p.getRelationMsg(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getRelationMsg() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBinaryParser_getUpdateMsg(t *testing.T) {
	type fields struct {
		src []byte
	}
	tests := []struct {
		name   string
		fields fields
		want   Update
	}{
		{
			name: "get message",
			fields: fields{
				// 0,0,0,5 = 5 int32, relation id
				// 79 = O flag - old tuple data
				// 0,1 = 1 int16 - count of rows
				// 116 = t data type text
				// 0,0,0,5 = 5 int32 size (data bytes)
				// 104, 101, 108, 108, 111 = hello
				// 78 = N flag - new tuple data
				// 0,1 = 1 int16 - count of rows
				// 116 = t data type text
				// 0,0,0,6 = 6 int32 size (data bytes)
				// 104, 101, 108, 108, 111, 50  = hello2
				src: []byte{
					0, 0, 0, 5,
					79,
					0, 1,
					116,
					0, 0, 0, 5,
					104, 101, 108, 108, 111,
					78,
					0, 1,
					116,
					0, 0, 0, 6,
					104, 101, 108, 108, 111, 50,
				},
			},
			want: Update{
				RelationID: 5,
				KeyTuple:   false,
				OldTuple:   true,
				OldRow: []TupleData{
					{
						Value: []byte{104, 101, 108, 108, 111},
					},
				},
				NewTuple: false,
				NewRow: []TupleData{
					{
						Value: []byte{104, 101, 108, 108, 111, 50},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &BinaryParser{
				byteOrder: binary.BigEndian,
				buffer:    bytes.NewBuffer(tt.fields.src),
			}
			got := p.getUpdateMsg()
			assert.Equal(t, got, tt.want)
		})
	}
}

func TestBinaryParser_getDeleteMsg(t *testing.T) {
	type fields struct {
		src []byte
	}
	tests := []struct {
		name   string
		fields fields
		want   Delete
	}{
		{
			name: "parse delete message",
			fields: fields{
				// 0,0,0,5 = 5 int32, relation id
				// 79 = O flag - old tuple data
				// 0,1 = 1 int16 - count of rows
				// 116 = t data type text
				// 0,0,0,5 = 5 int32 size (data bytes)
				// 105, 100 = id
				src: []byte{
					0, 0, 0, 5,
					79,
					0, 1,
					116,
					0, 0, 0, 5,
					105, 100,
				},
			},
			want: Delete{
				RelationID: 5,
				KeyTuple:   false,
				OldTuple:   true,
				OldRow: []TupleData{
					{
						Value: []byte{105, 100},
					},
				},
			},
		},
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &BinaryParser{
				log:       logger,
				byteOrder: binary.BigEndian,
				buffer:    bytes.NewBuffer(tt.fields.src),
			}

			if got := p.getDeleteMsg(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getDeleteMsg() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBinaryParser_getInsertMsg(t *testing.T) {
	type fields struct {
		src []byte
	}
	tests := []struct {
		name   string
		fields fields
		want   Insert
	}{
		{
			name: "parse insert message",
			fields: fields{
				// 0,0,0,5 = 5 int32, relation id
				// 78 = N flag - new tuple data
				// 0,1 = 1 int16 - count of rows
				// 116 = t data type text
				// 0,0,0,6 = 6 int32 size (data bytes)
				// 104, 101, 108, 108, 111  = hello
				src: []byte{
					0, 0, 0, 5,
					78,
					0, 1,
					116,
					0, 0, 0, 6,
					104, 101, 108, 108, 111,
				},
			},
			want: Insert{
				RelationID: 5,
				NewTuple:   true,
				NewRow: []TupleData{
					{
						Value: []byte{104, 101, 108, 108, 111},
					},
				},
			},
		},
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &BinaryParser{
				log:       logger,
				byteOrder: binary.BigEndian,
				buffer:    bytes.NewBuffer(tt.fields.src),
			}
			if got := p.getInsertMsg(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getInsertMsg() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBinaryParser_getCommitMsg(t *testing.T) {
	type fields struct {
		src []byte
	}

	tests := []struct {
		name   string
		fields fields
		want   Commit
	}{
		{
			name: "parse commit message",
			fields: fields{
				// 0 int8, flag
				// 7 int64, lsn start
				// 8 int64, lsn stop
				// 0 int64, timestamp (start postgres epoch)
				src: []byte{
					0,
					0, 0, 0, 0, 0, 0, 0, 7,
					0, 0, 0, 0, 0, 0, 0, 8,
					0, 0, 0, 0, 0, 0, 0, 0,
				},
			},
			want: Commit{
				Flags:          0,
				LSN:            7,
				TransactionLSN: 8,
				Timestamp:      postgresEpoch,
			},
		},
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &BinaryParser{
				log:       logger,
				byteOrder: binary.BigEndian,
				buffer:    bytes.NewBuffer(tt.fields.src),
			}
			if got := p.getCommitMsg(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getCommitMsg() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBinaryParser_getBeginMsg(t *testing.T) {
	type fields struct {
		src []byte
	}
	tests := []struct {
		name   string
		fields fields
		want   Begin
	}{
		{
			name: "parse begin message",
			fields: fields{
				// int64 lsn
				// int64 timestamp
				// int32 transaction id
				src: []byte{
					0, 0, 0, 0, 0, 0, 0, 7,
					0, 0, 0, 0, 0, 0, 0, 0,
					0, 0, 0, 5,
				},
			},
			want: Begin{
				LSN:       7,
				Timestamp: postgresEpoch,
				XID:       5,
			},
		},
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &BinaryParser{
				log:       logger,
				byteOrder: binary.BigEndian,
				buffer:    bytes.NewBuffer(tt.fields.src),
			}

			if got := p.getBeginMsg(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getBeginMsg() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBinaryParser_getOriginMsg(t *testing.T) {
	type fields struct {
		src []byte
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "parse origin message",
			fields: fields{
				// 111, 114, 105, 103, 105, 110, 95, 49 = origin_1 (text)
				// 0 = end of string
				src: []byte{111, 114, 105, 103, 105, 110, 95, 49, 0},
			},
			want: "origin_1",
		},
		{
			name: "parse empty origin",
			fields: fields{
				// 0 = end of string (empty origin)
				src: []byte{0},
			},
			want: "",
		},
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &BinaryParser{
				log:       logger,
				byteOrder: binary.BigEndian,
				buffer:    bytes.NewBuffer(tt.fields.src),
			}
			if got := p.getOriginMsg(); got != tt.want {
				t.Errorf("getOriginMsg() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBinaryParser_ParseWalMessage(t *testing.T) {
	type args struct {
		msg []byte
		tx  *WalTransaction
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	metrics := new(monitorMock)

	tests := []struct {
		name    string
		args    args
		wantErr bool
		want    *WalTransaction
	}{
		{
			name:    "empty data",
			args:    args{},
			wantErr: true,
		},
		{
			name: "begin message",
			args: args{
				msg: []byte{
					66,
					0, 0, 0, 0, 0, 0, 0, 7,
					0, 0, 0, 0, 0, 0, 0, 0,
					0, 0, 0, 5,
				},
				tx: NewWalTransaction(logger, nil, metrics, nil, config.ExcludeStruct{}, map[string]string{
					"environment": "test",
					"service":     "wal-listener",
				}, false),
			},
			want: &WalTransaction{
				pool:          nil,
				log:           logger,
				LSN:           7,
				monitor:       metrics,
				BeginTime:     &postgresEpoch,
				RelationStore: make(map[int32]RelationData),
				Actions:       make([]ActionData, 0),
				tags:          map[string]string{"environment": "test", "service": "wal-listener"},
			},
			wantErr: false,
		},
		{
			name: "commit message",
			args: args{
				msg: []byte{
					67,
					0,
					0, 0, 0, 0, 0, 0, 0, 7,
					0, 0, 0, 0, 0, 0, 0, 8,
					0, 0, 0, 0, 0, 0, 0, 0,
				},
				tx: &WalTransaction{
					log:           logger,
					LSN:           7,
					monitor:       metrics,
					BeginTime:     &postgresEpoch,
					RelationStore: make(map[int32]RelationData),
				},
			},
			want: &WalTransaction{
				log:           logger,
				LSN:           7,
				monitor:       metrics,
				BeginTime:     &postgresEpoch,
				CommitTime:    &postgresEpoch,
				RelationStore: make(map[int32]RelationData),
			},
			wantErr: false,
		},
		{
			name: "relation message",
			args: args{
				// 82 - R
				// 3 - int32 relation id
				// public
				// users
				// int8 replica ?
				// int16 rows count
				// int8 isKey bool
				// field name
				// filed type pgtype
				// modificator?
				msg: []byte{
					82,
					0, 0, 0, 3,
					112, 117, 98, 108, 105, 99, 0,
					117, 115, 101, 114, 115, 0,
					1,
					0, 1,
					1,
					105, 100, 0,
					0, 0, 0, 23,
					0, 0, 0, 1,
				},
				tx: &WalTransaction{
					log:           logger,
					LSN:           3,
					monitor:       metrics,
					BeginTime:     &postgresEpoch,
					CommitTime:    &postgresEpoch,
					RelationStore: make(map[int32]RelationData),
				},
			},
			want: &WalTransaction{
				log:        logger,
				monitor:    metrics,
				LSN:        3,
				BeginTime:  &postgresEpoch,
				CommitTime: &postgresEpoch,
				RelationStore: map[int32]RelationData{
					3: {
						Schema: "public",
						Table:  "users",
						Columns: []Column{
							{
								log:       logger,
								name:      "id",
								value:     nil,
								valueType: pgtype.Int4OID,
								isKey:     true,
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "insert message",
			args: args{
				// 73 - I
				// 3 - int32 relation id
				// public
				// users
				// int8 replica ?
				// int16 rows count
				// int8 isKey bool
				// field name
				// filed type pgtype
				// modificator?
				msg: []byte{
					73,
					0, 0, 0, 2,
					78,
					0, 1,
					116,
					0, 0, 0, 6,
					49, 48,
				},
				tx: &WalTransaction{
					monitor:    metrics,
					log:        logger,
					LSN:        4,
					BeginTime:  &postgresEpoch,
					CommitTime: &postgresEpoch,
					RelationStore: map[int32]RelationData{
						2: {
							Schema: "public",
							Table:  "users",
							Columns: []Column{
								{
									log:       logger,
									name:      "id",
									value:     nil,
									valueType: pgtype.Int4OID,
									isKey:     true,
								},
							},
						},
					},
				},
			},
			want: &WalTransaction{
				monitor:    metrics,
				log:        logger,
				LSN:        4,
				BeginTime:  &postgresEpoch,
				CommitTime: &postgresEpoch,
				RelationStore: map[int32]RelationData{
					2: {
						Schema: "public",
						Table:  "users",
						Columns: []Column{
							{
								log:       logger,
								name:      "id",
								value:     nil,
								valueType: pgtype.Int4OID,
								isKey:     true,
							},
						},
					},
				},
				Actions: []ActionData{
					{
						Schema: "public",
						Table:  "users",
						Kind:   ActionKindInsert,
						NewColumns: []Column{
							{
								log:       logger,
								name:      "id",
								value:     10,
								valueType: pgtype.Int4OID,
								isKey:     true,
							},
						},
						OldColumns: []Column{},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "parse update message",
			args: args{
				// 85 - U
				// 0,0,0,5 = 5 int32, relation id
				// 79 = O flag - old tuple data
				// 0,1 = 1 int16 - count of rows
				// 116 = t data type text
				// 0,0,0,2 = 2 int32 size (data bytes)
				// 55, 55 = 77
				// 78 = N flag - new tuple data
				// 0,1 = 1 int16 - count of rows
				// 116 = t data type text
				// 0,0,0,2 = 2 int32 size (data bytes)
				// 56, 48  = 80
				msg: []byte{
					85,
					0, 0, 0, 5,
					79,
					0, 1,
					116,
					0, 0, 0, 2,
					55, 55,
					78,
					0, 1,
					116,
					0, 0, 0, 2,
					56, 48,
				},
				tx: &WalTransaction{
					log:        logger,
					monitor:    metrics,
					LSN:        4,
					BeginTime:  &postgresEpoch,
					CommitTime: &postgresEpoch,
					RelationStore: map[int32]RelationData{
						5: {
							Schema: "public",
							Table:  "users",
							Columns: []Column{
								{
									log:       logger,
									name:      "id",
									value:     nil,
									valueType: pgtype.Int4OID,
									isKey:     true,
								},
							},
						},
					},
				},
			},
			want: &WalTransaction{
				monitor:    metrics,
				log:        logger,
				LSN:        4,
				BeginTime:  &postgresEpoch,
				CommitTime: &postgresEpoch,
				RelationStore: map[int32]RelationData{
					5: {
						Schema: "public",
						Table:  "users",
						Columns: []Column{
							{
								log:       logger,
								name:      "id",
								value:     nil,
								valueType: pgtype.Int4OID,
								isKey:     true,
							},
						},
					},
				},
				Actions: []ActionData{
					{
						Schema: "public",
						Table:  "users",
						Kind:   ActionKindUpdate,
						OldColumns: []Column{
							{
								log:       logger,
								name:      "id",
								value:     77,
								valueType: pgtype.Int4OID,
								isKey:     true,
							},
						},
						NewColumns: []Column{
							{
								log:       logger,
								name:      "id",
								value:     80,
								valueType: pgtype.Int4OID,
								isKey:     true,
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "parse delete message",
			args: args{
				// 68 - D,
				// 0,0,0,5 = 5 int32, relation id
				// 79 = O flag - old tuple data
				// 0,1 = 1 int16 - count of rows
				// 116 = t data type text
				// 0,0,0,2 = 2 int32 size (data bytes)
				// 55,55 = 77
				msg: []byte{
					68,
					0, 0, 0, 5,
					79,
					0, 1,
					116,
					0, 0, 0, 2,
					55, 55,
				},
				tx: &WalTransaction{
					monitor:    metrics,
					log:        logger,
					LSN:        4,
					BeginTime:  &postgresEpoch,
					CommitTime: &postgresEpoch,
					RelationStore: map[int32]RelationData{
						5: {
							Schema: "public",
							Table:  "users",
							Columns: []Column{
								{
									log:       logger,
									name:      "id",
									value:     nil,
									valueType: pgtype.Int4OID,
									isKey:     true,
								},
							},
						},
					},
				},
			},
			want: &WalTransaction{
				monitor:    metrics,
				log:        logger,
				LSN:        4,
				BeginTime:  &postgresEpoch,
				CommitTime: &postgresEpoch,
				RelationStore: map[int32]RelationData{
					5: {
						Schema: "public",
						Table:  "users",
						Columns: []Column{
							{
								log:       logger,
								name:      "id",
								value:     nil,
								valueType: pgtype.Int4OID,
								isKey:     true,
							},
						},
					},
				},
				Actions: []ActionData{
					{
						Schema:     "public",
						Table:      "users",
						Kind:       ActionKindDelete,
						NewColumns: []Column{},
						OldColumns: []Column{
							{
								log:       logger,
								name:      "id",
								value:     77,
								valueType: pgtype.Int4OID,
								isKey:     true,
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "unknown message type",
			args: args{
				msg: []byte{
					11,
					0, 0, 0, 5,
					79,
					0, 1,
					116,
					0, 0, 0, 2,
					55, 55,
				},
				tx: &WalTransaction{
					monitor:    metrics,
					log:        logger,
					LSN:        4,
					BeginTime:  &postgresEpoch,
					CommitTime: &postgresEpoch,
					RelationStore: map[int32]RelationData{
						5: {
							Schema: "public",
							Table:  "users",
							Columns: []Column{
								{
									log:       logger,
									name:      "id",
									value:     nil,
									valueType: pgtype.Int4OID,
									isKey:     true,
								},
							},
						},
					},
				},
			},
			want: &WalTransaction{
				monitor:    metrics,
				log:        logger,
				LSN:        4,
				BeginTime:  &postgresEpoch,
				CommitTime: &postgresEpoch,
				RelationStore: map[int32]RelationData{
					5: {
						Schema: "public",
						Table:  "users",
						Columns: []Column{
							{
								log:       logger,
								name:      "id",
								value:     nil,
								valueType: pgtype.Int4OID,
								isKey:     true,
							},
						},
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &BinaryParser{
				log:       logger,
				byteOrder: binary.BigEndian,
			}

			if err := p.ParseWalMessage(tt.args.msg, tt.args.tx); (err != nil) != tt.wantErr {
				t.Errorf("ParseWalMessage() error = %v, wantErr %v", err, tt.wantErr)
			}

			assert.Equal(t, tt.want, tt.args.tx)
		})
	}
}

func TestBinaryParser_ParseWalMessage_DropForeignOrigin(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	metrics := new(monitorMock)

	tests := []struct {
		name              string
		dropForeignOrigin bool
		messages          [][]byte
		wantActionsCount  int
		wantOrigin        string
		wantErr           bool
	}{
		{
			name:              "origin message - drop disabled",
			dropForeignOrigin: false,
			messages: [][]byte{
				// Origin message: OriginMsgType + "origin_1\0"
				{byte(OriginMsgType), 111, 114, 105, 103, 105, 110, 95, 49, 0},
				// Insert message: InsertMsgType + relation_id + N + tuple_data
				{
					byte(InsertMsgType),
					0, 0, 0, 1, // relation id
					78,   // N flag
					0, 1, // 1 column
					116,        // text type
					0, 0, 0, 4, // 4 bytes
					116, 101, 115, 116, // "test"
				},
			},
			wantActionsCount: 1,
			wantOrigin:       "origin_1",
			wantErr:          false,
		},
		{
			name:              "origin message - drop enabled with foreign origin",
			dropForeignOrigin: true,
			messages: [][]byte{
				// Origin message: OriginMsgType + "origin_1\0"
				{byte(OriginMsgType), 111, 114, 105, 103, 105, 110, 95, 49, 0},
				// Insert message should be dropped
				{
					byte(InsertMsgType),
					0, 0, 0, 1, // relation id
					78,   // N flag
					0, 1, // 1 column
					116,        // text type
					0, 0, 0, 4, // 4 bytes
					116, 101, 115, 116, // "test"
				},
			},
			wantActionsCount: 0, // Actions should be empty because message was dropped
			wantOrigin:       "origin_1",
			wantErr:          false,
		},
		{
			name:              "no origin message - drop enabled",
			dropForeignOrigin: true,
			messages: [][]byte{
				// Insert message without origin should pass through
				{
					byte(InsertMsgType),
					0, 0, 0, 1, // relation id
					78,   // N flag
					0, 1, // 1 column
					116,        // text type
					0, 0, 0, 4, // 4 bytes
					116, 101, 115, 116, // "test"
				},
			},
			wantActionsCount: 1, // Should pass through when no origin set
			wantOrigin:       "",
			wantErr:          false,
		},
		{
			name:              "multiple data messages after origin - drop enabled",
			dropForeignOrigin: true,
			messages: [][]byte{
				// Origin message: OriginMsgType + "foreign_origin\0"
				{byte(OriginMsgType), 102, 111, 114, 101, 105, 103, 110, 95, 111, 114, 105, 103, 105, 110, 0},
				// Multiple insert messages should all be dropped
				{
					byte(InsertMsgType),
					0, 0, 0, 1, // relation id
					78,   // N flag
					0, 1, // 1 column
					116,        // text type
					0, 0, 0, 4, // 4 bytes
					116, 101, 115, 116, // "test"
				},
				{
					byte(InsertMsgType),
					0, 0, 0, 1, // relation id
					78,   // N flag
					0, 1, // 1 column
					116,        // text type
					0, 0, 0, 5, // 5 bytes
					116, 101, 115, 116, 50, // "test2"
				},
				// Update message should also be dropped
				{
					byte(UpdateMsgType),
					0, 0, 0, 1, // relation id
					78,   // N flag (new tuple)
					0, 1, // 1 column
					116,        // text type
					0, 0, 0, 7, // 7 bytes
					117, 112, 100, 97, 116, 101, 100, // "updated"
				},
			},
			wantActionsCount: 0, // All messages should be dropped
			wantOrigin:       "foreign_origin",
			wantErr:          false,
		},
		{
			name:              "large transaction with foreign origin - all dropped",
			dropForeignOrigin: true,
			messages: [][]byte{
				// Origin message: OriginMsgType + "replica_db\0"
				{byte(OriginMsgType), 114, 101, 112, 108, 105, 99, 97, 95, 100, 98, 0},
				// Relation message should be dropped
				{
					byte(RelationMsgType),
					0, 0, 0, 1, // relation id
					112, 117, 98, 108, 105, 99, 0, // "public\0"
					116, 101, 115, 116, 95, 116, 97, 98, 108, 101, 0, // "test_table\0"
					100, // replica identity
					0, 1, // 1 column
					0, // not key
					99, 111, 108, 49, 0, // "col1\0"
					0, 0, 0, 25, // text type
					255, 255, 255, 255, // no modifier
				},
				// Insert should be dropped
				{
					byte(InsertMsgType),
					0, 0, 0, 1, // relation id
					78,   // N flag
					0, 1, // 1 column
					116,        // text type
					0, 0, 0, 4, // 4 bytes
					116, 101, 115, 116, // "test"
				},
				// Delete should be dropped
				{
					byte(DeleteMsgType),
					0, 0, 0, 1, // relation id
					79,   // O flag (old tuple)
					0, 1, // 1 column
					116,        // text type
					0, 0, 0, 4, // 4 bytes
					116, 101, 115, 116, // "test"
				},
			},
			wantActionsCount: 0, // All data messages should be dropped
			wantOrigin:       "replica_db",
			wantErr:          false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx := NewWalTransaction(logger, nil, metrics, nil, config.ExcludeStruct{}, map[string]string{}, tt.dropForeignOrigin)
			// Add a dummy relation to prevent relation not found errors
			tx.RelationStore[1] = RelationData{
				Schema: "public",
				Table:  "test_table",
				Columns: []Column{
					{
						log:       logger,
						name:      "name",
						valueType: TextOID,
					},
				},
			}

			parser := NewBinaryParser(logger, binary.BigEndian)

			for _, msg := range tt.messages {
				err := parser.ParseWalMessage(msg, tx)
				if (err != nil) != tt.wantErr {
					t.Errorf("ParseWalMessage() error = %v, wantErr %v", err, tt.wantErr)
				}
			}

			if len(tx.Actions) != tt.wantActionsCount {
				t.Errorf("Actions count = %v, want %v", len(tx.Actions), tt.wantActionsCount)
			}

			if tx.origin != tt.wantOrigin {
				t.Errorf("Origin = %v, want %v", tx.origin, tt.wantOrigin)
			}
		})
	}
}
