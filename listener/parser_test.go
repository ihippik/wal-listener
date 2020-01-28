package listener

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"

	"github.com/jackc/pgx/pgtype"
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
				{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &BinaryParser{
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
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &BinaryParser{
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
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &BinaryParser{
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
				NewTuple:   false,
				Row: []TupleData{
					{
						Value: []byte{104, 101, 108, 108, 111, 50},
					},
				},
				OldRow: []TupleData{
					{
						Value: []byte{104, 101, 108, 108, 111},
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
				Row: []TupleData{
					{
						Value: []byte{105, 100},
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
				Row: []TupleData{
					{
						Value: []byte{104, 101, 108, 108, 111},
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
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &BinaryParser{
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
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &BinaryParser{
				byteOrder: binary.BigEndian,
				buffer:    bytes.NewBuffer(tt.fields.src),
			}
			if got := p.getBeginMsg(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getBeginMsg() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBinaryParser_ParseWalMessage(t *testing.T) {
	type args struct {
		msg []byte
		tx  *WalTransaction
	}
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
				tx: NewWalTransaction(),
			},
			want: &WalTransaction{
				LSN:           7,
				BeginTime:     &postgresEpoch,
				RelationStore: make(map[int32]RelationData),
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
					LSN:           7,
					BeginTime:     &postgresEpoch,
					RelationStore: make(map[int32]RelationData),
				},
			},
			want: &WalTransaction{
				LSN:           7,
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
					LSN:           3,
					BeginTime:     &postgresEpoch,
					CommitTime:    &postgresEpoch,
					RelationStore: make(map[int32]RelationData),
				},
			},
			want: &WalTransaction{
				LSN:        3,
				BeginTime:  &postgresEpoch,
				CommitTime: &postgresEpoch,
				RelationStore: map[int32]RelationData{
					3: {
						Schema: "public",
						Table:  "users",
						Columns: []Column{
							{
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
					LSN:        4,
					BeginTime:  &postgresEpoch,
					CommitTime: &postgresEpoch,
					RelationStore: map[int32]RelationData{
						2: {
							Schema: "public",
							Table:  "users",
							Columns: []Column{
								{
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
				LSN:        4,
				BeginTime:  &postgresEpoch,
				CommitTime: &postgresEpoch,
				RelationStore: map[int32]RelationData{
					2: {
						Schema: "public",
						Table:  "users",
						Columns: []Column{
							{
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
						Columns: []Column{
							{
								name:      "id",
								value:     10,
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
					LSN:        4,
					BeginTime:  &postgresEpoch,
					CommitTime: &postgresEpoch,
					RelationStore: map[int32]RelationData{
						5: {
							Schema: "public",
							Table:  "users",
							Columns: []Column{
								{
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
				LSN:        4,
				BeginTime:  &postgresEpoch,
				CommitTime: &postgresEpoch,
				RelationStore: map[int32]RelationData{
					5: {
						Schema: "public",
						Table:  "users",
						Columns: []Column{
							{
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
						Columns: []Column{
							{
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
					LSN:        4,
					BeginTime:  &postgresEpoch,
					CommitTime: &postgresEpoch,
					RelationStore: map[int32]RelationData{
						5: {
							Schema: "public",
							Table:  "users",
							Columns: []Column{
								{
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
				LSN:        4,
				BeginTime:  &postgresEpoch,
				CommitTime: &postgresEpoch,
				RelationStore: map[int32]RelationData{
					5: {
						Schema: "public",
						Table:  "users",
						Columns: []Column{
							{
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
						Kind:   ActionKindDelete,
						Columns: []Column{
							{
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
					LSN:        4,
					BeginTime:  &postgresEpoch,
					CommitTime: &postgresEpoch,
					RelationStore: map[int32]RelationData{
						5: {
							Schema: "public",
							Table:  "users",
							Columns: []Column{
								{
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
				LSN:        4,
				BeginTime:  &postgresEpoch,
				CommitTime: &postgresEpoch,
				RelationStore: map[int32]RelationData{
					5: {
						Schema: "public",
						Table:  "users",
						Columns: []Column{
							{
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
				byteOrder: binary.BigEndian,
			}
			if err := p.ParseWalMessage(tt.args.msg, tt.args.tx); (err != nil) != tt.wantErr {
				t.Errorf("ParseWalMessage() error = %v, wantErr %v", err, tt.wantErr)
			}
			assert.Equal(t, tt.want, tt.args.tx)
		})
	}
}
