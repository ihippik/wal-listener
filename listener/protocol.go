package listener

import (
	"time"
)

const (
	CommitMsgType   byte = 'C'
	BeginMsgType    byte = 'B'
	OriginMsgType   byte = 'O'
	RelationMsgType byte = 'R'
	TypeMsgType     byte = 'Y'
	InsertMsgType   byte = 'I'
	UpdateMsgType   byte = 'U'
	DeleteMsgType   byte = 'D'

	NewTupleDataType byte = 'N'
	TextDataType     byte = 't'
	NullDataType     byte = 'n'
	ToastDataType    byte = 'u'
)

var postgresEpoch = time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)

// Logical Replication Message Formats.
// https://postgrespro.ru/docs/postgrespro/10/protocol-logicalrep-message-formats#
type (
	Begin struct {
		// Identifies the message as a begin message.
		LSN int64
		// Commit timestamp of the transaction.
		Timestamp time.Time
		// 	Xid of the transaction.
		XID int32
	}

	Commit struct {
		// Flags; currently unused (must be 0).
		Flags int8
		// The LSN of the commit.
		LSN int64
		// The end LSN of the transaction.
		TransactionLSN int64
		// Commit timestamp of the transaction.
		Timestamp time.Time
	}

	Origin struct {
		// The LSN of the commit on the origin server.
		LSN int64
		// Name of the origin.
		Name string
	}

	Relation struct {
		// ID of the relation.
		ID int32
		// Namespace (empty string for pg_catalog).
		Namespace string
		// Relation name.
		Name string
		// Replica identity setting for the relation (same as relreplident in pg_class).
		Replica int8
		Columns []RelationColumn
	}

	Insert struct {
		/// ID of the relation corresponding to the ID in the relation message.
		RelationID int32
		// Identifies the following TupleData message as a new tuple.
		NewTuple bool
		// TupleData message part representing the contents of new tuple.
		Row []TupleData
	}

	Update struct {
		/// ID of the relation corresponding to the ID in the relation message.
		RelationID int32
		// Identifies the following TupleData submessage as a key.
		KeyTuple bool
		// Identifies the following TupleData message as a old tuple.
		OldTuple bool
		// Identifies the following TupleData message as a new tuple.
		NewTuple bool
		// TupleData message part representing the contents of new tuple.
		Row []TupleData
		// TupleData message part representing the contents of the old tuple or primary key.
		//Only present if the previous 'O' or 'K' part is present.
		OldRow []TupleData
	}

	Delete struct {
		/// ID of the relation corresponding to the ID in the relation message.
		RelationID int32
		// Identifies the following TupleData submessage as a key.
		KeyTuple bool
		// Identifies the following TupleData message as a old tuple.
		OldTuple bool
		// TupleData message part representing the contents of new tuple.
		Row []TupleData
	}
)
type DataType struct {
	// ID of the data type.
	ID int32
	// Namespace (empty string for pg_catalog).
	Namespace string
	// Name of the data type.
	Name string
}

type RelationColumn struct {
	// Flags for the column which marks the column as part of the key.
	Key bool
	// Name of the column.
	Name string
	// ID of the column's data type.
	TypeID int32
	// Type modifier of the column (atttypmod).
	ModifierType int32
}

type TupleData struct {
	Value []byte
}
