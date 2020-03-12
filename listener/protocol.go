package listener

import (
	"time"
)

const (
	// CommitMsgType protocol commit message type.
	CommitMsgType byte = 'C'

	// BeginMsgType protocol begin message type.
	BeginMsgType byte = 'B'

	// OriginMsgType protocol original message type.
	OriginMsgType byte = 'O'

	// RelationMsgType protocol relation message type.
	RelationMsgType byte = 'R'

	// TypeMsgType protocol message type.
	TypeMsgType byte = 'Y'

	// InsertMsgType protocol insert message type.
	InsertMsgType byte = 'I'

	// UpdateMsgType protocol update message type.
	UpdateMsgType byte = 'U'

	// DeleteMsgType protocol delete message type.
	DeleteMsgType byte = 'D'

	// NewTupleDataType protocol new tuple data type.
	NewTupleDataType byte = 'N'

	// TextDataType protocol test data type.
	TextDataType byte = 't'

	// NullDataType protocol NULL data type.
	NullDataType byte = 'n'

	// ToastDataType protocol toast data type.
	ToastDataType byte = 'u'
)

var postgresEpoch = time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)

// Logical Replication Message Formats.
// https://postgrespro.ru/docs/postgrespro/10/protocol-logicalrep-message-formats#
type (
	// Begin message format.
	Begin struct {
		// Identifies the message as a begin message.
		LSN int64
		// Commit timestamp of the transaction.
		Timestamp time.Time
		// 	Xid of the transaction.
		XID int32
	}

	// Commit message format.
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

	// Origin message format.
	Origin struct {
		// The LSN of the commit on the origin server.
		LSN int64
		// name of the origin.
		Name string
	}

	// Relation message format.
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

	// Insert message format.
	Insert struct {
		/// ID of the relation corresponding to the ID in the relation message.
		RelationID int32
		// Identifies the following TupleData message as a new tuple.
		NewTuple bool
		// TupleData message part representing the contents of new tuple.
		Row []TupleData
	}

	// Update message format.
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

	// Delete message format.
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

// DataType path of WAL message data.
type DataType struct {
	// ID of the data type.
	ID int32
	// Namespace (empty string for pg_catalog).
	Namespace string
	// name of the data type.
	Name string
}

// RelationColumn path of WAL message data.
type RelationColumn struct {
	// Flags for the column which marks the column as part of the key.
	Key bool
	// name of the column.
	Name string
	// ID of the column's data type.
	TypeID int32
	// valueType modifier of the column (atttypmod).
	ModifierType int32
}

// TupleData path of WAL message data.
type TupleData struct {
	Value []byte
}
