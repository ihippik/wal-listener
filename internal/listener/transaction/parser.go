package transaction

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// BinaryParser represent binary protocol parser.
type BinaryParser struct {
	log       *slog.Logger
	byteOrder binary.ByteOrder
	msgType   byte
	buffer    *bytes.Buffer
}

var (
	ErrEmptyWALMessage = errors.New("empty WAL message")
	ErrMessageLost     = errors.New("messages are lost")
)

// NewBinaryParser create instance of a binary parser.
func NewBinaryParser(logger *slog.Logger, byteOrder binary.ByteOrder) *BinaryParser {
	return &BinaryParser{
		log:       logger,
		byteOrder: byteOrder,
	}
}

// ParseWalMessage parse postgres WAL message.
func (p *BinaryParser) ParseWalMessage(msg []byte, tx *WAL) error {
	if len(msg) == 0 {
		return ErrEmptyWALMessage
	}

	p.msgType = msg[0]
	p.buffer = bytes.NewBuffer(msg[1:])

	switch p.msgType {
	case BeginMsgType:
		begin := p.getBeginMsg()

		p.log.Debug(
			"begin type message was received",
			slog.Int64("lsn", begin.LSN),
			slog.Any("xid", begin.XID),
		)

		tx.LSN = begin.LSN
		tx.BeginTime = &begin.Timestamp
	case CommitMsgType:
		commit := p.getCommitMsg()

		p.log.Debug(
			"commit message was received",
			slog.Int64("lsn", commit.LSN),
			slog.Int64("transaction_lsn", commit.TransactionLSN),
		)

		if tx.LSN > 0 && tx.LSN != commit.LSN {
			return fmt.Errorf("commit: %w", ErrMessageLost)
		}

		tx.CommitTime = &commit.Timestamp
	case OriginMsgType:
		p.log.Debug("origin type message was received")
	case RelationMsgType:
		relation := p.getRelationMsg()

		p.log.Debug(
			"relation type message was received",
			slog.Any("relation_id", relation.ID),
			slog.String("schema", relation.Namespace),
		)

		if tx.LSN == 0 {
			return fmt.Errorf("commit: %w", ErrMessageLost)
		}

		rd := RelationData{
			Schema: relation.Namespace,
			Table:  relation.Name,
		}

		for _, rf := range relation.Columns {
			c := InitColumn(p.log, rf.Name, nil, int(rf.TypeID), rf.Key)
			rd.Columns = append(rd.Columns, c)
		}

		tx.RelationStore[relation.ID] = rd
	case TypeMsgType:
		p.log.Debug("type message was received")
	case InsertMsgType:
		insert := p.getInsertMsg()

		p.log.Debug(
			"insert type message was received",
			slog.Any("relation_id", insert.RelationID),
		)

		action, err := tx.CreateActionData(
			insert.RelationID,
			nil,
			insert.NewRow,
			ActionKindInsert,
		)
		if err != nil {
			return fmt.Errorf("create action data: %w", err)
		}

		tx.Actions = append(tx.Actions, action)
	case UpdateMsgType:
		upd := p.getUpdateMsg()

		p.log.Debug("update type message was received", slog.Any("relation_id", upd.RelationID))

		action, err := tx.CreateActionData(
			upd.RelationID,
			upd.OldRow,
			upd.NewRow,
			ActionKindUpdate,
		)
		if err != nil {
			return fmt.Errorf("create action data: %w", err)
		}

		tx.Actions = append(tx.Actions, action)
	case DeleteMsgType:
		del := p.getDeleteMsg()

		p.log.Debug(
			"delete type message was received",
			slog.Any("relation_id", del.RelationID),
		)

		action, err := tx.CreateActionData(
			del.RelationID,
			del.OldRow,
			nil,
			ActionKindDelete,
		)
		if err != nil {
			return fmt.Errorf("create action data: %w", err)
		}

		tx.Actions = append(tx.Actions, action)
	case TruncateMsgType:
		trc := p.getTruncateMsg()

		for _, relID := range trc.RelationIDs {
			action, err := tx.CreateActionData(
				relID,
				nil,
				nil,
				ActionKindTruncate,
			)
			if err != nil {
				return fmt.Errorf("create action data: %w", err)
			}

			tx.Actions = append(tx.Actions, action)
		}

		p.log.Debug(
			"truncate type message was received",
			slog.Any("num of relations", trc.RelationsCount),
		)
	default:
		p.log.Warn("unknown type message was received", slog.String("type", string(p.msgType)))
	}

	return nil
}

func (p *BinaryParser) getBeginMsg() Begin {
	return Begin{
		LSN:       p.readInt64(),
		Timestamp: p.readTimestamp(),
		XID:       p.readInt32(),
	}
}

func (p *BinaryParser) getCommitMsg() Commit {
	return Commit{
		Flags:          p.readInt8(),
		LSN:            p.readInt64(),
		TransactionLSN: p.readInt64(),
		Timestamp:      p.readTimestamp(),
	}
}

func (p *BinaryParser) getInsertMsg() Insert {
	return Insert{
		RelationID: p.readInt32(),
		NewTuple:   p.buffer.Next(1)[0] == NewTupleDataType,
		NewRow:     p.readTupleData(),
	}
}

func (p *BinaryParser) getDeleteMsg() Delete {
	return Delete{
		RelationID: p.readInt32(),
		KeyTuple:   p.charIsExists('K'),
		OldTuple:   p.charIsExists('O'),
		OldRow:     p.readTupleData(),
	}
}

func (p *BinaryParser) getTruncateMsg() Truncate {
	var trunc Truncate

	trunc.RelationsCount = p.readInt32()
	trunc.Option = p.readInt8()

	trunc.RelationIDs = make([]int32, 0, int(trunc.RelationsCount))

	for range trunc.RelationsCount {
		trunc.RelationIDs = append(trunc.RelationIDs, p.readInt32())
	}

	return trunc
}

func (p *BinaryParser) getUpdateMsg() Update {
	u := Update{}
	u.RelationID = p.readInt32()
	u.KeyTuple = p.charIsExists('K')
	u.OldTuple = p.charIsExists('O')

	if u.KeyTuple || u.OldTuple {
		u.OldRow = p.readTupleData()
	}

	u.OldTuple = p.charIsExists('N')
	u.NewRow = p.readTupleData()

	return u
}

func (p *BinaryParser) getRelationMsg() Relation {
	return Relation{
		ID:        p.readInt32(),
		Namespace: p.readString(),
		Name:      p.readString(),
		Replica:   p.readInt8(),
		Columns:   p.readColumns(),
	}
}

func (p *BinaryParser) readInt32() (val int32) {
	r := bytes.NewReader(p.buffer.Next(4))
	_ = binary.Read(r, p.byteOrder, &val)

	return
}

func (p *BinaryParser) readInt64() (val int64) {
	r := bytes.NewReader(p.buffer.Next(8))
	_ = binary.Read(r, p.byteOrder, &val)

	return
}

func (p *BinaryParser) readInt8() (val int8) {
	r := bytes.NewReader(p.buffer.Next(1))
	_ = binary.Read(r, p.byteOrder, &val)

	return
}

func (p *BinaryParser) readInt16() (val int16) {
	r := bytes.NewReader(p.buffer.Next(2))
	_ = binary.Read(r, p.byteOrder, &val)

	return
}

func (p *BinaryParser) readTimestamp() time.Time {
	ns := p.readInt64()

	return postgresEpoch.Add(time.Duration(ns) * time.Microsecond)
}

func (p *BinaryParser) readString() (str string) {
	stringBytes, _ := p.buffer.ReadBytes(0)

	return string(bytes.Trim(stringBytes, "\x00"))
}

func (p *BinaryParser) readBool() bool {
	x := p.buffer.Next(1)[0]

	return x != 0
}

func (p *BinaryParser) charIsExists(char byte) bool {
	if p.buffer.Next(1)[0] == char {
		return true
	}
	_ = p.buffer.UnreadByte()

	return false
}

func (p *BinaryParser) readColumns() []RelationColumn {
	size := int(p.readInt16())
	data := make([]RelationColumn, size)

	for i := 0; i < size; i++ {
		data[i] = RelationColumn{
			Key:          p.readBool(),
			Name:         p.readString(),
			TypeID:       p.readInt32(),
			ModifierType: p.readInt32(),
		}
	}

	return data
}

func (p *BinaryParser) readTupleData() []TupleData {
	size := int(p.readInt16())
	data := make([]TupleData, size)

	for i := 0; i < size; i++ {
		sl := p.buffer.Next(1)

		switch sl[0] {
		case NullDataType:
			p.log.Debug("tupleData: null data type")
		case ToastDataType:
			p.log.Debug("tupleData: toast data type")
		case TextDataType:
			vSize := int(p.readInt32())
			data[i] = TupleData{Value: p.buffer.Next(vSize)}
		}
	}

	return data
}
