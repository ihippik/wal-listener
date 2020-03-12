package listener

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
)

// BinaryParser represent binary protocol parser.
type BinaryParser struct {
	byteOrder binary.ByteOrder
	msgType   byte
	buffer    *bytes.Buffer
}

// NewBinaryParser create instance of binary parser.
func NewBinaryParser(byteOrder binary.ByteOrder) *BinaryParser {
	return &BinaryParser{
		byteOrder: byteOrder,
	}
}

// ParseWalMessage parse postgres WAL message.
func (p *BinaryParser) ParseWalMessage(msg []byte, tx *WalTransaction) error {
	if len(msg) == 0 {
		return errEmptyWALMessage
	}
	p.msgType = msg[0]
	p.buffer = bytes.NewBuffer(msg[1:])
	switch p.msgType {
	case BeginMsgType:
		begin := p.getBeginMsg()
		logrus.
			WithFields(
				logrus.Fields{
					"lsn": begin.LSN,
					"xid": begin.XID,
				}).
			Infoln("receive begin message")
		tx.LSN = begin.LSN
		tx.BeginTime = &begin.Timestamp
	case CommitMsgType:
		commit := p.getCommitMsg()
		logrus.
			WithFields(
				logrus.Fields{
					"lsn":             commit.LSN,
					"transaction_lsn": commit.TransactionLSN,
				}).
			Infoln("receive commit message")
		if tx.LSN > 0 && tx.LSN != commit.LSN {
			return fmt.Errorf("commit: %w", errMessageLost)
		}
		tx.CommitTime = &commit.Timestamp
	case OriginMsgType:
		logrus.Infoln("receive origin message")
	case RelationMsgType:
		relation := p.getRelationMsg()
		logrus.
			WithFields(
				logrus.Fields{
					"relation_id": relation.ID,
					"replica":     relation.Replica,
				}).
			Infoln("receive relation message")
		if tx.LSN == 0 {
			return fmt.Errorf("commit: %w", errMessageLost)
		}
		rd := RelationData{
			Schema: relation.Namespace,
			Table:  relation.Name,
		}
		for _, rf := range relation.Columns {
			c := Column{
				name:      rf.Name,
				valueType: int(rf.TypeID),
				isKey:     rf.Key,
			}
			rd.Columns = append(rd.Columns, c)
		}
		tx.RelationStore[relation.ID] = rd

	case TypeMsgType:
		logrus.Infoln("type")
	case InsertMsgType:
		insert := p.getInsertMsg()
		logrus.
			WithFields(
				logrus.Fields{
					"relation_id": insert.RelationID,
				}).
			Infoln("receive insert message")
		action, err := tx.CreateActionData(
			insert.RelationID,
			insert.Row,
			ActionKindInsert,
		)
		if err != nil {
			return fmt.Errorf("create action data: %w", err)
		}
		tx.Actions = append(tx.Actions, action)
	case UpdateMsgType:
		upd := p.getUpdateMsg()
		logrus.
			WithFields(
				logrus.Fields{
					"relation_id": upd.RelationID,
				}).
			Infoln("receive update message")
		action, err := tx.CreateActionData(
			upd.RelationID,
			upd.Row,
			ActionKindUpdate,
		)
		if err != nil {
			return fmt.Errorf("create action data: %w", err)
		}
		tx.Actions = append(tx.Actions, action)
	case DeleteMsgType:
		del := p.getDeleteMsg()
		logrus.
			WithFields(
				logrus.Fields{
					"relation_id": del.RelationID,
				}).
			Infoln("receive delete message")
		action, err := tx.CreateActionData(
			del.RelationID,
			del.Row,
			ActionKindDelete,
		)
		if err != nil {
			return fmt.Errorf("create action data: %w", err)
		}
		tx.Actions = append(tx.Actions, action)
	default:
		return fmt.Errorf("%w : %s", errUnknownMessageType, []byte{p.msgType})
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
		Row:        p.readTupleData(),
	}
}

func (p *BinaryParser) getDeleteMsg() Delete {
	return Delete{
		RelationID: p.readInt32(),
		KeyTuple:   p.charIsExists('K'),
		OldTuple:   p.charIsExists('O'),
		Row:        p.readTupleData(),
	}
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
	u.Row = p.readTupleData()
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
			logrus.Debugln("tupleData: null data type")
		case ToastDataType:
			logrus.Debugln(
				"tupleData: toast data type")
		case TextDataType:
			vsize := int(p.readInt32())
			data[i] = TupleData{Value: p.buffer.Next(vsize)}
		}
	}
	return data
}
