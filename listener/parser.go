package listener

import (
	"bytes"
	"encoding/binary"
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

// NewBinaryParser create instance of binary parser.
func NewBinaryParser(logger *slog.Logger, byteOrder binary.ByteOrder) *BinaryParser {
	return &BinaryParser{
		log:       logger,
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
			return fmt.Errorf("commit: %w", errMessageLost)
		}

		tx.CommitTime = &commit.Timestamp
		tx.LogDroppedActions()
	case OriginMsgType:
		// Origin messages are received when a transaction originated from a different replication origin (e.g., another subscriber). When dropForeignOrigin is enabled, PostgreSQL's pgoutput plugin filters these at the source using origin='none', so we don't do any filtering client side here.
		origin := p.getOriginMsg()
		p.log.Debug("origin type message was received", slog.String("origin", origin))
	case RelationMsgType:
		// Always process relation messages - we need the schema info even for foreign origin transactions because future local transactions may use the same tables
		relation := p.getRelationMsg()

		p.log.Debug(
			"relation type message was received",
			slog.Any("relation_id", relation.ID),
			slog.String("schema", relation.Namespace),
		)

		if tx.LSN == 0 {
			return fmt.Errorf("commit: %w", errMessageLost)
		}

		rd := RelationData{
			Schema: relation.Namespace,
			Table:  relation.Name,
		}

		for _, rf := range relation.Columns {
			c := Column{
				log:       p.log,
				name:      rf.Name,
				valueType: int(rf.TypeID),
				isKey:     rf.Key,
			}
			rd.Columns = append(rd.Columns, c)
		}

		tx.RelationStore[relation.ID] = rd
	case TypeMsgType:
		// Always process type messages for schema info
		p.log.Debug("type message was received")
	case InsertMsgType:
		insert := p.getInsertMsg()

		p.log.Debug(
			"insert type message was received",
			slog.Any("relation_id", insert.RelationID),
		)

		if tx.ShouldDropAction() {
			return nil
		}

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
		tx.emittedActionCount++
	case UpdateMsgType:
		upd := p.getUpdateMsg()

		p.log.Debug("update type message was received", slog.Any("relation_id", upd.RelationID))

		if tx.ShouldDropAction() {
			return nil
		}

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
		tx.emittedActionCount++
	case DeleteMsgType:
		del := p.getDeleteMsg()

		p.log.Debug(
			"delete type message was received",
			slog.Any("relation_id", del.RelationID),
		)

		if tx.ShouldDropAction() {
			return nil
		}

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
		tx.emittedActionCount++
	case TruncateMsgType:
		truncateMessages := p.getTruncateMessages()
		p.log.Debug("truncate type message was received")

		for _, msg := range truncateMessages {
			if tx.ShouldDropAction() {
				continue
			}

			action, err := tx.CreateActionData(msg.RelationID, nil, nil, ActionKindTruncate)
			if err != nil {
				return fmt.Errorf("truncate action data: %w", err)
			}

			tx.Actions = append(tx.Actions, action)
			tx.emittedActionCount++
		}
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

func (p *BinaryParser) getTruncateMessages() []Truncate {
	count := p.readInt32()
	options := p.readInt8()

	messages := make([]Truncate, count)
	for i := int32(0); i < count; i++ {
		messages[i] = Truncate{
			RelationID: p.readInt32(),
			Cascade:    options&1 == 1,
		}
	}
	return messages
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

func (p *BinaryParser) getOriginMsg() string {
	// Origin message format:
	// Int64 - LSN of the commit on the origin server
	// String - Name of the origin
	_ = p.readInt64() // Skip the LSN, we only need the name
	return p.readString()
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
			data[i] = TupleData{IsUnchangedToastedValue: true}
		case TextDataType:
			vSize := int(p.readInt32())
			data[i] = TupleData{Value: p.buffer.Next(vSize)}
		}
	}

	return data
}
