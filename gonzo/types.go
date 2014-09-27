package gonzo

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"gopkg.in/mgo.v2/bson"
)

var requestIDLock sync.Mutex
var requestID = int32(0)

func newRequestID() int32 {
	requestIDLock.Lock()
	defer requestIDLock.Unlock()
	requestID++
	return requestID
}

type OpCode int32

var (
	OpReply       = OpCode(1)
	OpMsg         = OpCode(1000)
	OpUpdate      = OpCode(2001)
	OpInsert      = OpCode(2002)
	OpQuery       = OpCode(2004)
	OpGetMore     = OpCode(2005)
	OpDelete      = OpCode(2006)
	OpKillCursors = OpCode(2007)
)

type Header struct {
	Length     int32
	RequestID  int32
	ResponseTo int32
	OpCode     OpCode
	Contents   []byte
}

func readInt32(b []byte) (int32, []byte, bool) {
	if len(b) >= 4 {
		return int32(binary.LittleEndian.Uint32(b[:4])), b[4:], true
	}
	return 0, nil, false
}

func readCstring(b []byte) (string, []byte, bool) {
	for i := 0; i < len(b); i++ {
		if b[i] == 0 {
			return string(b[:i]), b[i+1:], true
		}
	}
	return "", nil, false
}

func readBsonDoc(b []byte, out interface{}) ([]byte, error) {
	l, _, ok := readInt32(b)
	if !ok {
		return nil, errTruncMsg
	}
	if int(l) > len(b) {
		l = int32(len(b))
	}
	if err := bson.Unmarshal(b[:l], out); err != nil {
		return nil, err
	}
	return b[l:], nil
}

func writeInt32(w io.Writer, b []byte, v int32) error {
	binary.LittleEndian.PutUint32(b, uint32(v))
	_, err := w.Write(b[:4])
	return err
}

func writeInt64(w io.Writer, b []byte, v int64) error {
	binary.LittleEndian.PutUint64(b, uint64(v))
	_, err := w.Write(b[:8])
	return err
}

var errTruncMsg = fmt.Errorf("truncated message")

func (h *Header) Read(r io.Reader) error {
	buf := make([]byte, 4)
	_, err := r.Read(buf)
	if err != nil {
		return err
	}
	var ok bool
	h.Length, _, ok = readInt32(buf)
	if !ok {
		return errTruncMsg
	}

	buf = make([]byte, h.Length-4)
	_, err = r.Read(buf)
	if err != nil {
		return err
	}

	if h.RequestID, buf, ok = readInt32(buf); !ok {
		return errTruncMsg
	}
	if h.ResponseTo, buf, ok = readInt32(buf); !ok {
		return errTruncMsg
	}

	opCode, buf, ok := readInt32(buf)
	if !ok {
		return errTruncMsg
	} else {
		h.OpCode = OpCode(opCode)
	}

	h.Contents = buf
	return nil
}

func (h *Header) Write(w io.Writer) error {
	b := make([]byte, 4)
	var err error
	if err = writeInt32(w, b, h.Length); err != nil {
		return err
	}
	if err = writeInt32(w, b, h.RequestID); err != nil {
		return err
	}
	if err = writeInt32(w, b, h.ResponseTo); err != nil {
		return err
	}
	if err = writeInt32(w, b, int32(h.OpCode)); err != nil {
		return err
	}
	_, err = w.Write(h.Contents)
	return err
}

type QueryFlags int32

const (
	QueryFlagTailableCursor  = 1 << 1
	QueryFlagSlaveOk         = 1 << 2
	QueryFlagOplogReplay     = 1 << 3
	QueryFlagNoCursorTimeout = 1 << 4
	QueryFlagAwaitData       = 1 << 5
	QueryFlagExhaust         = 1 << 6
	QueryFlagPartial         = 1 << 7
)

type OpQueryMsg struct {
	*Header

	// Bit vector of query options.
	Flags QueryFlags

	// "dbname.collectionname"
	FullCollectionName string

	// number of documents to skip
	NumberToSkip int32

	// number of documents to return in the first OP_REPLY batch
	NumberToReturn int32

	// query object.  See below for details.
	Doc bson.D

	// Optional. Selector indicating the fields to return.
	ReturnFieldsSelector bson.D
}

func (m *OpQueryMsg) Command() (cmd string, arg interface{}) {
	for _, kv := range m.Doc {
		return kv.Name, kv.Value
	}
	return "", nil
}

func (m *OpQueryMsg) Get(key string) (interface{}, bool) {
	for _, kv := range m.Doc {
		if kv.Name == key {
			return kv.Value, true
		}
	}
	return nil, false
}

func NewOpQueryMsg(h *Header) (*OpQueryMsg, error) {
	m := &OpQueryMsg{Header: h}
	b := h.Contents

	flags, b, ok := readInt32(b)
	if !ok {
		return nil, errTruncMsg
	} else {
		m.Flags = QueryFlags(flags)
	}

	if m.FullCollectionName, b, ok = readCstring(b); !ok {
		return nil, errTruncMsg
	}
	if m.NumberToSkip, b, ok = readInt32(b); !ok {
		return nil, errTruncMsg
	}
	if m.NumberToReturn, b, ok = readInt32(b); !ok {
		return nil, errTruncMsg
	}

	var err error
	if b, err = readBsonDoc(b, &m.Doc); err != nil {
		return nil, err
	}
	if len(b) > 0 {
		if b, err = readBsonDoc(b, &m.ReturnFieldsSelector); err != nil {
			return nil, err
		}
	}
	return m, nil
}

type InsertFlags int32

const (
	InsertFlagContinueOnError = 1 << 0
)

type OpInsertMsg struct {
	*Header

	// Bit vector of query options.
	Flags InsertFlags

	// "dbname.collectionname"
	FullCollectionName string

	// one or more documents to insert into the collection
	Docs []bson.M
}

func NewOpInsertMsg(h *Header) (*OpInsertMsg, error) {
	m := &OpInsertMsg{Header: h}
	b := h.Contents

	flags, b, ok := readInt32(b)
	if !ok {
		return nil, errTruncMsg
	} else {
		m.Flags = InsertFlags(flags)
	}

	if m.FullCollectionName, b, ok = readCstring(b); !ok {
		return nil, errTruncMsg
	}

	var err error
	for len(b) > 0 {
		mdoc := make(bson.M)
		if b, err = readBsonDoc(b, mdoc); err != nil {
			return nil, err
		}
		m.Docs = append(m.Docs, mdoc)
	}
	return m, nil
}

type OpReplyMsg struct {
	*Header

	// bit vector - see details below
	ResponseFlags int32

	// cursor id if client needs to do get more's
	CursorID int64

	// where in the cursor this reply is starting
	StartingFrom int32

	// number of documents in the reply
	NumberReturned int32

	// documents
	Docs []interface{}

	responseTo int32
}

func NewOpReplyMsg(responseTo int32, docs ...interface{}) *OpReplyMsg {
	return &OpReplyMsg{
		Header: &Header{
			RequestID:  newRequestID(),
			ResponseTo: responseTo,
			OpCode:     OpReply,
		},
		NumberReturned: int32(len(docs)),
		Docs:           docs,
	}
}

func (m *OpReplyMsg) Write(w io.Writer) error {
	b := make([]byte, 8)
	var out bytes.Buffer

	writeInt32(&out, b, m.ResponseFlags)
	writeInt64(&out, b, m.CursorID)
	writeInt32(&out, b, m.StartingFrom)
	writeInt32(&out, b, m.NumberReturned)

	for _, doc := range m.Docs {
		b, err := bson.Marshal(doc)
		if err != nil {
			return err
		}
		_, err = out.Write(b)
		if err != nil {
			return err
		}
	}
	m.Header.Contents = out.Bytes()
	m.Header.Length = int32(out.Len() + 16)
	return m.Header.Write(w)
}

type UpdateFlags int32

const (
	UpdateFlagUpsert      = 1 << 0
	UpdateFlagMultiUpdate = 1 << 1
)

type OpUpdateMsg struct {
	*Header

	zero int32

	// "dbname.collectionname"
	FullCollectionName string

	// Bit vector of update options.
	Flags UpdateFlags

	// Selector is the query to select the document.
	Selector bson.M

	// Update is the document update specification.
	Update bson.M
}

func NewOpUpdateMsg(h *Header) (*OpUpdateMsg, error) {
	m := &OpUpdateMsg{Header: h}
	b := h.Contents

	var ok bool

	if m.zero, b, ok = readInt32(b); !ok {
		return nil, errTruncMsg
	}

	if m.FullCollectionName, b, ok = readCstring(b); !ok {
		return nil, errTruncMsg
	}

	flags, b, ok := readInt32(b)
	if !ok {
		return nil, errTruncMsg
	} else {
		m.Flags = UpdateFlags(flags)
	}

	var err error
	if len(b) > 0 {
		m.Selector = make(bson.M)
		if b, err = readBsonDoc(b, m.Selector); err != nil {
			return nil, err
		}
	} else {
		return nil, errTruncMsg
	}

	if len(b) > 0 {
		m.Update = make(bson.M)
		if b, err = readBsonDoc(b, m.Update); err != nil {
			return nil, err
		}
	} else {
		return nil, errTruncMsg
	}

	return m, nil
}
