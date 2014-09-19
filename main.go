package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"

	"gopkg.in/mgo.v2/bson"
)

func die(err error) {
	panic(err)
}

func main() {
	ln, err := net.Listen("tcp", ":47017")
	if err != nil {
		die(err)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println("accept: %v", err)
			continue
		}
		go handle(conn)
	}
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

type msg struct {
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

var errTruncMsg = fmt.Errorf("truncated message")

func (m *msg) Read(r io.Reader) error {
	buf := make([]byte, 4)
	_, err := r.Read(buf)
	if err != nil {
		return err
	}
	var ok bool
	m.Length, _, ok = readInt32(buf)
	if !ok {
		return errTruncMsg
	}

	buf = make([]byte, m.Length-4)
	_, err = r.Read(buf)
	if err != nil {
		return err
	}

	if m.RequestID, buf, ok = readInt32(buf); !ok {
		return errTruncMsg
	}
	if m.ResponseTo, buf, ok = readInt32(buf); !ok {
		return errTruncMsg
	}

	opCode, buf, ok := readInt32(buf)
	if !ok {
		return errTruncMsg
	} else {
		m.OpCode = OpCode(opCode)
	}

	m.Contents = buf
	return nil
}

type queryFlags int32

const (
	queryFlagTailableCursor  = 1 << 1
	queryFlagSlaveOk         = 1 << 2
	queryFlagOplogReplay     = 1 << 3
	queryFlagNoCursorTimeout = 1 << 4
	queryFlagAwaitData       = 1 << 5
	queryFlagExhaust         = 1 << 6
	queryFlagPartial         = 1 << 7
)

type queryMsg struct {
	// Bit vector of query options.
	flags queryFlags
	// "dbname.collectionname"
	fullCollectionName string
	// number of documents to skip
	numberToSkip int32
	// number of documents to return in the first OP_REPLY batch
	numberToReturn int32
	// query object.  See below for details.
	query map[string]interface{}
	// Optional. Selector indicating the fields to return.
	returnFieldsSelector map[string]int
}

func (m *queryMsg) SetBytes(b []byte) error {
	flags, b, ok := readInt32(b)
	if !ok {
		return errTruncMsg
	} else {
		m.flags = queryFlags(flags)
	}

	if m.fullCollectionName, b, ok = readCstring(b); !ok {
		return errTruncMsg
	}
	if m.numberToSkip, b, ok = readInt32(b); !ok {
		return errTruncMsg
	}
	if m.numberToReturn, b, ok = readInt32(b); !ok {
		return errTruncMsg
	}

	var err error
	m.query = make(map[string]interface{})
	if b, err = readBsonDoc(b, &m.query); err != nil {
		return err
	}
	if len(b) > 0 {
		m.returnFieldsSelector = make(map[string]int)
		if b, err = readBsonDoc(b, &m.returnFieldsSelector); err != nil {
			return err
		}
	}
	return nil
}

func handle(c net.Conn) {
	defer c.Close()
	msg := &msg{}
	err := msg.Read(c)
	if err != nil {
		log.Printf("header read: %v", err)
		return
	}
	log.Printf("got: %v", msg)
	switch msg.OpCode {
	//case OpReply:
	//case OpMsg:
	//case OpUpdate:
	//case OpInsert:
	case OpQuery:
		qmsg := &queryMsg{}
		if err = qmsg.SetBytes(msg.Contents); err != nil {
			log.Printf("query error: %v", err)
		} else {
			log.Printf("query: %v", qmsg)
		}
	//case OpGetMore:
	//case OpDelete:
	//case OpKillCursors:
	default:
		log.Printf("unsupported op code: %v", msg)
	}
}
