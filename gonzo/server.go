package gonzo

import (
	"fmt"
	"io"
	"log"
	"net"

	"gopkg.in/mgo.v2/bson"
	"gopkg.in/tomb.v2"
)

type Backend interface {
	HandleQuery(c net.Conn, query *OpQueryMsg)
}

type MemoryBackend struct {
}

func (b *MemoryBackend) HandleQuery(c net.Conn, query *OpQueryMsg) {
	if query.FullCollectionName == "admin.$cmd" {
		err := b.handleAdminCommand(c, query)
		if err != nil {
			log.Println(err)
		}
		return
	}
	respError(c, query.RequestID, fmt.Errorf("unsupported query: %v", query))
}

func (b *MemoryBackend) handleAdminCommand(c net.Conn, query *OpQueryMsg) error {
	if v, ok := query.QueryDoc["whatsmyuri"]; ok && v == 1 {
		return respDoc(c, query.RequestID, bson.D{{"you", c.RemoteAddr().String()}})
	} else if logName, ok := query.QueryDoc["getLog"]; ok {
		var msg bson.D
		switch logName {
		case "*":
			msg = markOk(bson.D{{"names", []string{"startupWarnings"}}})
		case "startupWarnings":
			msg = markOk(bson.D{
				{"totalLinesWritten", 0},
				{"log", []string{}},
			})
		default:
			msg = errReply(fmt.Errorf("log not found: %q", logName))
		}
		return respDoc(c, query.RequestID, msg)
	} else if _, ok := query.QueryDoc["replSetGetStatus"]; ok {
		return respError(c, query.RequestID, fmt.Errorf("not running with --replSet"))
	}
	return respError(c, query.RequestID, fmt.Errorf("unsupported admin command: %v", query))
}

type Server struct {
	Backend Backend

	ln net.Listener
	t  tomb.Tomb
}

func NewServerAddr(netname, addr string) (*Server, error) {
	ln, err := net.Listen(netname, addr)
	if err != nil {
		return nil, err
	}
	return NewServer(ln), nil
}

func NewServer(ln net.Listener) *Server {
	return &Server{
		Backend: &MemoryBackend{},
		ln:      ln,
	}
}

func (s *Server) Start() {
	s.t.Go(s.run)
}

func (s *Server) Wait() error {
	return s.t.Wait()
}

func (s *Server) run() error {
	s.t.Go(func() (err error) {
		var conn net.Conn
		defer s.t.Kill(err)
		for {
			conn, err = s.ln.Accept()
			if err != nil {
				return
			}
			s.t.Go(func() error { s.handle(conn); return nil })
		}
	})
	<-s.t.Dying()
	s.t.Kill(nil)
	return nil
}

func (s *Server) Stop() {
	s.t.Kill(nil)
	s.t.Wait()
}

func errReply(err error) bson.D {
	return bson.D{
		{"errmsg", err.Error()},
		{"ok", 0},
	}
}

func markOk(msg bson.D) bson.D {
	return append(msg, bson.DocElem{"ok", 1})
}

func respDoc(w io.Writer, requestID int32, doc bson.D) error {
	resp := NewOpReplyMsg(requestID, doc)
	return resp.Write(w)
}

func respError(w io.Writer, requestID int32, err error) error {
	log.Println(err)
	resp := NewOpReplyMsg(requestID, errReply(err))
	return resp.Write(w)
}

func (s *Server) handle(c net.Conn) {
	defer c.Close()
	for {
		h := &Header{}
		err := h.Read(c)
		if err != nil {
			log.Printf("header read: %v", err)
			return
		}
		switch h.OpCode {
		//case OpReply:
		//case OpMsg:
		//case OpUpdate:
		//case OpInsert:
		case OpQuery:
			query, err := NewOpQueryMsg(h)
			if err != nil {
				respError(c, h.RequestID, err)
				return
			}
			s.Backend.HandleQuery(c, query)
			continue
		//case OpGetMore:
		//case OpDelete:
		//case OpKillCursors:
		default:
			err := fmt.Errorf("unsupported op code %d", h.OpCode)
			respError(c, h.RequestID, err)
			return
		}
	}
}
