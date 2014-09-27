package gonzo

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"gopkg.in/mgo.v2/bson"
	"gopkg.in/tomb.v2"
)

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
	s := &Server{ln: ln}
	s.Backend = NewMemoryBackend(&s.t)
	return s
}

func (s *Server) Start() {
	s.t.Go(s.run)
	log.Printf("gonzodb running pid=%d addr=%q", os.Getpid(), s.ln.Addr())
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
	s.ln.Close()
	return nil
}

func (s *Server) Stop() {
	s.t.Kill(nil)
	s.t.Wait()
}

func errReply(err error) bson.D {
	if err != nil {
		return bson.D{
			{"errmsg", err.Error()},
			{"ok", 0},
		}
	}
	return markOk(nil)
}

func markOk(msg bson.D) bson.D {
	return append(msg, bson.DocElem{"ok", 1})
}

func respDoc(w io.Writer, requestID int32, docs ...interface{}) error {
	resp := NewOpReplyMsg(requestID, docs...)
	return resp.Write(w)
}

func respError(w io.Writer, requestID int32, err error) error {
	if err != nil {
		log.Println(err)
	}
	resp := NewOpReplyMsg(requestID, errReply(err))
	return resp.Write(w)
}

func (s *Server) handle(c net.Conn) {
	defer c.Close()
	for {
		select {
		case <-s.t.Dying():
			return
		default:
		}

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
		case OpInsert:
			insert, err := NewOpInsertMsg(h)
			if err != nil {
				respError(c, h.RequestID, err)
				return
			}
			s.Backend.HandleInsert(c, insert)
			continue
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
