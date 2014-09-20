package gonzo

import (
	"fmt"
	"log"
	"net"

	"gopkg.in/mgo.v2/bson"
	"gopkg.in/tomb.v2"
)

type Backend interface {
	HandleQuery(c net.Conn, query *OpQueryMsg)
}

type MemoryCollection struct {
	contents map[string]bson.D
}

type MemoryDB struct {
	name        string
	collections map[string]*MemoryCollection
}

type MemoryBackend struct {
	dbs map[string]*MemoryDB
	t   *tomb.Tomb
}

func NewMemoryBackend(t *tomb.Tomb) *MemoryBackend {
	return &MemoryBackend{
		dbs: make(map[string]*MemoryDB),
		t:   t,
	}
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
	if _, ok := query.Doc["whatsmyuri"]; ok {
		return respDoc(c, query.RequestID, bson.D{{"you", c.RemoteAddr().String()}})

	} else if logName, ok := query.Doc["getLog"]; ok {
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

	} else if _, ok := query.Doc["replSetGetStatus"]; ok {
		return respError(c, query.RequestID, fmt.Errorf("not running with --replSet"))

	} else if _, ok := query.Doc["shutdown"]; ok {
		log.Println("shutdown requested")
		b.t.Kill(nil)
		return c.Close()
	}
	return respError(c, query.RequestID, fmt.Errorf("unsupported admin command: %v", query))
}
