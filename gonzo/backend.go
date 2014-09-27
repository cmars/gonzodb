package gonzo

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"strings"

	"gopkg.in/mgo.v2/bson"
	"gopkg.in/tomb.v2"
)

type Backend interface {
	HandleQuery(c net.Conn, query *OpQueryMsg)
	HandleInsert(c net.Conn, insert *OpInsertMsg)

	DBNames() []string
	DB(name string) DB
}

type DB interface {
	Empty() bool
	CNames() []string
	C(name string) Collection

	LastError() error
	SetLastError(err error)
}

type Collection interface {
	Id(id string) interface{}
	All() []interface{}
	Match(pattern bson.M) []interface{}
	Insert(item interface{}) error
}

type MemoryCollection struct {
	docs map[string]bson.M
}

type MemoryDB struct {
	collections map[string]*MemoryCollection
	lastErr     error
}

func NewMemoryDB() *MemoryDB {
	return &MemoryDB{collections: make(map[string]*MemoryCollection)}
}

func (db *MemoryDB) Empty() bool {
	return len(db.collections) == 0
}

func (db *MemoryDB) CNames() (result []string) {
	for cname, _ := range db.collections {
		result = append(result, cname)
	}
	return result
}

func (db *MemoryDB) C(name string) Collection {
	result, ok := db.collections[name]
	if !ok {
		result = &MemoryCollection{docs: make(map[string]bson.M)}
		db.collections[name] = result
	}
	return result
}

func (db *MemoryDB) LastError() error       { return db.lastErr }
func (db *MemoryDB) SetLastError(err error) { db.lastErr = err }

func (c *MemoryCollection) Id(id string) interface{} {
	for _, item := range c.docs {
		mitem := item
		if match, ok := mitem["_id"]; ok && match == id {
			return item
		}
	}
	return nil
}

func (c *MemoryCollection) All() (result []interface{}) {
	for _, doc := range c.docs {
		result = append(result, doc)
	}
	return result
}

func (c *MemoryCollection) Match(pattern bson.M) (result []interface{}) {
	for _, doc := range c.docs {
		if isPatternMatch(doc, pattern) {
			result = append(result, doc)
		}
	}
	return result
}

func isPatternMatch(doc, pattern bson.M) bool {
	for matchKey, matchValue := range pattern {
		value, ok := doc[matchKey]
		if !ok || matchValue != value {
			return false
		}
	}
	return true
}

func (c *MemoryCollection) Insert(doc interface{}) error {
	mdoc, ok := doc.(bson.M)
	if !ok {
		return fmt.Errorf("cannot insert instance of this type: %v", doc)
	}
	var idStr string
	if id, ok := mdoc["_id"]; ok {
		idStr = id.(fmt.Stringer).String()
	} else {
		oid := bson.NewObjectId()
		mdoc["_id"] = oid
		idStr = oid.String()
	}
	c.docs[idStr] = mdoc
	return nil
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

func (b *MemoryBackend) DBNames() (result []string) {
	for dbname, _ := range b.dbs {
		result = append(result, dbname)
	}
	return result
}

func (b *MemoryBackend) DB(name string) DB {
	result, ok := b.dbs[name]
	if !ok {
		result = NewMemoryDB()
		b.dbs[name] = result
	}
	return result
}

func (b *MemoryBackend) HandleQuery(c net.Conn, query *OpQueryMsg) {
	if query.FullCollectionName == "admin.$cmd" {
		err := b.handleAdminCommand(c, query)
		if err != nil {
			log.Println(err)
		}
		return
	}

	fields := strings.SplitN(query.FullCollectionName, ".", 2)
	if len(fields) < 2 {
		respError(c, query.RequestID, fmt.Errorf("malformed full collection name %q", query.FullCollectionName))
		return
	}
	dbname, cname := fields[0], fields[1]
	if strings.HasPrefix(cname, "system.") {
		b.handleSystemQuery(c, query, dbname, cname)
		return
	}
	db := b.DB(dbname)
	if cname == "$cmd" {
		err := b.handleDBCommand(c, db, query)
		if err != nil {
			log.Println(err)
		}
		return
	}
	coll := db.C(cname)

	var results []interface{}
	if match, ok := query.Get("$query"); ok {
		if matchDoc, ok := match.(bson.M); ok {
			results = append(results, coll.Match(matchDoc)...)
		} else {
			respError(c, query.RequestID, fmt.Errorf("unexpected $query type %v", match))
			return
		}
	} else if len(query.Doc) == 0 {
		results = append(results, coll.All()...)
	} else {
		results = append(results, coll.Match(query.Doc.Map())...)
	}
	respDoc(c, query.RequestID, results...)
}

func (b *MemoryBackend) HandleInsert(c net.Conn, insert *OpInsertMsg) {
	if strings.HasPrefix(insert.FullCollectionName, "admin.") {
		respError(c, insert.RequestID, fmt.Errorf("insert not supported on admin.*"))
		return
	}

	fields := strings.SplitN(insert.FullCollectionName, ".", 2)
	if len(fields) < 2 {
		respError(c, insert.RequestID, fmt.Errorf("malformed full collection name %q", insert.FullCollectionName))
		return
	}
	dbname, cname := fields[0], fields[1]
	if strings.HasPrefix(cname, "system.") {
		respError(c, insert.RequestID, fmt.Errorf("insert not supported on %s.system.*", dbname))
		return
	}
	db := b.DB(dbname)
	coll := db.C(cname)
	for _, doc := range insert.Docs {
		err := coll.Insert(doc)
		db.SetLastError(err)
		if err != nil {
			log.Println(err)
			return
		}
	}
	//respDoc(c, insert.RequestID, markOk(bson.D{}))
	// TODO: if err != nil { ... set last error ... }
}

// TODO: implement Collection interface instead
func (b *MemoryBackend) handleSystemQuery(c net.Conn, query *OpQueryMsg, dbname, cname string) {
	log.Println("system query:", dbname, cname, query.Doc)
	switch cname {
	case "system.namespaces":
		var result []interface{}
		for _, name := range b.DB(dbname).CNames() {
			result = append(result, bson.D{{"name", name}})
		}
		respDoc(c, query.RequestID, result...)
		return
	}
	respError(c, query.RequestID, fmt.Errorf(
		"unsupported system query on %s: %v", query.FullCollectionName, query.Doc))
}

func (b *MemoryBackend) handleDBCommand(c net.Conn, db DB, query *OpQueryMsg) error {
	switch cmd, _ := query.Command(); cmd {
	case "getLastError":
		fallthrough
	case "getlasterror":
		return respError(c, query.RequestID, db.LastError())
	}
	return respError(c, query.RequestID, fmt.Errorf("unsupported db command: %v", query))
}

func (b *MemoryBackend) handleAdminCommand(c net.Conn, query *OpQueryMsg) error {
	switch cmd, arg := query.Command(); cmd {
	case "getLog":
		var msg bson.D
		switch logName := arg.(string); logName {
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
	case "listDatabases":
		var dbinfos []bson.D
		for _, dbname := range b.DBNames() {
			dbinfos = append(dbinfos, bson.D{
				{"name", dbname},
				{"empty", b.DB(dbname).Empty()},
			})
		}
		return respDoc(c, query.RequestID, markOk(bson.D{
			{"databases", dbinfos},
		}))
	case "replSetGetStatus":
		return respError(c, query.RequestID, fmt.Errorf("not running with --replSet"))
	case "shutdown":
		log.Println("shutdown requested")
		b.t.Kill(nil)
		return c.Close()
	case "whatsmyuri":
		return respDoc(c, query.RequestID, bson.D{{"you", c.RemoteAddr().String()}})
	case "ismaster":
		return respDoc(c, query.RequestID, markOk(bson.D{{"ismaster", true}}))
	case "getnonce":
		nonce := make([]byte, 32)
		_, err := rand.Reader.Read(nonce[:])
		if err != nil {
			return err
		}
		return respDoc(c, query.RequestID, markOk(bson.D{
			{"nonce", hex.EncodeToString(nonce)},
		}))
	case "ping":
		return respDoc(c, query.RequestID, markOk(nil))
	}
	return respError(c, query.RequestID, fmt.Errorf("unsupported admin command: %v", query))
}
