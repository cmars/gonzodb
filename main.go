package main

import (
	"fmt"
	"log"
	"net"

	"github.com/cmars/gonzodb/gonzo"
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

func handle(c net.Conn) {
	defer c.Close()
	for {
		h := &gonzo.Header{}
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
		case gonzo.OpQuery:
			query, err := gonzo.NewOpQueryMsg(h)
			if err != nil {
				log.Println("query error:", err)
				return
			}
			if query.FullCollectionName == "admin.$cmd" {
				handleAdminCommand(c, query)
				continue
			}
			log.Println("unsupported query:", query)
		//case OpGetMore:
		//case OpDelete:
		//case OpKillCursors:
		default:
			log.Println("unsupported op code:", h.OpCode)
		}
	}
}

func handleAdminCommand(c net.Conn, query *gonzo.OpQueryMsg) {
	var err error

	if v, ok := query.QueryDoc["whatsmyuri"]; ok && v == 1 {
		resp := gonzo.NewOpReplyMsg(query.RequestID, bson.D{{"you", c.RemoteAddr().String()}})
		err = resp.Write(c)
	} else {
		errmsg := fmt.Sprintf("unsupported admin command: %v", query)
		log.Println(errmsg)
		resp := gonzo.NewOpReplyMsg(query.RequestID, bson.D{
			{"errmsg", errmsg},
			{"ok", 0},
		})
		err = resp.Write(c)
	}

	if err != nil {
		log.Println(err)
	}
}
