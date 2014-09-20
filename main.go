package main

import (
	"log"
	"os"

	"github.com/cmars/gonzodb/gonzo"
)

func die(err error) {
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	os.Exit(0)
}

func main() {
	server, err := gonzo.NewServerAddr("tcp", ":47017")
	if err != nil {
		die(err)
	}
	server.Start()
	err = server.Wait()
	if err != nil {
		die(err)
	}
}
