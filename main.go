package main

import "github.com/cmars/gonzodb/gonzo"

func die(err error) {
	panic(err)
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
