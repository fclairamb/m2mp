package main

// This is only some draft code. It cannot be used at this stage.

import (
	"bytes"
	"fmt"
	db "github.com/fclairamb/m2mp/go/m2mp-db"
	"net"
)

var initialized = false

func Fuzz(data []byte) int {

	if !initialized {
		par = LoadConfig() // main's reference

		log.Debug("Connecting to DB...")
		if err := db.NewSessionSimple(par.Db.Keyspace); err != nil {
			log.Fatal("DB error: ", err)
		}

		server = NewServer() // main's reference

		if err := server.Start(); err != nil {
			log.Fatal("Server error: ", err)
		}
	}

	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", par.Net.ListenPort))
	if err != nil {
		return -1
	}

	lines := bytes.Split(data, []byte("\n"))
	for _, line := range lines {
		conn.Write(line)
	}

	return 0
}
