package main

import (
	"fmt"
	"log"
	"net"
	"time"
)

type ClientHandler struct {
	Id             int
	Conn           net.Conn
	daddy          *Server
	connectionTime time.Time
}

func NewClientHandler(daddy *Server, id int, conn net.Conn) *ClientHandler {
	ch := &ClientHandler{daddy: daddy, Id: id, Conn: conn, connectionTime: time.Now().UTC()}

	return ch
}

func (ch *ClientHandler) Start() {
	if par.LogLevel >= 3 {
		log.Print("Added ", ch, " / ", ch.daddy.NbClients())
	}
	go ch.handle()
}

func (ch *ClientHandler) end() {
	ch.daddy.removeClientHandler(ch)
	if par.LogLevel >= 3 {
		log.Print("Removed ", ch)
	}
}

func (ch *ClientHandler) handle() {
	// When things will go wrong, we will end it properly
	defer ch.end()
	for {
		buffer := make([]byte, 1024)

		_, err := ch.Conn.Read(buffer[0:1])

		if err != nil {
			log.Print("Problem with ", ch.Id, " : ", err)
			break
		}

		if buffer[0] == 0x01 {
			if par.LogLevel >= 5 {
				log.Print("Identification !")
			}
		}
	}
}

func (ch *ClientHandler) String() string {
	return fmt.Sprintf("CH{Id=%d,Conn=%v}", ch.Id, ch.Conn.RemoteAddr())
}
