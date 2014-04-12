package main

import (
	"fmt"
	ent "github.com/fclairamb/m2mp/go/m2mp-db/entities"
	pr "github.com/fclairamb/m2mp/go/m2mp-protocol"
	"log"
	"net"
	"strings"
	"time"
)

type ClientHandler struct {
	Id             int
	daddy          *Server
	connectionTime time.Time
	device         *ent.Device
	Conn           *pr.ProtoHandler
	LogLevel       int
}

func NewClientHandler(daddy *Server, id int, conn net.Conn) *ClientHandler {
	ch := &ClientHandler{
		daddy:          daddy,
		Id:             id,
		Conn:           pr.NewProtoHandlerServer(conn),
		connectionTime: time.Now().UTC(),
		LogLevel:       9,
	}

	return ch
}

func (ch *ClientHandler) Start() {
	if par.LogLevel >= 3 {
		log.Print("Added ", ch, " / ", ch.daddy.NbClients())
	}
	go ch.handleConnection()
}

func (ch *ClientHandler) Disconnect() error {
	return ch.Conn.Conn.Close()
}

func (ch *ClientHandler) end() {
	ch.daddy.removeClientHandler(ch)
	if par.LogLevel >= 3 {
		log.Print("Removed ", ch)
	}
}

func (ch *ClientHandler) handleConnection() {
	defer ch.end()
	for {
		msg := ch.Conn.Recv()
		switch m := msg.(type) {
		case *pr.MessageDataSimple:
			ch.handleData(m)
		case *pr.MessageIdentRequest:
			{
				var err error
				if ch.LogLevel >= 5 {
					log.Print(ch, " --> Identification: ", m.Ident)
				}
				ch.device, err = ent.NewDeviceByIdentCreate(m.Ident)
				if err != nil {
					log.Print("Problem with ", ch, " : ", err)
				}

				// OK
				if err == nil {
					ch.Conn.Send(&pr.MessageIdentResponse{Ok: true})
				} else {
					ch.Conn.Send(&pr.MessageIdentResponse{Ok: false})
				}
			}

		case *pr.MessagePingRequest:
			{
				ch.Conn.Send(&pr.MessagePingResponse{Data: m.Data})
			}
		case *pr.EventDisconnected:
			{
				break
			}
		}
	}
}

func (ch *ClientHandler) handleData(msg *pr.MessageDataSimple) error {
	//if par.LogLevel >= 7 {
	//	log.Print(ch, " --> \"", msg.Channel, "\" : ", msg.Data)
	//}

	tokens := strings.SplitN(msg.Channel, ":", 2)

	switch tokens[0] {
	case "_set": // settings have their own logic
		{

		}

	case "_cmd": // commands have their own logic
		{

		}
	case "_sta": // status have their own logic
		{

		}
	case "echo": // echo is just replied
		{
			ch.Conn.Send(msg)
		}

	case "sen": // sensor is just stored
		{
			if ch.device != nil {
				ch.device.SaveTSTime(msg.Channel, time.Now().UTC(), string(msg.Data))
			}
		}
	}

	return nil
}

func (ch *ClientHandler) handleDataArray(msg *pr.MessageDataArray) error {
	if par.LogLevel >= 7 {
		log.Print(ch, " --> \"", msg.Channel, "\" : ", msg.Data)
	}

	return nil
}

func (ch *ClientHandler) String() string {
	return fmt.Sprintf("CH{Id=%d,Conn=%v}", ch.Id, ch.Conn)
}
