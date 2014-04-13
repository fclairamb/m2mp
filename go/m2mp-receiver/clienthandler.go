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
	Id               int
	daddy            *Server
	connectionTime   time.Time
	device           *ent.Device
	Conn             *pr.ProtoHandler
	connRecv         chan interface{}
	LogLevel         int
	lastReceivedData time.Time
	lastSentData     time.Time
	ticker           *time.Ticker
	pingCounter      byte
	//connSend         chan interface{}
}

func NewClientHandler(daddy *Server, id int, conn net.Conn) *ClientHandler {
	now := time.Now().UTC()
	ch := &ClientHandler{
		daddy:            daddy,
		Id:               id,
		Conn:             pr.NewProtoHandlerServer(conn),
		connectionTime:   now,
		LogLevel:         9,
		connRecv:         make(chan interface{}, 3),
		ticker:           time.NewTicker(time.Second * 30),
		lastReceivedData: now,
		lastSentData:     now,
		//connSend:       make(chan interface{}, 3),
	}

	return ch
}

func (ch *ClientHandler) Start() {
	if par.LogLevel >= 3 {
		log.Print("Added ", ch, " / ", ch.daddy.NbClients())
	}
	go ch.runRecv()
	go ch.runCoreHandling()
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

func (ch *ClientHandler) runRecv() {
	defer ch.end()
	for {
		msg := ch.Conn.Recv()
		ch.connRecv <- msg
		switch msg.(type) {
		case *pr.EventDisconnected:
			{
				return
			}
		}
	}
}

func (ch *ClientHandler) Send(m interface{}) error {
	err := ch.Conn.Send(m)
	ch.lastSentData = time.Now().UTC()
	return err
}

func (ch *ClientHandler) receivedData() {
	ch.lastReceivedData = time.Now().UTC()
}

func (ch *ClientHandler) considerCurrentStatus() {
	now := time.Now().UTC()
	if ch.LogLevel >= 5 {
		log.Print(ch, " - Considering current status (", now, ")")
	}
	if now.Sub(ch.lastReceivedData) > time.Duration(time.Minute*1) &&
		now.Sub(ch.lastSentData) > time.Duration(time.Second*30) {
		ch.Send(&pr.MessagePingRequest{Data: ch.pingCounter})
		ch.pingCounter += 1
	}
}

func (ch *ClientHandler) runCoreHandling() {
	for {
		select {
		// We
		case msg := <-ch.connRecv:
			{
				switch m := msg.(type) {
				case *pr.MessageDataSimple:
					ch.receivedData()
					ch.handleData(m)
				case *pr.MessageIdentRequest:
					ch.receivedData()
					ch.handleIdentRequest(m)
				case *pr.MessagePingRequest:
					{
						ch.receivedData()
						ch.Conn.Send(&pr.MessagePingResponse{Data: m.Data})
					}
				case *pr.EventDisconnected:
					{
						return
					}
				}

			}
		case <-ch.ticker.C:
			{

			}
		}

		ch.considerCurrentStatus()
	}
}

func (ch *ClientHandler) handleIdentRequest(m *pr.MessageIdentRequest) error {
	var err error
	ch.device, err = ent.NewDeviceByIdentCreate(m.Ident)
	if err != nil {
		log.Print("Problem with ", ch, " : ", err)
	}

	if ch.LogLevel >= 5 {
		log.Print(ch, " --> Identification ", m.Ident, " : ", err == nil)
	}

	// OK
	if err == nil {
		return ch.Send(&pr.MessageIdentResponse{Ok: true})
	} else {
		return ch.Send(&pr.MessageIdentResponse{Ok: false})
	}
}

func (ch *ClientHandler) handleData(msg *pr.MessageDataSimple) error {
	//if par.LogLevel >= 7 {
	//	log.Print(ch, " --> \"", msg.Channel, "\" : ", msg.Data)
	//}

	tokens := strings.SplitN(msg.Channel, ":", 2)

	switch tokens[0] {
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
