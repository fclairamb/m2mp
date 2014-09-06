package main

import (
	"fmt"
	m2common "github.com/fclairamb/m2mp/go/m2mp-common"
	m2log "github.com/fclairamb/m2mp/go/m2mp-log"
	msg "github.com/fclairamb/m2mp/go/m2mp-messaging"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Server struct {
	sync.RWMutex
	listener      net.Listener
	clients       *m2common.Registry
	clientCounter int // To allocate an new ID to each client's connection
	msg           *msg.Client
}

func NewServer() *Server {
	s := &Server{clients: m2common.NewRegistry()}
	return s
}

func (s *Server) handleIncomingConnection(c net.Conn) {
	// We just lock it for the whole function (beter keep things simple)
	s.Lock()
	defer s.Unlock()

	s.clientCounter += 1

	id := s.clientCounter
	ch := NewClientHandler(s, id, c)

	primary := fmt.Sprintf("%d", id)
	s.clients.AddPrimaryKey(primary, ch)
	s.clients.AddSecondaryKey(primary, fmt.Sprintf("source=%s", c.RemoteAddr().String()))

	go ch.Start()
}

func (s *Server) acceptIncomingConnections() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			log.Error("Error while listening: %v", err)
			time.Sleep(time.Millisecond * 50)
			continue
		}
		s.handleIncomingConnection(conn)
		log.Debug("Number of clients: %d", s.NbClients())
	}
}

func (s *Server) clientIdentified(ch *ClientHandler) {
	s.clients.AddSecondaryKey(fmt.Sprintf("%d", ch.Id), fmt.Sprintf("device_id=%s", ch.device.Id()))
}

func (s *Server) NbClients() int {
	return s.clients.NbPrimary()
}

func (s *Server) removeClientHandler(ch *ClientHandler) {
	s.clients.RemovePrimaryKey(fmt.Sprintf("%d", ch.Id))
}

func (s *Server) listen() error {
	var err error = nil
	s.listener, err = net.Listen("tcp", fmt.Sprintf(":%d", par.Net.ListenPort))
	if err != nil {
		return err
	}

	if m2log.Level >= 2 {
		log.Notice("Listening on %s...", s.listener.Addr())
	}

	go s.acceptIncomingConnections()

	return err
}

func (s *Server) handleMessageForConnectionId(m *msg.JsonWrapper, connectionId int) {
	if clientHandler := s.clients.Primary(fmt.Sprintf("%d", connectionId)).(*ClientHandler); clientHandler != nil {
		clientHandler.HandleMQMessage(m)
	} else {
		log.Warning("Could not find connection_id=%d", connectionId)
	}
}

func (s *Server) handleMessageForDeviceId(m *msg.JsonWrapper, deviceId string) {
	c := 0
	for _, i := range s.clients.Secondary(fmt.Sprintf("device_id=%s", deviceId)) {
		ch := i.(*ClientHandler)
		ch.HandleMQMessage(m)

		c += 1
	}

	if c == 0 {
		log.Warning("Could not find device_id=\"%s\"", deviceId)
	}
}

func (s *Server) handleMessage(m *msg.JsonWrapper) {
	log.Debug("Handling %s", m.String())

	call := m.Call()

	array := strings.Split(m.To(), ";")

	connectionId := 0
	deviceId := ""

	// We parse alll the destination parameters
	for _, v := range array[1:] {
		spl := strings.SplitN(v, "=", 2)
		key := spl[0]
		value := ""
		if len(spl) > 1 {
			value = spl[1]
		}

		// We're actually only intesterested by this one at this point
		if key == "connection_id" {
			id, _ := strconv.ParseInt(value, 10, 0)
			connectionId = int(id)
		} else if key == "device_id" {
			deviceId = value
		}
	}

	if connectionId != 0 {
		s.handleMessageForConnectionId(m, connectionId)
	} else if deviceId != "" {
		s.handleMessageForDeviceId(m, deviceId)
	} else {
		switch call {
		case "quit":
			{
				log.Warning("Quit received! Not handling it right now!")
			}
		}
	}
}

func (s *Server) runMessaging() error {
	for {
		m := <-s.msg.Recv
		if m == nil {
			log.Critical("Stopping messaging...")
			return nil
		}
		s.handleMessage(m)
	}
}

func (s *Server) Start() error {
	err := s.listen()

	if err == nil {
		s.msg, err = msg.NewClient(par.Mq.Topic, par.Mq.Channel)
	}

	if err == nil {
		log.Debug("Opening MQ %s", par.Mq.Server)
		err = s.msg.Start(par.Mq.Server)
	}

	if err == nil {
		go s.runMessaging()
	}

	if err == nil {
		m := msg.NewMessage(msg.TOPIC_GENERAL_EVENTS, "new_receiver")
		m.Set("tcp_port", par.Net.ListenPort)
		s.SendMessage(m)
	}

	return err
}

func (s *Server) SendMessage(m *msg.JsonWrapper) error {
	return s.msg.Publish(m)
}

func (s *Server) Close() {

}
