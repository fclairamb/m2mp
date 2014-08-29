package main

import (
	"fmt"
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
	clients       map[int]*ClientHandler
	clientCounter int // To allocate an new ID to each client's connection
	msg           *msg.Client
}

func NewServer() *Server {
	s := &Server{clients: make(map[int]*ClientHandler)}
	return s
}

func (s *Server) handleIncomingConnection(c net.Conn) {
	// We just lock it for the whole function (beter keep things simple)
	s.Lock()
	defer s.Unlock()

	s.clientCounter += 1

	id := s.clientCounter
	ch := NewClientHandler(s, id, c)
	s.clients[id] = ch

	go ch.Start()
}

func (s *Server) acceptIncomingConnections() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			log.Error("Error while listening: ", err)
			time.Sleep(time.Millisecond * 50)
			continue
		}
		s.handleIncomingConnection(conn)
		log.Debug("Number of clients: %d", s.NbClients())
	}
}

func (s *Server) NbClients() int {
	s.RLock()
	defer s.RUnlock()

	return len(s.clients)
}

func (s *Server) removeClientHandler(ch *ClientHandler) {
	s.Lock()
	defer s.Unlock()

	delete(s.clients, ch.Id)
}

func (s *Server) listen() error {
	var err error = nil
	s.listener, err = net.Listen("tcp", fmt.Sprintf(":%d", par.ListenPort))
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
	s.Lock()
	defer s.Unlock()

	if clientHandler := s.clients[connectionId]; clientHandler != nil {
		clientHandler.HandleMQMessage(m)
	} else {
		log.Warning("Could not find connectionId=%d", connectionId)
	}
}

func (s *Server) handleMessageForDeviceId(m *msg.JsonWrapper, deviceId string) {
	s.Lock()
	defer s.Unlock()

	log.Warning("device_id not handled at this point !")
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
			log.Warning("Stopping messaging...")
			return nil
		}
		s.handleMessage(m)
	}
}

func (s *Server) Start() error {
	err := s.listen()

	s.msg, err = msg.NewClientUsingHost(msg.TOPIC_RECEIVERS)

	if err == nil {
		log.Debug("Opening MQ %s", par.MQServer)
		err = s.msg.Start(par.MQServer)
	}

	if err == nil {
		go s.runMessaging()
	}

	if err == nil {
		m := msg.NewMessage(msg.TOPIC_GENERAL_EVENTS, "new_receiver")
		m.Data.Set("tcp_port", par.ListenPort)
		s.SendMessage(m)
	}

	if err != nil {
		log.Warning("Error: %v", err)
	}

	return err
}

func (s *Server) SendMessage(m *msg.JsonWrapper) error {
	return s.msg.Publish(m)
}

func (s *Server) Close() {

}
