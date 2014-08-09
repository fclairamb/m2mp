package main

import (
	"fmt"
	"github.com/fclairamb/m2mp/go/m2log"
	msg "github.com/fclairamb/m2mp/go/m2mp-messaging"
	"net"
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

func (s *Server) handleMessaging(m *msg.JsonWrapper) {
	log.Debug("Handling ", m)
}

func (s *Server) runMessaging() error {
	for {
		m := <-s.msg.Recv
		if m != nil {
			return nil
		}
		s.handleMessaging(m)
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
		m := msg.NewMessageEvent("new_receiver")
		m.Data.Set("tcp_port", fmt.Sprint(par.ListenPort))
		s.SendMessage(m)
	}

	return err
}

func (s *Server) SendMessage(m *msg.JsonWrapper) error {
	return s.msg.Publish(m)
}

func (s *Server) Close() {

}