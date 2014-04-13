package main

import (
	pr "github.com/fclairamb/m2mp/go/m2mp-protocol"
	"log"
	"net"
	"time"
)

type Client struct {
	Conn      *pr.ProtoHandler
	Recv      chan interface{}
	Connected bool
	ticker    *time.Ticker
	Ident     string
}

func NewClient(target, ident string) *Client {
	conn, _ := net.Dial("tcp", target)
	handler := pr.NewProtoHandlerClient(conn)
	clt := &Client{Conn: handler, Ident: ident, Recv: make(chan interface{}), ticker: time.NewTicker(time.Second * 30)}

	return clt
}

func (c *Client) considerAction() error {
	if !c.Connected {
		return c.Conn.Send(&pr.MessageIdentRequest{Ident: c.Ident})
	} else {

	}

	return nil
}

func (c *Client) runRecv() {
	for {
		msg := c.Conn.Recv()
		switch msg.(type) {
		case *pr.EventDisconnected:
			return
		}
		c.Recv <- msg
	}
}

func (c *Client) runCore() {
	for {
		if err := c.considerAction(); err != nil {
			log.Println(err)
			break
		}
		select {
		case msg := <-c.Recv:
			{
				switch m := msg.(type) {
				case *pr.EventDisconnected:
					{
						log.Println("We got disconnected !")
						break
					}
				case *pr.MessageIdentResponse:
					{
						c.Connected = m.Ok
					}
				case *pr.MessagePingRequest:
					{
						c.Conn.Send(&pr.MessagePingResponse{Data: m.Data})
					}
				}
			}
		case <-c.ticker.C:
			{

			}
		}
	}
}

func (c *Client) Start() {
	go c.runRecv()
	go c.runCore()
}
