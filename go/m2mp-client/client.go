package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	pr "github.com/fclairamb/m2mp/go/m2mp-protocol"
	"net"
	"os"
	"strings"
	"time"
)

type Client struct {
	Conn      *pr.ProtoHandler
	Recv      chan interface{}
	Connected bool
	ticker    *time.Ticker
	Ident     string
	status    map[string]string
	settings  map[string]string
}

func NewClient(target, ident string) *Client {
	conn, _ := net.Dial("tcp", target)
	handler := pr.NewProtoHandlerClient(conn)
	clt := &Client{Conn: handler, Ident: ident, Recv: make(chan interface{}), ticker: time.NewTicker(time.Second * 30), status: make(map[string]string)}

	clt.status["cap"] = "sen"

	if err := clt.loadSettings(); err != nil {
		log.Debug("Settings: ", err)
	}

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
		switch m := msg.(type) {
		case *pr.EventDisconnected:
			return
		case *pr.MessageDataArray:
			if m.Channel == "__sta" { // If we are on the status channel, we handle things directly
				requestType := string(m.Data[0])
				if requestType == "g" {
					name := string(m.Data[1])
					value := c.status[name]
					msg := pr.NewMessageDataArray(m.Channel)
					msg.AddString("g")
					msg.AddString(fmt.Sprintf("%s=%s", name, value))
					c.Conn.Send(msg)
				} else if requestType == "ga" {
					msg := pr.NewMessageDataArray(m.Channel)
					msg.AddString("g")
					for name, value := range c.status {
						msg.AddString(fmt.Sprintf("%s=%s", name, value))
					}
				}
			} else if m.Channel == "_set" { // if we are on the settings channnel, we also handle things directly
				requestType := string(m.Data[0])
				set := strings.Contains(requestType, "s")
				get := strings.Contains(requestType, "g")
				all := strings.Contains(requestType, "a")

				var response *pr.MessageDataArray

				if get {
					response = pr.NewMessageDataArray(m.Channel)
					response.AddString("g")
				} else {
					response = nil
				}
				if all {
					for name, value := range c.settings {
						response.AddString(fmt.Sprintf("%s=%s", name, value))
					}
				} else {
					for _, v := range m.Data[1:] {
						spl := strings.SplitN(string(v), "=", 2)
						name := spl[0]
						if set && len(spl) == 2 {
							value := spl[1]
							c.setSetting(name, value)
						}
						if get {
							value := c.settings[name]
							response.AddString(fmt.Sprintf("%s=%s", name, value))
						}
					}
				}

				if response != nil {
					c.Conn.Send(response)
				}
			} else if m.Channel == "_cmd" { // If we are on the command channel, we will *try* to handle things directly
				requestType := string(m.Data[0])
				if requestType == "e" {
					response, err := c.handleCommand(m.Data[2:])

					if err == nil { // If we could actually handle this...
						responseMsg := pr.NewMessageDataArray(m.Channel)
						responseMsg.AddString("a")
						responseMsg.Add(m.Data[1])
						if response != nil {
							responseMsg.Data = append(responseMsg.Data, response...)
						}
						c.Conn.Send(responseMsg)
						break
					}
				}
			}
		}
		c.Recv <- msg
	}
}

func (c *Client) handleCommand(array [][]byte) ([][]byte, error) {
	cmd := string(array[0])
	if cmd == "reset" {
		c.settings = make(map[string]string)
		return nil, c.saveSettings()
	}
	return nil, errors.New(fmt.Sprintf("Unknow command \"%s\"", cmd))
}

const SETTINGS_FILE = "settings.json"

func (c *Client) loadSettings() error {
	// We open the file
	fi, err := os.Open(SETTINGS_FILE)
	if err != nil {
		return err
	}
	defer fi.Close()

	// Create a reader
	r := bufio.NewReader(fi)

	// Create a decode
	dec := json.NewDecoder(r)

	// Load the settings
	return dec.Decode(&c.settings)
}

func (c *Client) saveSettings() error {
	// We open the file
	fi, err := os.Open(SETTINGS_FILE)
	if err != nil {
		return err
	}
	defer fi.Close()

	// Create a writer
	w := bufio.NewWriter(fi)
	defer w.Flush()

	// Create an encoder
	enc := json.NewEncoder(w)

	// Save the settings
	return enc.Encode(c.settings)
}

func (c *Client) setSetting(name, value string) error {
	c.settings[name] = value
	return c.saveSettings()
}

func (c *Client) runCore() {
	for {
		if err := c.considerAction(); err != nil {
			log.Error("considerAction: %s", err)
			break
		}
		select {
		case msg := <-c.Recv:
		{
			switch m := msg.(type) {
			case *pr.EventDisconnected:
			{
				log.Error("We got disconnected !")
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
