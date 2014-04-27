package m2mpmsg

import (
	"github.com/bitly/go-nsq"
	"github.com/likexian/simplejson"
	// "github.com/bitly/nsq/util"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
)

type Client struct {
	reader *nsq.Reader
	Recv   chan *JsonWrapper
}

func (c *Client) HandleMessage(m *nsq.Message) error {
	json, err := simplejson.Loads(string(m.Body))
	if err != nil {
		log.Print("Error: ", err)
	}

	var msg *JsonWrapper
	{
		var from, to, call string
		if from, err = json.Get("from").String(); err != nil {
			log.Print("MSG/From: ", err)
		}

		if to, err = json.Get("to").String(); err != nil {
			log.Print("MSG/To: ", err)
		}

		if call, err = json.Get("call").String(); err != nil {
			log.Print("MSG/Call: ", err)
		}
		msg = &JsonWrapper{Data: json, From: from, To: to, Call: call}
	}

	if err == nil {
		c.Recv <- msg
	}

	return err
}

func NewClient(topic, channel string) (clt *Client, err error) {
	clt = &Client{Recv: make(chan *JsonWrapper, 10)}
	clt.reader, err = nsq.NewReader(topic, channel)
	if err != nil {
		return
	}
	clt.reader.AddHandler(clt)
	return
}

func NewClientUsingHost(topic string) (clt *Client, err error) {
	var hostname string
	hostname, err = os.Hostname()
	if err != nil {
		return
	}
	return NewClient(topic, hostname)
}

func NewClientGlobal(topic string) (clt *Client, err error) {
	return NewClient(topic, "_")
}

func (c *Client) StartNSQ(addr string) error {
	return c.reader.ConnectToNSQ(addr)
}

func (c *Client) StartLookup(addr string) error {
	return c.reader.ConnectToLookupd(addr)
}

func (c *Client) Start(addr string) error {
	tokens := strings.SplitN(addr, ":", 2)
	switch tokens[0] {
	case "nsq":
		return c.StartNSQ(tokens[1])
	case "lookup":
		return c.StartLookup(tokens[1])
	default:
		return errors.New(fmt.Sprint("Could not handle address type \"", tokens[0], "\""))
	}
}

func (c *Client) Stop() {
	c.reader.Stop()
}
