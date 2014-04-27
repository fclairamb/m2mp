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
	writer *nsq.Writer
	Recv   chan *JsonWrapper
	from   string
}

func (c *Client) HandleMessage(m *nsq.Message) error {
	json, err := simplejson.Loads(string(m.Body))
	if err != nil {
		log.Print("Error: ", err)
	}

	msg := NewJsonWrapperFromJson(json)

	if err = msg.Check(); err != nil {
		log.Print("Invalid message: ", err)
	} else {
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

	clt.from = clt.reader.TopicName + "/" + clt.reader.ChannelName

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

func NewClientUsingHostTemp(topic string) (clt *Client, err error) {
	var hostname string
	hostname, err = os.Hostname()
	if err != nil {
		return
	}
	return NewClient(topic, "#"+hostname)
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

func (c *Client) Start(addr string) (err error) {
	tokens := strings.SplitN(addr, ":", 2)

	switch tokens[0] {
	case "nsq":
		err = c.StartNSQ(tokens[1])
	case "lookup":
		err = c.StartLookup(tokens[1])
	default:
		err = errors.New(fmt.Sprint("Could not handle address type \"", tokens[0], "\""))
	}

	c.writer = nsq.NewWriter(addr)
	return
}

func (c *Client) Publish(msg *JsonWrapper) error {
	if msg.From() == "" {
		msg.SetFrom(c.from)
	}

	tokens := strings.SplitN(msg.To(), ":", 2)
	_, _, err := c.writer.Publish(tokens[0], []byte(msg.String()))
	return err
}

func (c *Client) Stop() {
	c.reader.Stop()
	c.writer.Stop()
}
