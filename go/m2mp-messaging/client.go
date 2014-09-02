package m2mpmsg

import (
	"errors"
	"fmt"
	nsq "github.com/bitly/go-nsq"
	"github.com/bitly/go-simplejson"
	"os"
	"strings"
)

type Client struct {
	reader *nsq.Consumer
	writer *nsq.Producer
	config *nsq.Config
	Recv   chan *JsonWrapper
	from   string
}

func (c *Client) HandleMessage(m *nsq.Message) error {
	json, err := simplejson.NewJson(m.Body)

	if err == nil {
		msg := NewJsonWrapperFromJson(json)

		if err = msg.Check(); err == nil {
			c.Recv <- msg
		}
	}

	if err != nil {
		log.Warning("Invalid message: %s, %v", string(m.Body), err)
	}

	return err
}

func NewClient(topic, channel string) (clt *Client, err error) {
	clt = &Client{Recv: make(chan *JsonWrapper, 1)}

	clt.config = nsq.NewConfig()
	clt.config.MaxInFlight = 10

	clt.reader, err = nsq.NewConsumer(topic, channel, clt.config)
	if err != nil {
		return
	}
	clt.reader.AddHandler(clt)

	clt.from = topic + "/" + channel

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
	return NewClient(topic, hostname+"#ephemeral")
}

func NewClientGlobal(topic string) (clt *Client, err error) {
	return NewClient(topic, "_")
}

func (c *Client) StartNSQ(addr string) error {
	return c.reader.ConnectToNSQD(addr)
}

func (c *Client) StartLookup(addr string) error {
	return c.reader.ConnectToNSQLookupd(addr)
}

func (c *Client) Start(addr string) (err error) {
	tokens := strings.SplitN(addr, ":", 2)

	target := "localhost:4150"

	switch tokens[0] {
	case "nsq":
		err = c.StartNSQ(tokens[1])
		target = tokens[1]
	case "lookup":
		err = c.StartLookup(tokens[1])
	default:
		err = errors.New(fmt.Sprint("Could not handle address type \"", tokens[0], "\""))
	}

	c.writer, err = nsq.NewProducer(target, c.config)

	if err != nil {
		fmt.Println("NewProducer:", err)
	}

	return
}

func (c *Client) Publish(msg *JsonWrapper) error {
	if msg.From() == "" {
		msg.SetFrom(c.from)
	} else if msg.From()[0] == ':' {
		msg.SetFrom(c.from + msg.From())
	}

	tokens := strings.SplitN(msg.To(), ":", 2)
	log.Debug("Publishing to %s with %s", tokens[0], msg.String())
	err := c.writer.Publish(tokens[0], []byte(msg.String()))
	return err
}

func (c *Client) Stop() {
	c.reader.Stop()
	c.writer.Stop()
}
