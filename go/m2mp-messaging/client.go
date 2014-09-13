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
	//fmt.Printf("Received message: %s\n", string(m.Body))

	json, err := simplejson.NewJson(m.Body)

	if err == nil {
		msg := NewJsonWrapperFromJson(json)

		if err = msg.Check(); err == nil {
			target := strings.SplitN(msg.To(), ";", 2)[0]
			spl := strings.SplitN(target, "/", 2)

			// We skip messages that aren't for us (this channel)
			if len(spl) > 1 && target != c.from {
				log.Debug("Message skipped: %v (%s instead of %s)", msg.String(), target, c.from)
			} else {
				c.Recv <- msg
			}
		}
	}

	if err != nil {
		log.Warning("Invalid message: %s, %v", string(m.Body), err)
	}

	return err
}

func NewClient(topic, channel string) (clt *Client, err error) {
	clt = &Client{Recv: make(chan *JsonWrapper)}

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

func NewClientAddingHost(topic, channel string) (clt *Client, err error) {
	return NewClient(topic, channel+"_"+HostnameSimple())
}

func HostnameSimple() string {
	hostname, _ := os.Hostname()

	spl := strings.SplitN(hostname, ".", 2)
	hostname = spl[0]

	return hostname
}

func NewClientUsingHost(topic string) (clt *Client, err error) {
	return NewClient(topic, HostnameSimple())
}

func NewClientUsingHostTemp(topic string) (clt *Client, err error) {
	return NewClient(topic, HostnameSimple()+"#ephemeral")
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
	if c.writer == nil {
		return errors.New("Writer !")
	}
	if msg.From() == "" {
		msg.SetFrom(c.from)
	} else if msg.From()[0] == ';' {
		msg.SetFrom(c.from + msg.From())
	}

	target := msg.To()
	{
		spl := strings.SplitN(target, ";", 2)
		target = spl[0]
	}
	{
		spl := strings.SplitN(target, "/", 2)
		target = spl[0]
	}
	//log.Debug("Publishing to \"%s\" with %s", target, msg.String())
	err := c.writer.Publish(target, []byte(msg.String()))
	return err
}

func (c *Client) Stop() {
	c.reader.Stop()
	c.writer.Stop()
}
