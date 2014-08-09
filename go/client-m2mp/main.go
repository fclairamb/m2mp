package main

import (
	"bufio"
	"encoding/hex"
	"fmt"
	pr "github.com/fclairamb/m2mp/go/m2mp-protocol"
	"os"
	"strings"
)

func handleSendText(channel, data string) {
	clt.Send <- &pr.MessageDataSimple{Channel: channel, Data: []byte(data)}
}

func handleSendBinary(channel, data string) error {
	bin, err := hex.DecodeString(data)
	if err == nil {
		clt.Send <- &pr.MessageDataSimple{Channel: channel, Data: bin}
	}
	return err
}

func handleSendArrayText(channel, data string) {
	msg := pr.NewMessageDataArray(channel)
	for _, v := range strings.Split(data, " ") {
		msg.AddString(v)
	}
	clt.Send <- msg
}

func handleSendArrayBin(channel, data string) error {
	msg := pr.NewMessageDataArray(channel)
	for _, v := range strings.Split(data, " ") {
		if b, err := hex.DecodeString(v); err != nil {
			msg.Add(b)
		} else {
			return err
		}
	}
	clt.Send <- msg
	return nil
}

func console_handling() {
	in := bufio.NewReader(os.Stdin)
	channel := "_default_"
	for {
		fmt.Printf("> ")
		line, err := in.ReadString('\n')
		if err != nil {
			log.Fatal(err)
			continue
		}
		line = strings.TrimRight(line, "\n")

		tokens := strings.SplitN(line, " ", 2)

		switch tokens[0] {
		case "":
			continue
		case "quit":
			waitForRc <- 0
		case "channel":
			channel = tokens[1]
		case "st":
			handleSendText(channel, tokens[1])
		case "sb":
			if err := handleSendBinary(channel, tokens[1]); err != nil {
				log.Warning("handleSendBinary: %v", err)
			}
		case "sat":
			handleSendArrayText(channel, tokens[1])
		case "sab":
			if err := handleSendArrayBin(channel, tokens[1]); err != nil {
				log.Warning("handleSendArrayBin: %v", err)
			}
		case "help":
			log.Debug(`
Help :
======
* quit               - To quit
* channel <channel>  - Change channel name
* st      <data>     - Send text data on channel
* sat     <array>    - Send an array of text data
* sb      <data>     - Send binary data on channel
* sab     <array>    - Send an array of binary data on channel
`)
		default:
			log.Debug("%+v (%d) not understood !", tokens, len(tokens))
		}
	}
}

var waitForRc chan int

var clt *Client

func main() {
	par := NewParameters()

	waitForRc = make(chan int)
	clt = NewClient(par.Server, par.Ident)
	clt.Start()

	if par.Console {
		go console_handling()
	}

	rc := <-waitForRc
	if rc == 0 {
		log.Info("Bye bye...")
	} else {
		log.Info("Quitting with RC %d", rc)
	}
	os.Exit(rc)
}
