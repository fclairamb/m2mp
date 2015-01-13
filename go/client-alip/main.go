package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

const DATA_DIR = "client-alip-data"

const (
	SETTING_SERVERS    = "servers"
	SETTING_MODE       = "mode"
	SETTING_GPS_PERIOD = "gps.period"
)

var authorized_settings = []string{
	SETTING_SERVERS,
	SETTING_MODE,
	SETTING_GPS_PERIOD,
}

type Core struct {
	sync.RWMutex
	waitForRc      chan int
	clients        map[int]*Client
	clientCounter  int
	selectedClient *Client
}

func NewCore() *Core {
	return &Core{
		waitForRc: make(chan int),
		clients:   make(map[int]*Client),
	}
}

func (this *Core) AddClient(c *Client) {
	this.Lock()
	defer this.Unlock()
	this.clients[c.Id] = c
	if this.selectedClient == nil {
		this.selectedClient = c
	}
}

type Parameters struct {
	NbClients  int
	ReplayFile string
}

var par Parameters

func handleConsoleCommand(line string) error {
	tokens := strings.SplitN(line, " ", 2)
	switch tokens[0] {
	case "select":
		if id, err := strconv.Atoi(tokens[1]); err == nil {
			if c := core.Client(id); c != nil {
				core.selectedClient = c
			} else {
				log.Warning("Could not find client %d.", id)
			}
		}
	case "new":
		nbClients := 1
		if len(tokens) == 2 {
			if nb, err := strconv.Atoi(tokens[1]); err == nil {
				nbClients = nb
			} else {
				log.Error("Invalid nb %s: %v", tokens[1], err)
				break
			}
		}
		client := createClients(nbClients)
		core.selectedClient = client
	case "quit":
		log.Info("We're quitting...")
		core.waitForRc <- 0
	case "":
	default:
		core.selectedClient.cmdLine <- line
	}
	return nil
}

func consoleHandling() {
	in := bufio.NewReader(os.Stdin)
	for {
		fmt.Printf("%v > ", core.selectedClient)
		line, err := in.ReadString('\n')
		if err != nil {
			log.Warning("Console: %v", err)
			return
		}
		line = strings.TrimRight(line, "\r\n")
		if err := handleConsoleCommand(line); err != nil {
			log.Error("Command \"%s\" created an error: %v", line, err)
		}
	}
}

var core *Core

func createClients(nbClients int) *Client {
	var client *Client = nil
	for i := 0; i < nbClients; i++ {
		core.clientCounter += 1
		client = NewClient(core.clientCounter)
		client.Start()
		core.AddClient(client)
	}
	return client
}

func main() {
	core = NewCore()

	os.MkdirAll(DATA_DIR, 0777)

	flag.IntVar(&par.NbClients, "clients", 1, "Number of clients")

	createClients(par.NbClients)

	go consoleHandling()

	os.Exit(<-core.waitForRc)
}
