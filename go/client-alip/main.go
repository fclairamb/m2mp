package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const DATA_DIR = "client-alip-data"

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

func (this *Core) Client(id int) *Client {
	this.RLock()
	defer this.RUnlock()
	return this.clients[id]
}

// This is a sample client implementation of the ALIP protocol.
// Lots of checks (like boundary) aren't performed. It's supposed to be
// as simple as possible and it's the responsability of the server to
// correctly prepare data.

type Client struct {
	// Data
	Id   int
	File string
	data *Data

	// Socket handling
	conn     net.Conn
	reader   *bufio.Reader
	writer   *bufio.Writer
	recvLine chan string
	cmdLine  chan string

	// State machine
	identified bool
	ticker     *time.Ticker
	lastLoc    time.Time
	lastMem    time.Time
	//rand       *rand.Rand
}

func NewClient(id int) *Client {
	clt := &Client{
		Id:       id,
		File:     fmt.Sprintf("%s/c%d.json", DATA_DIR, id),
		recvLine: make(chan string),
		cmdLine:  make(chan string),
		ticker:   time.NewTicker(time.Second * 15),
	}
	return clt
}

func (this *Client) handleReading() {
	this.reader = bufio.NewReader(this.conn)
	for {
		if line, err := this.reader.ReadString('\n'); err == nil {
			line = strings.Trim(line, "\n ")
			log.Debug("%s <-- %s", this, line)
			this.recvLine <- line
		} else {
			log.Warning("%s - Error: %v", this, err)
			return
		}
	}
}

func (this *Client) Send(line string) {
	log.Debug("%s --> %s", this, line)
	this.conn.Write([]byte(fmt.Sprintf("%s\n", line)))
}

func (this *Client) considerState() {
	if !this.identified {
		log.Info("%s - Sending identification request", this)
		this.Send(fmt.Sprintf("ID %s", this.data.Ident))
	} else {
		now := time.Now().UTC()
		if now.Sub(this.lastLoc) > time.Minute {
			this.lastLoc = now
			switch this.lastLoc.Nanosecond() % 3 {
			case 0:
				log.Info("%s - Sending full location", this)
				lat := 48.8 + rand.Float64()
				lon := 2.5 + rand.Float64()
				spd := 20 + rand.Int63()%20
				alt := 400 + rand.Int63()%100
				this.Send(fmt.Sprintf("D L %d,%6.5f,%6.5f,%d,%d", this.lastLoc.Unix(), lat, lon, spd, alt))
			case 1:
				log.Info("%s - Sending lat,lon only location", this)
				lat := 48.8 + rand.Float64()
				lon := 2.5 + rand.Float64()
				this.Send(fmt.Sprintf("D L %v,%v,%v", this.lastLoc.Unix(), lat, lon))
			case 2:
				log.Info("%s - Sending satellites only", this)
				sat := rand.Int() % 15
				this.Send(fmt.Sprintf("D L %v,%v", this.lastLoc.Unix(), sat))
			}
		}
		if now.Sub(this.lastMem) > time.Minute*5 {
			this.lastMem = now
			var mem runtime.MemStats
			runtime.ReadMemStats(&mem)
			log.Info("%s - Sending memory consumption (sample generic data)", this)
			this.Send(fmt.Sprintf("D mem:heap:in-use %v", mem.HeapInuse))
			this.Send(fmt.Sprintf("D mem:num-gc %v", mem.NumGC))
		}
	}
}

func (this *Client) sendAllSettings() {
	for name, set := range this.data.Settings {
		this.Send(fmt.Sprintf("S S %s %s", name, set.Value))
	}
}

func (this *Client) handleSettingsCmd(command string) {
	tokens := strings.SplitN(command, " ", 2)

	switch tokens[0] {
	case "GA":
		log.Info("%s - Server is requesting all settings...", this)
		this.sendAllSettings()
	case "S":
		tokens := strings.SplitN(tokens[1], " ", 2)
		name := tokens[0]
		value := tokens[1]
		log.Info("%s - Server is setting \"%s\" to value \"%s\"", this, name, value)
		this.data.Settings[name] = Setting{Value: value}
	}
}

func (this *Client) handleCommandCmd(command string) {
	tokens := strings.SplitN(command, " ", 2)

	cmdId := tokens[0]
	content := tokens[1]

	this.Send(fmt.Sprintf("C A %s", cmdId))

	switch content {
	case "quit":
		log.Warning("%s - We're quitting !", this)
		core.waitForRc <- 0
	}
}

func (this *Client) core() {
	log.Info("%s - Connected !", this)
	go this.handleReading()
	this.writer = bufio.NewWriter(this.conn)

	for {

		this.considerState()

		select {
		case <-this.ticker.C:
			log.Info("%s - Tick", this)
		case line := <-this.recvLine:
			tokens := strings.SplitN(line, " ", 2)
			cmd := tokens[0]
			switch cmd {
			case "ID":
				this.identified = tokens[1] == "1"
				if this.identified {
					log.Info("%s - Identification was accepted !", this)
				} else {
					log.Info("%s - Identification was refused...", this)
				}

			case "S":
				this.handleSettingsCmd(tokens[1])

			case "A":
				log.Info("%s - Acknowledging ack request", this)
				this.Send("B " + tokens[1])

			case "C":
				this.handleCommandCmd(tokens[1])
			}
		case line := <-this.cmdLine:
			tokens := strings.SplitN(line, " ", 2)
			cmd := tokens[0]
			switch cmd {
			case "disconnect":
				this.conn.Close()
			default:
				this.Send(line)
			}
		}
	}
}

func (this *Client) Save() error {
	return this.data.Save(this.File)
}

func (this *Client) String() string {
	return fmt.Sprintf("%d", this.Id) //this.conn.LocalAddr().String()
}

func (this *Client) Run() {
	var err error = nil
	if this.data, err = NewData(this.File); err != nil {
		if this.data, err = NewData(""); err != nil {
			log.Fatalf("Could not load \"%s\": %v", this.File, err)
		} else {
			log.Warning("Creating new data !")
			this.data.Save(this.File)
		}
	}
	for {
		if this.data.Settings["servers"].Value == "" {
			this.data.Settings["servers"] = Setting{Value: "localhost:3050,ovh3.webingenia.com:3050"}
		}
		this.Save()
		for _, server := range strings.Split(this.data.Settings["servers"].Value, ",") {
			server = strings.Trim(server, " ")
			log.Info("Connecting to %s", server)
			if this.conn, err = net.Dial("tcp", server); err != nil {
				log.Warning("Connection to \"%s\" failed: %v", server, err)
			} else {
				this.core()
			}

			time.Sleep(time.Second)
		}
	}
}

func (this *Client) Start() {
	go this.Run()
}

type Parameters struct {
	NbClients int
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

func createClients(nb int) *Client {
	var client *Client = nil
	for i := 0; i < nb; i++ {
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
