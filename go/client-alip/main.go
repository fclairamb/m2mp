package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"runtime"
	"strings"
	"time"
)

var waitForRc chan int

// This is a sample client implementation of the ALIP protocol.
// Lots of checks (like boundary) aren't performed. It's supposed to be
// as simple as possible and it's the responsability of the server to
// correctly prepare data.

type Client struct {
	// Data
	File string
	data *Data

	// Socket handling
	conn     net.Conn
	reader   *bufio.Reader
	writer   *bufio.Writer
	recvLine chan string

	// State machine
	identified bool
	ticker     *time.Ticker
	lastLoc    time.Time
	lastMem    time.Time
	//rand       *rand.Rand
}

func NewClient(file string) *Client {
	clt := &Client{
		File:     file,
		recvLine: make(chan string),
		ticker:   time.NewTicker(time.Second * 15),
		//	rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
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
		waitForRc <- 0
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
		}
	}
}

func (this *Client) Save() error {
	return this.data.Save(this.File)
}

func (this *Client) String() string {
	return this.conn.LocalAddr().String()
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

func init() {
	waitForRc = make(chan int)
}

type Parameters struct {
	NbClients int
}

var par Parameters

func main() {
	flag.IntVar(&par.NbClients, "clients", 1, "Number of clients")

	for i := 0; i < par.NbClients; i++ {
		client := NewClient(fmt.Sprintf("c%d.json", i))
		client.Start()
	}

	os.Exit(<-waitForRc)
}
