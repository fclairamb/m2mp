package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"runtime"
	"strconv"
	"strings"
	"time"
)

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
	sendLine chan string

	// State machine
	Identified bool
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
		sendLine: make(chan string),
		ticker:   time.NewTicker(time.Second * 5),
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

func (this *Client) handleSending() {
	this.writer = bufio.NewWriter(this.conn)
	for {
		line := <-this.sendLine

		line += "\n"
		if _, err := this.writer.WriteString(line); err == nil {
			log.Debug("%s --> %s", this, line)
		} else {
			log.Warning("%s - Error: %v", this, err)
		}
	}
}

func (this *Client) Send(line string) {
	log.Debug("%s --> %s", this, line)
	this.conn.Write([]byte(fmt.Sprintf("%s\n", line)))
}

func (this *Client) randomTransmission() {
	mode := this.data.Settings[SETTING_MODE]
	now := time.Now().UTC()

	if mode.Value == "" {
		mode.Value = "default"
		this.data.Settings[SETTING_MODE] = Setting{Value: mode.Value, Changed: true}
		this.Save()
		this.Send(fmt.Sprintf("S S %s %s", SETTING_MODE, mode.Value))
	}

	if mode.Value == "tracker" {
		if period, err := strconv.Atoi(this.data.Settings[SETTING_GPS_PERIOD].Value); err == nil {
			if now.Sub(this.lastLoc) > time.Duration(period)*time.Second {
				this.lastLoc = now
				log.Info("%s - Tracker - Sending full location", this)
				lat := 48.8 + rand.Float64()
				lon := 2.5 + rand.Float64()
				spd := 20 + rand.Int63()%20
				alt := 400 + rand.Int63()%100
				this.Send(fmt.Sprintf("D L %6.5f,%6.5f,%d,%d", lat, lon, spd, alt))
			}
		} else {
			this.data.Settings[SETTING_GPS_PERIOD] = Setting{Value: "15", Changed: true}
			this.Save()
			this.Send(fmt.Sprintf("S S %s %s", SETTING_GPS_PERIOD, this.data.Settings[SETTING_GPS_PERIOD].Value))
		}
	} else {
		if now.Sub(this.lastLoc) > time.Minute {
			this.lastLoc = now
			switch this.lastLoc.Nanosecond() % 3 {
			case 0:
				log.Info("%s - Default - Sending full location", this)
				lat := 48.8 + rand.Float64()
				lon := 2.5 + rand.Float64()
				spd := 20 + rand.Int63()%20
				alt := 400 + rand.Int63()%100
				this.Send(fmt.Sprintf("E %d L %6.5f,%6.5f,%d,%d", this.lastLoc.Unix(), lat, lon, spd, alt))
			case 1:
				log.Info("%s - Default - Sending lat,lon only location", this)
				lat := 48.8 + rand.Float64()
				lon := 2.5 + rand.Float64()
				this.Send(fmt.Sprintf("E %v L %v,%v", this.lastLoc.Unix(), lat, lon))
			case 2:
				log.Info("%s - Default - Sending satellites only", this)
				sat := rand.Int() % 15
				this.Send(fmt.Sprintf("E %v L %v", this.lastLoc.Unix(), sat))
			}
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

func (this *Client) considerState() {
	if !this.Identified {
		log.Info("%s - Sending identification request", this)
		this.Send(fmt.Sprintf("ID %s", this.data.Ident))
	} else {

	}
}

func (this *Client) sendAllSettings() {
	for name, set := range this.data.Settings {
		this.Send(fmt.Sprintf("S S %s %s", name, set.Value))
	}
}

func (this *Client) isAuthorizedSetting(name string) bool {
	for _, n := range authorized_settings {
		if name == n {
			return true
		}
	}

	return false
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

		if !this.isAuthorizedSetting(name) {
			this.Send(fmt.Sprintf("S U %s", name))
			return
		}

		value := tokens[1]
		log.Info("%s - Server is setting \"%s\" to value \"%s\"", this, name, value)
		this.data.Settings[name] = Setting{Value: value}
		this.Save()
		this.Send(fmt.Sprintf("S A %s %s", name, value))
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
	case "settings":
		this.sendAllSettings()
	}
}

func (this *Client) startReplayer() {
	replayer := NewReplayer(this, this.data.ReplayFile)
	go replayer.Run()
}

func (this *Client) core() {
	log.Info("%s - Connected !", this)
	go this.handleReading()
	go this.handleSending()
	this.writer = bufio.NewWriter(this.conn)

	for {

		this.considerState()

		select {
		case <-this.ticker.C:
			break
			//log.Info("%s - Tick", this)
		case line := <-this.recvLine:
			tokens := strings.SplitN(line, " ", 2)
			cmd := tokens[0]
			switch cmd {
			case "ID":
				identified := tokens[1] == "1"
				if !this.Identified {
					if identified {
						log.Info("%s - Identification was accepted !", this)
						if this.data.ReplayFile != "" {
							this.startReplayer()
						}
					} else {
						log.Info("%s - Identification was refused...", this)
					}
				}

				this.Identified = identified

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
		if this.data.Settings[SETTING_SERVERS].Value == "" {
			this.data.Settings[SETTING_SERVERS] = Setting{Value: "localhost:3050,ovh3.webingenia.com:3050"}
		}
		this.Save()
		for _, server := range strings.Split(this.data.Settings[SETTING_SERVERS].Value, ",") {
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
