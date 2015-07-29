package main

import (
	"bufio"
	"encoding/hex"
	"errors"
	"fmt"
	sjson "github.com/bitly/go-simplejson"
	db "github.com/fclairamb/m2mp/go/m2mp-db"
	ent "github.com/fclairamb/m2mp/go/m2mp-db/entities"
	mq "github.com/fclairamb/m2mp/go/m2mp-messaging"
	version "github.com/fclairamb/m2mp/go/m2mp-version"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// General description
type DataProcessor interface {
	Match(string) bool
	ProcessData(store *mq.JsonWrapper, content string) error
}

// Implementation
type SimpleDataProcessor struct {
	regexp     *regexp.Regexp
	replace    string
	processing func(store *mq.JsonWrapper, content string) error
}

func NewSimpleDataProcessor(match, replace string, processing func(store *mq.JsonWrapper, content string) error) *SimpleDataProcessor {
	return &SimpleDataProcessor{regexp: regexp.MustCompile("^" + match + "$"), replace: replace, processing: processing}
}

func (this SimpleDataProcessor) Match(str string) bool {
	return this.regexp.MatchString(str)
}

func (this SimpleDataProcessor) String() string {
	return fmt.Sprintf("DataProcessor{%s --> %s}", this.regexp.String(), this.replace)
}

var storeAsIs = func(store *mq.JsonWrapper, content string) error {
	store.Set("data", content)
	return nil
}

var dataProcessors = []DataProcessor{
	NewSimpleDataProcessor("L", "sen:loc", func(store *mq.JsonWrapper, content string) error {
		return processDataRequestL(store, content)
	}),
	NewSimpleDataProcessor("G", "sen:loc", func(store *mq.JsonWrapper, content string) error {
		return processDataRequestG(store, content)
	}),
	NewSimpleDataProcessor("A([0-9]+)", "sen:adc:$1", storeAsIs),   // ADC
	NewSimpleDataProcessor("DI([0-9]+)", "sen:gpio:$1", storeAsIs), // Digital input
	NewSimpleDataProcessor("T", "sen:temp", storeAsIs),             // Temperature
	NewSimpleDataProcessor("V", "sen:volt", storeAsIs),             // Voltage
	NewSimpleDataProcessor("D", "sen:door", storeAsIs),             // Door (an other digital input)
	NewSimpleDataProcessor("F", "sen:fuel", storeAsIs),             // Fuel
	NewSimpleDataProcessor("E", "sen:engine", storeAsIs),           // Engine status
	NewSimpleDataProcessor("SMS", "sen:sms_received", storeAsIs),   // Received SMS
	NewSimpleDataProcessor("VER", "sen:version", storeAsIs),        // Program version
	NewSimpleDataProcessor("LS:(.*)", "lsen:$1", storeAsIs),        // Any other listed sensor
}

func (this SimpleDataProcessor) ProcessData(store *mq.JsonWrapper, content string) error {
	dataType, _ := store.Get("type").String()

	dataType = this.regexp.ReplaceAllString(dataType, this.replace)

	store.Set("type", dataType)

	//if this.processing != nil {
	return this.processing(store, content)
	//}

	return nil
}

type ClientHandler struct {
	Id               int                  // Client number
	daddy            *Server              // Parent server
	connectionTime   time.Time            // Client's connection time
	device           *ent.Device          // Detected device
	Conn             net.Conn             // TCP connection
	reader           *bufio.Reader        // Text reader of the connection
	connRecv         chan *string         // Channel to read strings
	msgRecv          chan *mq.JsonWrapper // Channel to read server JSON messages
	LogLevel         int                  // Logging level
	lastReceivedData time.Time            // Last time some data was received
	lastSentData     time.Time            // Last time some data was sent
	ticker           *time.Ticker         // Ticker (every minute)
	pingCounter      byte                 // Ping counter
	cmdCounter       byte                 // Command counter
	cmdShort         map[int]string       // Command id converter
	sessionData      map[string]string    // Session data
}

const IDENTIFICATION_TIMEOUT = time.Duration(time.Second * 30)

const (
	MQ_CALL_DISCONNECT                   = "disconnect"
	MQ_CALL_SEND_SETTINGS                = "send_settings"
	MQ_CALL_SEND_COMMANDS                = "send_commands"
	MQ_CALL_DISCONNECT_IF_NOT_IDENTIFIED = "disconnect_if_not_identified"
	MQ_CALL_STORE_TS                     = "store_ts"
	MQ_CALL_DEVICE_CONNECTED             = "device_connected"
	MQ_CALL_DEVICE_IDENTIFIED            = "device_identified"
	MQ_CALL_DEVICE_DISCONNECTED          = "device_disconnected"
)

func NewClientHandler(daddy *Server, id int, conn net.Conn) *ClientHandler {
	now := time.Now().UTC()
	ch := &ClientHandler{
		daddy:            daddy,
		Id:               id,
		Conn:             conn,
		reader:           bufio.NewReader(conn),
		connectionTime:   now,
		LogLevel:         5,
		connRecv:         make(chan *string, 3),
		msgRecv:          make(chan *mq.JsonWrapper, 10),
		ticker:           time.NewTicker(time.Minute),
		lastReceivedData: now,
		lastSentData:     now,
		cmdShort:         make(map[int]string),
		sessionData:      make(map[string]string),
	}

	return ch
}

var HOSTNAME, _ = os.Hostname()

func (ch *ClientHandler) Start() {
	if ch.LogLevel >= 3 {
		log.Debug("Added %s", ch)
	}
	go ch.runRecv()
	go ch.runCoreHandling()

	{ // We report it in events
		m := mq.NewMessage(mq.TOPIC_GENERAL_EVENTS, MQ_CALL_DEVICE_CONNECTED)
		m.Set("source", ch.Conn.RemoteAddr().String())
		m.Set("connection_id", fmt.Sprint(ch.Id))
		ch.SendMessage(m)
	}

	// We will check the connection soon
	go func() {
		time.Sleep(IDENTIFICATION_TIMEOUT)
		m := mq.NewJsonWrapper()
		m.SetCall(MQ_CALL_DISCONNECT_IF_NOT_IDENTIFIED)
		ch.msgRecv <- m
	}()
}

func (ch *ClientHandler) SendMessage(m *mq.JsonWrapper) error {
	m.SetFrom(fmt.Sprintf(";connection_id=%d", ch.Id))
	if ch.LogLevel >= 9 {
		log.Info("%s - SendMessage( %s );", ch, m)
	}
	return ch.daddy.SendMessage(m)
}

func (ch *ClientHandler) Close() error {
	return ch.Conn.Close()
}

func (ch *ClientHandler) HandleMQMessage(m *mq.JsonWrapper) {
	ch.msgRecv <- m
}

func (ch *ClientHandler) end() {
	ch.daddy.removeClientHandler(ch)
	if ch.LogLevel >= 3 {
		log.Debug("Removed %s", ch)
	}
	ch.ticker.Stop()

	connectionDuration := int64(time.Now().UTC().Sub(ch.connectionTime).Seconds())

	{ // We save the event
		m := mq.NewMessage(mq.TOPIC_GENERAL_EVENTS, MQ_CALL_DEVICE_DISCONNECTED)
		m.Set("source", ch.Conn.RemoteAddr().String())
		m.Set("connection_id", fmt.Sprint(ch.Id))
		m.Set("connection_duration", connectionDuration)
		if ch.device != nil {
			m.Set("device_id", ch.device.Id())
		}
		ch.SendMessage(m)
	}

	if ch.device != nil { // We save it in storage
		m := mq.NewMessage(mq.TOPIC_STORAGE, MQ_CALL_STORE_TS)
		{
			data := sjson.New()
			m.Set("data", data)

			data.Set("type", "device_disconnected")
			data.Set("host", HOSTNAME)
			data.Set("source", ch.Conn.RemoteAddr().String())
			data.Set("connection_id", fmt.Sprint(ch.Id))
			data.Set("connection_duration", connectionDuration)
		}
		m.Set("key", "dev-"+ch.device.Id())
		m.Set("date_uuid", mq.UUIDFromTime(time.Now()))
		m.Set("type", "_server")
		ch.SendMessage(m)
	}
}

func (ch *ClientHandler) runRecv() {
	defer ch.end()
	for {
		if msg, err := ch.reader.ReadString('\n'); err == nil {
			msg = strings.Trim(msg, "\n\r")
			ch.connRecv <- &msg
		} else {
			ch.connRecv <- nil
			log.Warning("%s Error: %v", ch, err)
			return
		}
	}
}

func (ch *ClientHandler) Send(data string) error {
	if ch.LogLevel >= 3 {
		log.Info("%s <-- %s", ch, data)
	}
	_, err := ch.Conn.Write([]byte((data + "\n")))
	ch.lastSentData = time.Now().UTC()
	return err
}

func (ch *ClientHandler) receivedData() {
	ch.lastReceivedData = time.Now().UTC()
}

func (ch *ClientHandler) considerCurrentStatus() {
	now := time.Now().UTC()
	if ch.LogLevel >= 7 {
		log.Info("%s - Considering current status / lastReceived=%s, lastSent=%s", ch, ch.lastReceivedData, ch.lastSentData)
	}

	// We don't actually disconnect connections, we let the TCP connection
	// be killed by some routers.
	if now.Sub(ch.lastReceivedData) > time.Duration(time.Minute*15) &&
		now.Sub(ch.lastSentData) > time.Duration(time.Second*30) {
		if now.Sub(ch.lastReceivedData) > time.Duration(time.Hour*12) {
			log.Warning("%s - Disconnecting. No data transmitted for more than 12 hours !", ch)
			ch.Send("QUIT No data transmitted for more than 12 hours !")
			ch.Close()
		} else {
			if ch.LogLevel >= 6 {
				log.Info("%s - Sending ping request / pingCounter=%d", ch, ch.pingCounter)
			}
			ch.Send(fmt.Sprintf("A %d", ch.pingCounter))
			ch.pingCounter += 1
		}
	}
}

func (this *ClientHandler) handleMQMessage(msg *mq.JsonWrapper) {
	if this.LogLevel >= 9 {
		log.Info("%s - handleMQMessage( %s );", this, msg)
	}
	switch msg.Call() {
	case MQ_CALL_DISCONNECT:
		log.Warning("%s - Server requesting to close the connection !", this)
		this.Close()
	case MQ_CALL_SEND_SETTINGS:
		log.Debug("%s - Sending settings", this)
		if err := this.sendSettingsToSend(); err != nil {
			log.Warning("%s - Error while sending settings: %v", this, err)
		}
	case MQ_CALL_SEND_COMMANDS:
		log.Debug("%s - Sending commands", this)
		if err := this.sendCommands(); err != nil {
			log.Warning("%s - Error while sending commands: %v", this, err)
		}
	case MQ_CALL_DISCONNECT_IF_NOT_IDENTIFIED:
		log.Debug("%s - Check if we are not identified !", this)
		this.disconnectIfNotIdentified()
	}
}

func (this *ClientHandler) disconnectIfNotIdentified() {
	if this.device == nil {
		this.Send("QUIT It took you too long to identify yourself !")
		this.Close()
	}
}

func (ch *ClientHandler) runCoreHandling() {
	for {
		select {
		// We receive all events coming from the connection, the ticker (and later the server)
		case msgPtr := <-ch.connRecv:
			{
				var err error = nil
				if msgPtr != nil {
					msg := *msgPtr
					tokens := strings.SplitN(msg, " ", 2)
					cmd := tokens[0]
					if ch.LogLevel >= 3 {
						log.Info("%s --> %s", ch, msg)
					}
					var content string
					if len(tokens) == 2 {
						content = tokens[1]
					} else {
						content = ""
					}
					switch cmd {
					case "ID":
						err = ch.handleIdentRequest(content)
					case "DB":
						err = ch.handleDebugRequest(content)
					case "C":
						err = ch.handleCommandRequest(content)
					case "S":
						err = ch.handleSettingRequest(content)
					case "D":
						err = ch.handleDataRequest(content)
					case "E":
						err = ch.handleTimedDataRequest(content)
					case "J":
						err = ch.handleJsonDataRequest(content)
					case "A":
						err = ch.handleAckRequest(content)
					case "T":
						t := time.Now().UTC().Unix()
						ch.Send(fmt.Sprintf("T %d", t))
					case "F":
						err = ch.handleFileRequest(content)
					case "B": // Acknowledge response is not used at this point
						break
					case "QUIT":
						ch.Send("QUIT bye !")
						err = ch.Conn.Close()
					default:
						err = errors.New(fmt.Sprintf("Command \"%s\" not understood ! - See http://bit.ly/m2mp-alip for help", cmd))
					}
				} else {
					log.Info("%s - We got disconnected !", ch)
					return
				}

				if err != nil {
					log.Warning("%s - ERROR: %s", ch, err)
					ch.Send(fmt.Sprintf("ERR %s", err))
				}

				ch.receivedData()

			}
		case msg := <-ch.msgRecv:
			{
				ch.handleMQMessage(msg)
			}
		case <-ch.ticker.C:
			{
				if ch.LogLevel >= 6 {
					log.Debug("%s - Tick", ch)
				}
			}
		}

		ch.considerCurrentStatus()
	}
}

var IDENT_CONSTRAINT = regexp.MustCompile("^[a-z][0-9a-z]{2,6}:[a-zA-Z0-9]{4,20}$")

func (ch *ClientHandler) handleIdentRequest(ident string) error {
	var err error = nil

	if ch.device != nil {
		return errors.New("You already identified !")
	}

	if !IDENT_CONSTRAINT.MatchString(ident) {
		return errors.New("Invalid identification !")
	}

	ch.device, err = ent.NewDeviceByIdentCreate(ident)

	// OK
	if err == nil && ch.device != nil {
		err = ch.Send("ID 1")
		if err == nil {
			err = ch.justIdentified()
		}
		return err
	} else {
		// We send an identification refusal but we don't disconnect the device
		return ch.Send("ID 0")
	}

	return err
}

func (ch *ClientHandler) handleCommandRequest(request string) error {
	if ch.device == nil {
		return errors.New("You need to be identified to use settings...")
	}

	tokens := strings.Split(request, " ")
	if len(tokens) == 2 && tokens[0] == "A" && tokens[1] != "" {
		if cmdShort, err := strconv.Atoi(tokens[1]); err == nil {
			cmdId := ch.cmdShort[cmdShort]
			return ch.device.AckCommand(cmdId)
		} else {
			return errors.New("Invalid ack number")
		}
	} else if request == "L" {
		return ch.sendCommands()
	} else {
		return errors.New(fmt.Sprintf("Command not understood: %v", tokens))
	}
}

func (ch *ClientHandler) handleSettingRequest(request string) error {
	if ch.device == nil {
		return errors.New("You need to be identified to use settings...")
	}

	tokens := strings.SplitN(request, " ", 3)

	switch { // Sorted by their probability of usage

	// Acknowledge a setting
	// S A <name> <value>
	case tokens[0] == "A" && len(tokens) == 3:
		return ch.device.AckSetting(tokens[1] /* name */, tokens[2] /* value */)

	// Define a new setting
	// S S <name> <value>
	case tokens[0] == "S" && len(tokens) == 3:
		return ch.device.AckSetting(tokens[1] /* name */, tokens[2] /* value */)

	// Get all the settings (even the one acknowledged)
	// S GA
	case tokens[0] == "GA" && len(tokens) == 1:
		return ch.sendSettingsAll()

	// Erasing a setting because it's undefined
	// S U <name>
	case tokens[0] == "U" && len(tokens) == 2:
		ch.device.DelSetting(tokens[1] /* name */)

	// Get a particular setting
	// S G <name>
	case tokens[0] == "G" && len(tokens) == 2:
		name := tokens[1]
		value := ch.device.Setting(name)
		ch.Send(fmt.Sprintf("S S %s %s", name, value))
		return nil

	// Get all settings that we need to send:
	// S G
	case tokens[0] == "G":
		return ch.sendSettingsToSend()

	default:
		return errors.New(fmt.Sprintf("Command not understood: %v", tokens))
	}
	return nil
}

func (ch *ClientHandler) handleDebugRequest(request string) error {
	tokens := strings.SplitN(request, " ", 2)
	cmd := tokens[0]
	//content := tokens[1]
	switch cmd {
	case "ID":
		if ch.device == nil {
			return errors.New(fmt.Sprintf("Not identified !"))
		} else {
			ch.Send("DB ID " + ch.device.Id())
		}
	case "SOURCE":
		ch.Send(fmt.Sprintf("DB SOURCE %s %d", ch.Conn.RemoteAddr(), ch.Id))
	case "CONNECTED":
		duration := int64(time.Now().UTC().Sub(ch.connectionTime).Seconds())
		ch.Send(fmt.Sprintf("DB CONNECTED %d", duration))
	case "SET_SETTING":
		if ch.device == nil {
			return errors.New(fmt.Sprintf("Not identified !"))
		}
		tokens = strings.Split(request, " ")
		if ch.device == nil {
			return errors.New("Not identified !")
		}
		if len(tokens) < 3 {
			return errors.New(fmt.Sprintf("We have %d tokens instead of 3.", len(tokens)))
		}
		ch.device.SetSetting(tokens[1], tokens[2])
		ch.Send("DB SET_SETTING OK")
		send := mq.NewMessage(fmt.Sprintf("receivers;device_id=%s", ch.device.Id()), "send_settings")
		ch.SendMessage(send)
	case "ADD_COMMAND":
		if ch.device == nil {
			return errors.New(fmt.Sprintf("Not identified !"))
		}
		tokens = strings.SplitN(request, " ", 2)
		if ch.device == nil {
			return errors.New("Not identified !")
		}
		ch.device.AddCommand(tokens[1])
		ch.Send("DB ADD_COMMAND OK")
		send := mq.NewMessage(fmt.Sprintf("receivers;device_id=%s", ch.device.Id()), "send_commands")
		ch.SendMessage(send)
	case "LAST":
		nb := 10
		if ch.device == nil {
			return errors.New(fmt.Sprintf("Not identified !"))
		}
		if len(tokens) >= 2 {
			if nb2, err := strconv.Atoi(tokens[1]); err == nil {
				nb = int(nb2)
			} else {
				return errors.New(fmt.Sprintf("Bad last arg: %v", err))
			}
		}
		iter := db.NewTSDataIterator(fmt.Sprintf("dev-%s", ch.device.Id()), "", nil, nil, true)
		var td db.TimedData
		ch.Send("BEGIN LAST")
		for ; iter.Scan(&td) && nb > 0; nb-- {
			ch.Send(fmt.Sprintf("%.22s, %10s, %s", td.UTime.Time(), td.Type, td.Data))
		}
		ch.Send("END")
	case "LOGLEVEL":
		tokens = strings.Split(request, " ")
		if len(tokens) == 2 {
			if level, err := strconv.Atoi(tokens[1]); err != nil {
				return errors.New(fmt.Sprintf("Bad loglevel: %v", err))
			} else {
				ch.LogLevel = level
			}
		}
		ch.Send(fmt.Sprintf("DB LOGLEVEL %d", ch.LogLevel))
	case "VERSION":
		ch.Send(fmt.Sprintf("DB VERSION %s", version.VERSION))
	default:
		return errors.New(fmt.Sprintf("Debug command \"%s\" is not understood !", cmd))
	}
	return nil
}

func processDataRequestL(store *mq.JsonWrapper, content string) error {
	//store.Set("type", "sen:loc")
	tokens := strings.Split(content, ",")
	data := sjson.New()
	store.Set("data", data)
	// lat,lon
	if len(tokens) >= 2 {
		if lat, err := strconv.ParseFloat(tokens[0], 64); err != nil {
			return errors.New(fmt.Sprintf("Invalid latitude: %v", err))
		} else {
			data.Set("lat", lat)
		}

		lon, err := strconv.ParseFloat(tokens[1], 64)
		if err != nil {
			return errors.New(fmt.Sprintf("Invalid longitude: %v", err))
		} else {
			data.Set("lon", lon)
		}

		if len(tokens) >= 3 {
			if spd, err := strconv.ParseFloat(tokens[2], 64); err != nil {
				return errors.New(fmt.Sprintf("Invalid speed: %v", err))
			} else {
				data.Set("spd", spd)
			}
		}

		if len(tokens) >= 4 {
			if alt, err := strconv.ParseFloat(tokens[3], 64); err != nil {
				return errors.New(fmt.Sprintf("Invalid altitude: %v", err))
			} else {
				data.Set("alt", alt)
			}
		}

	} else if len(tokens) == 1 {
		if sat, err := strconv.Atoi(tokens[0]); err != nil {
			return errors.New(fmt.Sprintf("Invalid number of satellites: %v", err))
		} else {
			data.Set("sat", sat)
		}
	} else {
		return errors.New("Location: Not enough tokens")
	}
	return nil
}

func convertRMC(deg float64) float64 {
	dec := float64(int32(deg / 100))
	deg = (deg - (dec * 100))
	dec += deg / 60

	return dec
}

func processDataRequestG(store *mq.JsonWrapper, content string) error {
	tokens := strings.SplitN(content, ",", 2)
	data := sjson.New()
	store.Set("data", data)
	// date,lat,lon
	if len(tokens) >= 1 {
		// Date
		// We accept all time formats
		if dataTime, err := doYourBestWithTime(tokens[0]); err == nil {
			store.Set("date_uuid", mq.UUIDFromTime(dataTime))
		} else {
			return errors.New(fmt.Sprintf("Invalid date: \"%s\": %v", tokens[0], err))
		}
		return processDataRequestL(store, content[len(tokens[0])+1:])
	} else {
		return errors.New("Location: Not enough tokens")
	}
	return nil
}

func (ch *ClientHandler) processDataRequest(dataTime time.Time, dataType, content string) error {
	if ch.device == nil {
		return errors.New("You must be identified !")
	}

	store := mq.NewMessage(mq.TOPIC_STORAGE, MQ_CALL_STORE_TS)
	store.Set("date_uuid", mq.UUIDFromTime(dataTime))
	store.Set("key", "dev-"+ch.device.Id())
	store.Set("type", dataType)

	matched := false
	for _, dp := range dataProcessors {
		if dp.Match(dataType) {
			if ch.LogLevel >= 15 {
				log.Info("We have a match with %v.", dp)
			}
			if err := dp.ProcessData(store, content); err != nil {
				return err
			}
			matched = true
			break
		}
	}

	if !matched {
		switch dataType {
		case "echo":
			ch.Send(fmt.Sprintf("D %s %s", dataType, content))
			return nil
		default:
			store.Set("data", content)
		}
	}

	if ch.LogLevel >= 9 {
		log.Info("Sending for storage: %v", store)
	}
	if err := ch.SendMessage(store); err != nil {
		return errors.New(fmt.Sprintf("Could not send data for storage: %v", err))
	}
	return nil
}

func (ch *ClientHandler) handleJsonDataRequest(content string) error {
	if ch.device == nil {
		return errors.New("You must be identified !")
	}

	tokens := strings.SplitN(content, " ", 3)

	if len(tokens) < 3 {
		return errors.New("You need to specify a time, a time a content.")
	}

	var time time.Time
	if t, err := doYourBestWithTime(tokens[0]); err == nil {
		time = t
	} else {
		return err
	}

	dataType := tokens[1]
	body := tokens[2]

	if strings.HasPrefix(dataType, "_") {
		return errors.New(fmt.Sprintf("Forbidden data type: %s", dataType))
	}

	if data, err := sjson.NewJson([]byte(body)); err == nil {
		store := mq.NewMessage(mq.TOPIC_STORAGE, MQ_CALL_STORE_TS)
		store.Set("date_uuid", mq.UUIDFromTime(time))
		store.Set("key", "dev-"+ch.device.Id())
		store.Set("type", dataType)
		store.Set("data", data)
		ch.SendMessage(store)
	} else {
		return errors.New(fmt.Sprintf("Invalid json: %s: %v", content, err))
	}

	return nil
}

func (this *ClientHandler) handleAckRequest(content string) error {
	if this.device == nil {
		return errors.New("You must be identified !")
	} else {
		return this.Send("B " + content)
	}
}

func (this *ClientHandler) handleFileRequest(content string) error {
	if this.device == nil {
		return errors.New("You must be identified !")
	}

	tokens := strings.SplitN(content, " ", 4)
	switch {
	case tokens[0] == "N" && len(tokens) == 2: // N <file>
		{
			name := tokens[1]
			file := ent.GetFirmwareFile(name)
			if file != nil {
				this.sessionData["file"] = name
				return this.Send("F N 1")
			} else {
				this.sessionData["file"] = ""
				return this.Send("F N 0")
			}
		}
	case tokens[0] == "S" && len(tokens) == 1: // S
		{
			name := this.sessionData["file"]
			if name != "" {
				file := ent.GetFirmwareFile(name)
				return this.Send(fmt.Sprintf("F S %d", file.Size()))
			} else {
				return this.Send("ERR no file selected")
			}
		}
	case tokens[0] == "G" && len(tokens) == 3: // G <offset> <size>
		{
			var offset, size int
			{
				var err error

				offset, err = strconv.Atoi(tokens[1])
				if err != nil {
					return this.Send(fmt.Sprintf("ERR Invalid offset: %v", err))
				}
				size, err = strconv.Atoi(tokens[2])
				if err != nil {
					return this.Send(fmt.Sprintf("ERR Invalid size: %v", err))
				}
			}

			name := this.sessionData["file"]
			if name != "" {
				reader := ent.GetFirmwareFile(name).Reader()
				reader.Seek(offset, 0)
				data := make([]byte, size)
				n, err := reader.Read(data)
				data = data[0:n]
				if err != nil {
					return this.Send(fmt.Sprintf("ERR Could not read: %v", err))
				}
				return this.Send(fmt.Sprintf("F G %d %s", offset, hex.EncodeToString(data)))
			} else {
				return this.Send("ERR no file selected")
			}
		}
	default:
		return this.Send(fmt.Sprintf("ERR Invalid command: %s (%d)", tokens[0], len(tokens)))
	}
	return nil
}

func (ch *ClientHandler) handleDataRequest(content string) error {
	tokens := strings.SplitN(content, " ", 2)
	if len(tokens) < 2 {
		return errors.New("You need to specify a type and the content.")
	}
	return ch.processDataRequest(time.Now().UTC(), tokens[0] /* type */, tokens[1] /* content */)
}

func (ch *ClientHandler) handleTimedDataRequest(content string) error {
	tokens := strings.SplitN(content, " ", 3)
	if len(tokens) < 3 {
		return errors.New("You need to specify a type, a time and the content.")
	}
	if time, err := doYourBestWithTime(tokens[0]); err != nil {
		return errors.New(fmt.Sprintf("Invalid time: %v", err))
	} else {
		return ch.processDataRequest(time, tokens[1] /* type */, tokens[2] /* content */)
	}
}

func doYourBestWithTime(input string) (time.Time, error) {
	length := len(input)
	switch {
	// Unix timestamp
	case length >= 10 && length < 12:
		if value, err := strconv.ParseInt(input, 10, 64); err == nil {
			return time.Unix(int64(value), 0), nil
		}
	// Format: YYmmddDDHHMMSS
	case length == 12:
		if t, err := time.Parse("060102150405", input); err == nil {
			return t, nil
		} else {
			return time.Unix(0, 0), errors.New(fmt.Sprintf("GPS1 time: %v", err))
		}
	// Format: YYYYmmddHHMMSS
	case length >= 14:
		if t, err := time.Parse("20060102150405", input); err == nil {
			return t, nil
		} else {
			return time.Unix(0, 0), errors.New(fmt.Sprintf("GPS2 time: %v", err))
		}
	}
	return time.Unix(0, 0), errors.New(fmt.Sprintf("Could not guess time: %v", input))
}

func (ch *ClientHandler) justIdentified() error {
	ch.daddy.clientIdentified(ch)

	err := ch.sendSettingsToSend()

	if err == nil {
		err = ch.sendCommands()
	}

	if err == nil {
		err = ch.checkSettings()
	}

	{ // We report it in events
		m := mq.NewMessage(mq.TOPIC_GENERAL_EVENTS, MQ_CALL_DEVICE_IDENTIFIED)
		m.Set("source", ch.Conn.RemoteAddr().String())
		m.Set("connection_id", fmt.Sprint(ch.Id))
		m.Set("device_id", ch.device.Id())
		ch.SendMessage(m)
	}

	{ // We save it in storage
		m := mq.NewMessage(mq.TOPIC_STORAGE, MQ_CALL_STORE_TS)
		{
			data := sjson.New()
			m.Set("data", data)

			data.Set("host", HOSTNAME)
			data.Set("source", ch.Conn.RemoteAddr().String())
			data.Set("connection_id", fmt.Sprint(ch.Id))
			data.Set("type", "device_identified")
		}
		m.Set("key", "dev-"+ch.device.Id())
		m.Set("date_uuid", mq.UUIDFromTime(time.Now()))
		m.Set("type", "_server")
		ch.SendMessage(m)
	}

	{ // And we also save the connection time
		m := mq.NewMessage(mq.TOPIC_STORAGE, MQ_CALL_STORE_TS)
		{
			data := sjson.New()
			m.Set("data", data)

			data.Set("host", HOSTNAME)
			data.Set("source", ch.Conn.RemoteAddr().String())
			data.Set("connection_id", fmt.Sprint(ch.Id))
			data.Set("type", "device_connected")
		}
		m.Set("key", "dev-"+ch.device.Id())
		m.Set("date_uuid", mq.UUIDFromTime(ch.connectionTime))
		m.Set("type", "_server")
		ch.SendMessage(m)
	}

	return err
}

func (ch *ClientHandler) sendSettings(settings map[string]string) error {
	for k, v := range settings {
		ch.Send(fmt.Sprintf("S S %s %s", k, v))
	}
	return nil
}

func (ch *ClientHandler) sendSettingsToSend() error {
	return ch.sendSettings(ch.device.SettingsToSend())
}

func (ch *ClientHandler) sendSettingsAll() error {
	log.Info("%s - Sending all settings", ch)
	return ch.sendSettings(ch.device.Settings())
}

func (ch *ClientHandler) checkSettings() error {
	if len(ch.device.Settings()) == 0 { // If we don't have any setting
		return ch.Send("S GA") // We ask the device to transmit them all ("get all")
	}
	return nil
}

func (ch *ClientHandler) sendCommands() error {
	for k, v := range ch.device.Commands() {
		ch.cmdCounter += 1
		cmdId := int(ch.cmdCounter)
		ch.cmdShort[cmdId] = k
		ch.Send(fmt.Sprintf("C %d %s", cmdId, v))
	}
	return nil
}

func (ch *ClientHandler) String() string {
	return fmt.Sprintf("CH{Id=%d,Conn=%v}", ch.Id, ch.Conn.RemoteAddr())
}
