package main

import (
	"bufio"
	"errors"
	"fmt"
	sjson "github.com/bitly/go-simplejson"
	db "github.com/fclairamb/m2mp/go/m2mp-db"
	ent "github.com/fclairamb/m2mp/go/m2mp-db/entities"
	mq "github.com/fclairamb/m2mp/go/m2mp-messaging"
	"net"
	"strconv"
	"strings"
	"time"
)

type ClientHandler struct {
	Id                      int
	daddy                   *Server
	connectionTime          time.Time
	device                  *ent.Device
	deviceChannelTranslator *ent.DeviceChannelTrans
	Conn                    net.Conn
	reader                  *bufio.Reader
	connRecv                chan *string
	msgRecv                 chan *mq.JsonWrapper
	LogLevel                int
	lastReceivedData        time.Time
	lastSentData            time.Time
	ticker                  *time.Ticker
	pingCounter             byte
	cmdCounter              byte
	cmdShort                map[int]string
}

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
	}

	return ch
}

func (ch *ClientHandler) Start() {
	if ch.LogLevel >= 3 {
		log.Debug("Added %s", ch)
	}
	go ch.runRecv()
	go ch.runCoreHandling()

	{ // We report it in events
		m := mq.NewMessage(mq.TOPIC_GENERAL_EVENTS, "device_connected")
		m.Set("source", ch.Conn.RemoteAddr().String())
		m.Set("connection_id", fmt.Sprint(ch.Id))
		ch.SendMessage(m)
	}
}

func (ch *ClientHandler) SendMessage(m *mq.JsonWrapper) {
	m.SetFrom(fmt.Sprintf(";connection_id=%d", ch.Id))
	ch.daddy.SendMessage(m)
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
		m := mq.NewMessage(mq.TOPIC_GENERAL_EVENTS, "device_disconnected")
		m.Set("source", ch.Conn.RemoteAddr().String())
		m.Set("connection_id", fmt.Sprint(ch.Id))
		m.Set("connection_duration", connectionDuration)
		if ch.device != nil {
			m.Set("device_id", ch.device.Id())
		}
		ch.SendMessage(m)
	}

	if ch.device != nil { // We save it in storage
		m := mq.NewMessage(mq.TOPIC_STORAGE, "store_ts")
		{
			data := sjson.New()
			m.Set("data", data)

			data.Set("type", "device_disconnected")
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
		log.Debug("%s - Considering current status", ch)
	}

	// We don't actually disconnect connections, we let the TCP connection
	// be killed by some routers.
	if now.Sub(ch.lastReceivedData) > time.Duration(time.Minute*15) &&
		now.Sub(ch.lastSentData) > time.Duration(time.Second*30) {
		if ch.LogLevel >= 5 {
			log.Debug("%s - Sending ping request", ch)
		}
		ch.Send(fmt.Sprintf("A %d", ch.pingCounter))
		ch.pingCounter += 1
	}
}

func (this *ClientHandler) handleMQMessage(msg *mq.JsonWrapper) {
	switch msg.Call() {
	case "disconnect":
		log.Warning("%s - Server requesting to close the connection !", this)
		this.Close()
	case "send_settings":
		log.Debug("%s - Sending settings", this)
		if err := this.sendSettingsToSend(); err != nil {
			log.Warning("%s - Error while sending settings: %v", this, err)
		}
	case "send_commands":
		log.Debug("%s - Sending commands", this)
		if err := this.sendCommands(); err != nil {
			log.Warning("%s - Error while sending commands: %v", this, err)
		}
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
					case "A":
						ch.Send("B " + content)
					case "T":
						t := time.Now().UTC().Unix()
						ch.Send(fmt.Sprintf("T %d", t))
					case "B": // Acknowledge response is not used at this point
					case "QUIT":
						ch.Send("QUIT bye !")
						err = ch.Conn.Close()
					default:
						err = errors.New(fmt.Sprintf("Command \"%s\" not understood ! - See http://bit.ly/WqnmB1 for help", cmd))
					}
				} else {
					log.Warning("%s - We got disconnected !", ch)
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
				log.Debug("MQ message: %s", msg.String())
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

func (ch *ClientHandler) handleIdentRequest(ident string) error {
	var err error = nil

	if ch.device != nil {
		return errors.New("You already identified !")
	}

	if ident == "" {
		return errors.New("You need to specify an ID")
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
	if len(tokens) == 2 && tokens[0] == "G" {
		name := tokens[1]
		value := ch.device.Setting(name)
		ch.Send(fmt.Sprintf("S S %s %s", tokens[1], value))
		return nil
	} else if len(tokens) == 3 {
		if tokens[0] == "A" {
			return ch.device.AckSetting(tokens[1], tokens[2])
		} else if tokens[0] == "S" {
			name := tokens[1]
			value := tokens[2]
			return ch.device.AckSetting(name, value)
		}
	} else if len(tokens) == 1 && tokens[0] == "G" {
		return ch.sendSettingsToSend()
	} else if len(tokens) == 1 && tokens[0] == "GA" {
		return ch.sendSettingsAll()
	} else {
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
			ch.Send(fmt.Sprintf("%25s, %20s, %s", td.UTime.Time(), td.Type, td.Data))
		}
		ch.Send("END")
	case "LOGLEVEL":
		tokens = strings.Split(request, " ")
		if level, err := strconv.Atoi(tokens[2]); err != nil {
			return errors.New(fmt.Sprintf("Bad loglevel \"%s\": %v", tokens[2], err))
		} else {
			ch.LogLevel = level
		}
	default:
		return errors.New(fmt.Sprintf("Debug command \"%s\" is not understood !", cmd))
	}
	return nil
}

func (ch *ClientHandler) handleDataRequest(content string) error {
	if ch.device == nil {
		return errors.New("You must be identified !")
	}

	tokens := strings.SplitN(content, " ", 2)
	if len(tokens) < 2 {
		return errors.New("You need to specify a type and the content.")
	}
	dataType := tokens[0]
	content = tokens[1]

	store := mq.NewMessage(mq.TOPIC_STORAGE, "store_ts")
	store.Set("date_uuid", mq.UUIDFromTime(time.Now().UTC()))
	store.Set("key", "dev-"+ch.device.Id())
	store.Set("type", dataType)

	switch dataType {
	case "echo":
		{
			ch.Send(fmt.Sprintf("D %s %s", dataType, content))
			return nil
		}
	case "L":
		{
			store.Set("type", "sen:loc")
			tokens := strings.Split(content, ",")
			data := sjson.New()
			store.Set("data", data)
			// date,lat,lon
			if len(tokens) >= 3 {
				if t, err := strconv.Atoi(tokens[0]); err != nil {
					return errors.New(fmt.Sprintf("Invalid time: %v", err))
				} else {
					store.Set("date_uuid", mq.UUIDFromTime(time.Unix(int64(t), 0)))
				}

				if lat, err := strconv.ParseFloat(tokens[1], 64); err != nil {
					return errors.New(fmt.Sprintf("Invalid latitude: %v", err))
				} else {
					data.Set("lat", lat)
				}

				lon, err := strconv.ParseFloat(tokens[2], 64)
				if err != nil {
					return errors.New(fmt.Sprintf("Invalid longitude: %v", err))
				} else {
					data.Set("lon", lon)
				}

				if len(tokens) >= 4 {
					if spd, err := strconv.ParseFloat(tokens[3], 64); err != nil {
						return errors.New(fmt.Sprintf("Invalid speed \"%s\" : %v", tokens[3]))
					} else {
						data.Set("spd", spd)
					}
				}

				if len(tokens) >= 5 {
					if alt, err := strconv.ParseFloat(tokens[4], 64); err != nil {
						return errors.New(fmt.Sprintf("Invalid altitude \"%s\" : %v", tokens[4]))
					} else {
						data.Set("alt", alt)
					}
				}

			} else if len(tokens) == 2 {
				if t, err := strconv.Atoi(tokens[0]); err != nil {
					return errors.New(fmt.Sprintf("Invalid time \"%s\": %v", tokens[0], err))
				} else {
					store.Set("date_uuid", mq.UUIDFromTime(time.Unix(int64(t), 0)))
				}

				if sat, err := strconv.Atoi(tokens[1]); err != nil {
					return errors.New(fmt.Sprintf("Invalid number of satellites \"%s\": %v", tokens[1], err))
				} else {
					data.Set("sat", sat)
				}
			} else {
				return errors.New("Location: Not enough tokens")
			}
		}

	default:
		{
			store.Set("data", content)
		}
	}

	ch.SendMessage(store)
	return nil
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
		m := mq.NewMessage(mq.TOPIC_GENERAL_EVENTS, "device_identified")
		m.Set("source", ch.Conn.RemoteAddr().String())
		m.Set("connection_id", fmt.Sprint(ch.Id))
		m.Set("device_id", ch.device.Id())
		ch.SendMessage(m)
	}

	{ // We save it in storage
		m := mq.NewMessage(mq.TOPIC_STORAGE, "store_ts")
		{
			data := sjson.New()
			m.Set("data", data)

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
		m := mq.NewMessage(mq.TOPIC_STORAGE, "store_ts")
		{
			data := sjson.New()
			m.Set("data", data)

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