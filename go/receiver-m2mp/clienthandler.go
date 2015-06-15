package main

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	sjson "github.com/bitly/go-simplejson"
	ent "github.com/fclairamb/m2mp/go/m2mp-db/entities"
	mq "github.com/fclairamb/m2mp/go/m2mp-messaging"
	pr "github.com/fclairamb/m2mp/go/m2mp-protocol"
	"net"
	"os"
	"strings"
	"time"
)

type ClientHandler struct {
	Id                      int                     // Connection id
	daddy                   *Server                 // Server's reference
	connectionTime          time.Time               // Connection's time
	device                  *ent.Device             // Device's object
	Conn                    *pr.ProtoHandler        // Protocol handler
	connRecv                chan interface{}        // Protocol messages channel
	msgRecv                 chan *mq.JsonWrapper    // Server messaging channel
	LogLevel                int                     // Client log level
	lastReceivedData        time.Time               // Last received's data time
	lastSentData            time.Time               // Last sent's data time
	ticker                  *time.Ticker            // Ticker
	pingCounter             byte                    // Ping counter
	settingLastTransmission map[string]time.Time    // Settings last transmission
	deviceChannelTranslator *ent.DeviceChannelTrans // Device channel translator
}

func NewClientHandler(daddy *Server, id int, conn net.Conn) *ClientHandler {
	now := time.Now().UTC()
	ch := &ClientHandler{
		daddy:                   daddy,
		Id:                      id,
		Conn:                    pr.NewProtoHandlerServer(conn),
		connectionTime:          now,
		LogLevel:                7,
		connRecv:                make(chan interface{}, 3),
		msgRecv:                 make(chan *mq.JsonWrapper, 10),
		ticker:                  time.NewTicker(time.Minute),
		lastReceivedData:        now,
		lastSentData:            now,
		settingLastTransmission: make(map[string]time.Time),
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
		m := mq.NewMessage(mq.TOPIC_GENERAL_EVENTS, "device_connected")
		m.Set("source", ch.Conn.Conn.RemoteAddr().String())
		m.Set("connection_id", fmt.Sprint(ch.Id))
		m.Set("host", HOSTNAME)
		ch.SendMessage(m)
	}
}

func (ch *ClientHandler) SendMessage(m *mq.JsonWrapper) {
	m.SetFrom(fmt.Sprintf(";connection_id=%d", ch.Id))
	ch.daddy.SendMessage(m)
}

func (ch *ClientHandler) Close() error {
	return ch.Conn.Conn.Close()
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
		m.Set("source", ch.Conn.Conn.RemoteAddr().String())
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
			data.Set("host", HOSTNAME)
			data.Set("source", ch.Conn.Conn.RemoteAddr().String())
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
		msg := ch.Conn.Recv()
		ch.connRecv <- msg
		switch msg.(type) {
		// If this is a disconnection event, then we should quit the go routine
		case *pr.EventDisconnected:
			{
				return
			}
		}
	}
}

func (ch *ClientHandler) Send(m interface{}) error {
	err := ch.Conn.Send(m)
	ch.lastSentData = time.Now().UTC()
	return err
}

func (ch *ClientHandler) receivedData() {
	ch.lastReceivedData = time.Now().UTC()
}

func (ch *ClientHandler) considerCurrentStatus() {
	now := time.Now().UTC()
	if ch.LogLevel >= 9 {
		log.Debug("%s - Considering current status (%s)", ch, now)
	}

	// We don't actually disconnect connections, we let the TCP connection
	// be killed by some routers.
	if now.Sub(ch.lastReceivedData) > time.Duration(time.Minute*15) &&
		now.Sub(ch.lastSentData) > time.Duration(time.Second*30) {
		log.Debug("%s - Sending ping request", ch)
		ch.Send(&pr.MessagePingRequest{Data: ch.pingCounter})
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
		case msg := <-ch.connRecv:
			{
				switch m := msg.(type) {
				case *pr.MessageDataSimple:
					ch.handleData(m)
				case *pr.MessageDataArray:
					ch.handleDataArray(m)
				case *pr.MessageIdentRequest:
					ch.handleIdentRequest(m)
				case *pr.MessagePingRequest:
					ch.Conn.Send(&pr.MessagePingResponse{Data: m.Data})
					// If this is a disconnection event, we should quit the current go routine
				case *pr.EventDisconnected:
					{
						return
					}
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
				if ch.LogLevel >= 9 {
					log.Debug("%s - Tick", ch)
				}
			}
		}

		ch.considerCurrentStatus()
	}
}

func (ch *ClientHandler) handleIdentRequest(m *pr.MessageIdentRequest) error {
	var err error
	ch.device, err = ent.NewDeviceByIdentCreate(m.Ident)
	if err != nil {
		log.Warning("%s --> (error) %s", ch, err)
	}

	// OK
	if err == nil && ch.device != nil {
		err = ch.Send(&pr.MessageIdentResponse{Ok: true})
		if err == nil {
			err = ch.justIdentified()
		}
		return err
	} else {
		// We send an identification refusal but we don't disconnect the device
		return ch.Send(&pr.MessageIdentResponse{Ok: false})
	}
}

func (ch *ClientHandler) checkCapacities() error {
	status := ch.device.Status("cap")
	if status == "" {
		msg := pr.NewMessageDataArray("_sta")
		msg.AddString("g")
		msg.AddString("cap")
		return ch.Send(msg)
	}
	return nil
}

func (ch *ClientHandler) getDeviceChannelTranslator() *ent.DeviceChannelTrans {
	if ch.deviceChannelTranslator == nil && ch.device != nil {
		ch.deviceChannelTranslator = ent.NewDeviceChannelTrans(ch.device)
	}
	return ch.deviceChannelTranslator
}

func (ch *ClientHandler) justIdentified() error {
	ch.daddy.clientIdentified(ch)

	{ // We report it in events
		m := mq.NewMessage(mq.TOPIC_GENERAL_EVENTS, "device_identified")
		m.Set("source", ch.Conn.Conn.RemoteAddr().String())
		m.Set("connection_id", fmt.Sprint(ch.Id))
		m.Set("device_id", ch.device.Id())
		ch.SendMessage(m)
	}

	{ // We save it in storage
		m := mq.NewMessage(mq.TOPIC_STORAGE, "store_ts")
		{
			data := sjson.New()
			m.Set("data", data)

			data.Set("host", HOSTNAME)
			data.Set("source", ch.Conn.Conn.RemoteAddr().String())
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

			data.Set("host", HOSTNAME)
			data.Set("source", ch.Conn.Conn.RemoteAddr().String())
			data.Set("connection_id", fmt.Sprint(ch.Id))
			data.Set("type", "device_connected")
		}
		m.Set("key", "dev-"+ch.device.Id())
		m.Set("date_uuid", mq.UUIDFromTime(ch.connectionTime))
		m.Set("type", "_server")
		ch.SendMessage(m)
	}

	err := ch.sendSettingsToSend()

	if err == nil {
		err = ch.sendCommands()
	}

	if err == nil {
		err = ch.checkCapacities()
	}

	if err == nil {
		err = ch.checkSettings()
	}

	return err
}

func (ch *ClientHandler) handleData(msg *pr.MessageDataSimple) error {
	//if par.LogLevel >= 7 {
	//	log.Print(ch, " --> \"", msg.Channel, "\" : ", msg.Data)
	//}

	if ch.device == nil {
		return errors.New(fmt.Sprintf("%v - Device not identified !", ch))
	}

	tokens := strings.SplitN(msg.Channel, ":", 2)

	switch tokens[0] {
	case "echo": // echo is just replied
		{
			ch.Conn.Send(msg)
		}

	default:
		{
			target := "converter-m2mp"

			if dct := ch.getDeviceChannelTranslator(); dct != nil {
				if t := dct.GetTarget(msg.Channel); t != nil {
					target = *t
				}
			}

			{ // We send the data to the converter, and it has to deal with it...
				m := mq.NewMessage(target, "data_simple")
				m.SetFrom(fmt.Sprintf(":connection_id:%d", ch.Id))
				m.Set("connection_id", fmt.Sprint(ch.Id))
				m.Set("device_id", ch.device.Id())
				m.Set("data", hex.EncodeToString(msg.Data))
				m.Set("channel", msg.Channel)
				ch.SendMessage(m)
			}
		}
	}
	return nil
}

func (ch *ClientHandler) handleDataArray(msg *pr.MessageDataArray) error {

	//if ch.LogLevel >= 7 {
	//	log.Debug("%s --> \"%s\" : %s", ch, msg.Channel, msg.Data)
	//}

	switch msg.Channel {
	case "_set":
		return ch.handleDataArraySettings(msg)
	case "_sta":
		return ch.handleDataArrayStatus(msg)
	case "_cmd":
		return ch.handleDataArrayCommand(msg)
	default:
		{
			target := "converter-m2mp"

			if dct := ch.getDeviceChannelTranslator(); dct != nil {
				if t := dct.GetTarget(msg.Channel); t != nil {
					target = *t
				}
			}

			{ // We send the data to the converter, and it has to deal with it...
				m := mq.NewMessage(target, "data_array")
				m.SetFrom(fmt.Sprintf(":connection_id:%d", ch.Id))
				m.Set("connection_id", fmt.Sprint(ch.Id))
				m.Set("device_id", ch.device.Id())
				data := make([]string, 0)
				for _, b := range msg.Data {
					data = append(data, hex.EncodeToString(b))
				}
				m.Set("data", data)
				m.Set("channel", msg.Channel)
				ch.SendMessage(m)
			}
		}
	}

	return nil
}

func (ch *ClientHandler) handleDataArraySettings(msg *pr.MessageDataArray) error {
	if ch.device == nil {
		log.Warning("We don't have a device yet !")
		return nil
	}

	requestType := string(msg.Data[0])

	if requestType == "ga" { // "get all" settings
		return ch.sendSettingsAll()
	}

	if requestType == "u" { // unknown settings
		for i := 1; i < len(msg.Data); i++ {
			v := string(msg.Data[i])
			ch.device.DelSetting(v)
		}
	}

	if strings.Contains(requestType, "g") { // "get" settings
		for i := 1; i < len(msg.Data); i++ {
			v := string(msg.Data[i])
			tokens := strings.SplitN(v, "=", 2)
			if len(tokens) == 2 {
				ch.device.AckSetting(tokens[0], tokens[1])
			}
		}
	}
	return nil
}

func (ch *ClientHandler) handleDataArrayStatus(msg *pr.MessageDataArray) error {
	if ch.device == nil {
		log.Warning("We don't have a device yet !")
		return nil
	}

	requestType := string(msg.Data[0])

	if strings.Contains(requestType, "g") {
		for i := 1; i < len(msg.Data); i++ {
			v := string(msg.Data[i])
			tokens := strings.SplitN(v, "=", 2)
			if len(tokens) == 2 {
				ch.device.SetStatus(tokens[0], tokens[1])
			}
		}
	}
	return nil
}

func (ch *ClientHandler) handleDataArrayCommand(msg *pr.MessageDataArray) error {
	if ch.device == nil {
		log.Warning("We don't have a device yet !")
		return nil
	}

	requestType := string(msg.Data[0])

	if strings.Contains(requestType, "a") { // acknowledge
		if len(msg.Data) >= 2 {
			cmdId := string(msg.Data[1])
			if len(msg.Data) > 2 {
				var buffer bytes.Buffer
				for _, d := range msg.Data[2:] {
					buffer.WriteString(string(d))
					buffer.WriteString("\n")
				}
				ch.device.AckCommandWithResponse(cmdId, buffer.String())
			} else {
				ch.device.AckCommand(cmdId)
			}
		}
	}

	return nil
}

func (ch *ClientHandler) sendSettings(settings map[string]string) error {
	msg := pr.NewMessageDataArray("_set")
	msg.AddString("sg")
	c := 0
	for k, v := range settings {
		value := fmt.Sprintf("%s=%s", k, v)
		lastTransmission := ch.settingLastTransmission[value]
		diff := time.Now().UTC().Sub(lastTransmission) / time.Second
		if diff > 40 {
			ch.settingLastTransmission[value] = time.Now().UTC()
			msg.AddString(value)
			c += 1
		} else {
			log.Debug("%s - Setting %s transmitted %d seconds ago.", ch, value, diff)
		}
	}
	if c != 0 {
		return ch.Send(msg)
	} else {
		return nil
	}
}

func (ch *ClientHandler) sendSettingsToSend() error {
	return ch.sendSettings(ch.device.SettingsToSend())
}

func (ch *ClientHandler) sendSettingsAll() error {
	log.Info("%s - Sending all settings", ch)
	return ch.sendSettings(ch.device.Settings())
}

func (ch *ClientHandler) checkSettings() error {
	if len(ch.device.Settings()) == 0 {
		msg := pr.NewMessageDataArray("_set")
		msg.AddString("ga")
		return ch.Send(msg)
	}
	return nil
}

func (ch *ClientHandler) sendCommands() error {

	for k, v := range ch.device.Commands() {
		msg := pr.NewMessageDataArray("_cmd")
		msg.AddString("e")
		msg.AddString(k)
		for _, s := range strings.Split(v, "\n") {
			msg.AddString(s)
		}
		if err := ch.Send(msg); err != nil {
			log.Debug("%s - Send( %s ): %s", ch, msg.Channel, msg.Data)
			return err
		}
	}
	return nil
}

func (ch *ClientHandler) String() string {
	return fmt.Sprintf("CH{Id=%d,Conn=%v}", ch.Id, ch.Conn)
}
