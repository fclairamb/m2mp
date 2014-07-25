package main

import (
	"bytes"
	"fmt"
	ent "github.com/fclairamb/m2mp/go/m2mp-db/entities"
	mq "github.com/fclairamb/m2mp/go/m2mp-messaging"
	pr "github.com/fclairamb/m2mp/go/m2mp-protocol"
	"net"
	"strings"
	"time"
)

type ClientHandler struct {
	Id                      int
	daddy                   *Server
	connectionTime          time.Time
	device                  *ent.Device
	deviceChannelTranslator *ent.DeviceChannelTrans
	Conn                    *pr.ProtoHandler
	connRecv                chan interface{}
	LogLevel                int
	lastReceivedData        time.Time
	lastSentData            time.Time
	ticker                  *time.Ticker
	pingCounter             byte
	//connSend         chan interface{}
}

func NewClientHandler(daddy *Server, id int, conn net.Conn) *ClientHandler {
	now := time.Now().UTC()
	ch := &ClientHandler{
		daddy:            daddy,
		Id:               id,
		Conn:             pr.NewProtoHandlerServer(conn),
		connectionTime:   now,
		LogLevel:         9,
		connRecv:         make(chan interface{}, 3),
		ticker:           time.NewTicker(time.Second * 30),
		lastReceivedData: now,
		lastSentData:     now,
		//connSend:       make(chan interface{}, 3),
	}

	return ch
}

func (ch *ClientHandler) Start() {
	if ch.LogLevel >= 3 {
		log.Debug("Added %s", ch)
	}
	go ch.runRecv()
	go ch.runCoreHandling()

	{
		m := mq.NewMessageEvent("device_connected")
		m.Data.Set("source", ch.Conn.Conn.RemoteAddr().String())
		m.Data.Set("connection_id", fmt.Sprint(ch.Id))
		ch.daddy.SendMessage(m)
	}
}

func (ch *ClientHandler) Close() error {
	return ch.Conn.Conn.Close()
}

func (ch *ClientHandler) end() {
	ch.daddy.removeClientHandler(ch)
	if ch.LogLevel >= 3 {
		log.Debug("Removed %s", ch)
	}
	ch.ticker.Stop()

	{
		m := mq.NewMessageEvent("device_disconnected")
		m.Data.Set("source", ch.Conn.Conn.RemoteAddr().String())
		m.Data.Set("connection_id", fmt.Sprint(ch.Id))
		m.Data.Set("connection_duration", fmt.Sprint(int64(time.Now().UTC().Sub(ch.connectionTime).Seconds())))
		if ch.device != nil {
			m.Data.Set("device_id", ch.device.Id())
		}
		ch.daddy.SendMessage(m)
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
	if ch.LogLevel >= 5 {
		log.Debug("%s - Considering current status (%s)", ch, now)
	}
	if now.Sub(ch.lastReceivedData) > time.Duration(time.Minute*15) &&
		now.Sub(ch.lastSentData) > time.Duration(time.Second*30) {
		log.Debug("%s - Sending ping request", ch)
		ch.Send(&pr.MessagePingRequest{Data: ch.pingCounter})
		ch.pingCounter += 1
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
					ch.receivedData()
					ch.handleData(m)
				case *pr.MessageDataArray:
					ch.receivedData()
					ch.handleDataArray(m)
				case *pr.MessageIdentRequest:
					ch.receivedData()
					ch.handleIdentRequest(m)
				case *pr.MessagePingRequest:
					{
						ch.receivedData()
						ch.Conn.Send(&pr.MessagePingResponse{Data: m.Data})
					}
					// If this is a disconnection event, we should quit the current go routine
				case *pr.EventDisconnected:
					{
						return
					}
				}

			}
		case <-ch.ticker.C:
			{
				if ch.LogLevel >= 5 {
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
	if err == nil {
		err = ch.Send(&pr.MessageIdentResponse{Ok: true})
		if err == nil {
			err = ch.justIdentified()
		}
		return err
	} else {
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

	{
		m := mq.NewMessageEvent("device_identified")
		m.Data.Set("source", ch.Conn.Conn.RemoteAddr().String())
		m.Data.Set("connection_id", fmt.Sprint(ch.Id))
		m.Data.Set("device_id", ch.device.Id())
		ch.daddy.SendMessage(m)
	}

	return err
}

func (ch *ClientHandler) handleData(msg *pr.MessageDataSimple) error {
	//if par.LogLevel >= 7 {
	//	log.Print(ch, " --> \"", msg.Channel, "\" : ", msg.Data)
	//}

	tokens := strings.SplitN(msg.Channel, ":", 2)

	switch tokens[0] {
	case "echo": // echo is just replied
		{
			ch.Conn.Send(msg)
		}

	default:
		{
			if dct := ch.getDeviceChannelTranslator(); dct != nil {
				if target := dct.GetTarget(msg.Channel); target != nil {
					msg := mq.NewJsonWrapper()
					msg.SetTo(*target)
					msg.SetFrom(fmt.Sprintf(":conn:%d", ch.Id))
					msg.SetCall("data_simple")
					ch.daddy.SendMessage(msg)
					log.Debug("Sending %s", msg)
				} else if tokens[0] == "sen" {
					ch.device.SaveTSTime(msg.Channel, time.Now().UTC(), string(msg.Data))
				}
			}
		}
	}

	return nil
}

func (ch *ClientHandler) handleDataArray(msg *pr.MessageDataArray) error {

	if ch.LogLevel >= 7 {
		log.Debug("%s --> \"%s\" : %s", ch, msg.Channel, msg.Data)
	}

	if msg.Channel == "_set" {
		return ch.handleDataArraySettings(msg)
	} else if msg.Channel == "_sta" {
		return ch.handleDataArrayStatus(msg)
	} else if msg.Channel == "_cmd" {
		return ch.handleDataArrayCommand(msg)
	}

	return nil
}

func (ch *ClientHandler) handleDataArraySettings(msg *pr.MessageDataArray) error {
	if ch.device == nil {
		log.Warning("We don't have a device yet !")
		return nil
	}

	requestType := string(msg.Data[0])

	if requestType == "ga" {
		return ch.sendSettingsAll()
	}

	if strings.Contains(requestType, "g") {
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
		msg.AddString(fmt.Sprintf("%s=%s", k, v))
		c += 1
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
