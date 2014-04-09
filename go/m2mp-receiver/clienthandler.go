package main

import (
	"encoding/binary"
	"fmt"
	ent "github.com/fclairamb/m2mp/go/m2mp-db/entities"
	"log"
	"net"
	"strings"
	"time"
)

type ClientHandler struct {
	Id             int
	Conn           net.Conn
	daddy          *Server
	connectionTime time.Time
	device         *ent.Device
	sendChannels   map[string]int
	recvChannels   map[int]string
}

func NewClientHandler(daddy *Server, id int, conn net.Conn) *ClientHandler {
	ch := &ClientHandler{
		daddy:          daddy,
		Id:             id,
		Conn:           conn,
		connectionTime: time.Now().UTC(),
		sendChannels:   make(map[string]int),
		recvChannels:   make(map[int]string)}

	return ch
}

func (ch *ClientHandler) Start() {
	if par.LogLevel >= 3 {
		log.Print("Added ", ch, " / ", ch.daddy.NbClients())
	}
	go ch.handleConnection()
}

func (ch *ClientHandler) end() {
	ch.daddy.removeClientHandler(ch)
	if par.LogLevel >= 3 {
		log.Print("Removed ", ch)
	}
}

func (ch *ClientHandler) sendData(channel string, data []byte) error {
	if par.LogLevel >= 7 {
		log.Print(ch, " <-- \"", channel, "\" : ", data)
	}

	var channelId int

	if id, ok := ch.sendChannels[channel]; ok {
		channelId = id
	} else {
		channelId = len(ch.sendChannels)
		if channelId >= 255 {
			ch.sendChannels = make(map[string]int)
			channelId = 0
		}
		frame := []byte{0x20, byte(1 + len(channel)), byte(channelId)}
		frame = append(frame, []byte(channel)...)

		ch.sendChannels[channel] = channelId

		if par.LogLevel >= 7 {
			log.Print(ch, " --> \"", channel, "\" created on ", channelId)
		}

		_, err := ch.Conn.Write(frame)
		return err
	}

	if par.LogLevel >= 7 {
		log.Print(ch, " --> \"", channel, "\" : ", data)
	}

	return nil
}

func (ch *ClientHandler) handleData(channel string, data []byte) error {
	if par.LogLevel >= 7 {
		log.Print(ch, " --> \"", channel, "\" : ", data)
	}

	tokens := strings.SplitN(channel, ":", 2)

	switch tokens[0] {
	case "_set": // settings have their own logic
		{

		}

	case "_cmd": // commands have their own logic
		{

		}
	case "echo": // echo is just replied
		{
			ch.sendData(channel, data)
		}

	case "sen": // sensor is just stored
		{
			if ch.device != nil {
				ch.device.SaveTSTime(channel, time.Now().UTC(), string(data))
			}
		}
	}

	return nil
}

func (ch *ClientHandler) handleConnection() {
	// When things will go wrong, we will end it properly
	defer ch.end()
	buffer := make([]byte, 1024)
	for {

		_, err := ch.Conn.Read(buffer[0:2])

		if err != nil {
			log.Print("Problem with ", ch, " : ", err)
			break
		}

		switch buffer[0] {

		case 0x01: // Identification
			{
				size := buffer[1]
				ch.Conn.Read(buffer[:size])
				ident := string(buffer[:size])
				if par.LogLevel >= 5 {
					log.Print(ch, " --> Identification: ", ident)
					ch.device, err = ent.NewDeviceByIdentCreate(ident)
					if err != nil {
						log.Print("Problem with ", ch, " : ", err)
					}
				}

				// OK
				if err == nil {
					ch.Conn.Write([]byte{0x01, 0x01})
				} else {
					ch.Conn.Write([]byte{0x01, 0x00})
				}
			}

		case 0x02: // Ping
			{
				if par.LogLevel >= 7 {
					log.Print(ch, " - Ping: ", buffer[1])
				}
				ch.Conn.Write(buffer[0:2])
			}

		case 0x20: // Named channel
			{
				size := buffer[1]
				ch.Conn.Read(buffer[:size])
				channelId := int(buffer[0])
				channelName := string(buffer[1:size])
				ch.recvChannels[channelId] = channelName
				if par.LogLevel >= 7 {
					log.Print(ch, " --> \"", channelName, "\" created on ", channelId)
				}
			}
		case 0x21, 0x41, 0x61:
			{
				// We handle all size of messages at the same place
				var sizeLength int
				switch buffer[0] {
				case 0x21:
					sizeLength = 1 // 1 byte sized (0 to 255)
				case 0x41:
					sizeLength = 2 // 2 bytes sized (0 to 64K)
				case 0x61:
					sizeLength = 4 // 4 bytes sized (0 to 4G)
				}
				// We get the necessary remaining bytes of the size
				ch.Conn.Read(buffer[2 : sizeLength+1])

				// We convert this to a size
				var size int
				{
					s, _ := binary.Uvarint(buffer[1 : sizeLength+1])
					size = int(s)
				}

				// We might have to increase the buffer size
				if size > cap(buffer) {
					if par.LogLevel >= 7 {
						log.Printf("%s - Increasing buffer size to %d bytes.", ch, size)
					}
					buffer = make([]byte, size)
				}

				// We read everything (we get rid of existing buffer content)
				ch.Conn.Read(buffer[:size])

				// We get the channel id
				channelId := int(buffer[0])
				channelName := ch.recvChannels[channelId]

				// And we finally handle the data
				err = ch.handleData(channelName, buffer[1:size-1])
			}
		case 0x22, 0x42, 0x62:
			{
				// We handle all size of messages at the same place
				var sizeLength int
				switch buffer[0] {
				case 0x21:
					sizeLength = 1 // 1 byte sized (0 to 255)
				case 0x41:
					sizeLength = 2 // 2 bytes sized (0 to 64K)
				case 0x61:
					sizeLength = 4 // 4 bytes sized (0 to 4G)
				}
				// We get the necessary remaining bytes of the size
				ch.Conn.Read(buffer[2 : sizeLength+1])

				// We convert this to a size
				var size int
				{
					s, _ := binary.Uvarint(buffer[1 : sizeLength+1])
					size = int(s)
				}

				// We might have to increase the buffer size
				if size > cap(buffer) {
					if par.LogLevel >= 7 {
						log.Printf("%s - Increasing buffer size to %d bytes.", ch, size)
					}
					buffer = make([]byte, size)
				}

				// We read everything (we get rid of existing buffer content)
				ch.Conn.Read(buffer[:size])

				// We get the channel id
				channelId := int(buffer[0])
				channelName := ch.recvChannels[channelId]

				// And we finally handle the data
				//TODO: later
			}
		default:
			{
				log.Printf("%s - Unhandled header: 0x%02X", ch, buffer[0:1])
				ch.Conn.Close()
				return
			}
		}
	}
}

func (ch *ClientHandler) String() string {
	return fmt.Sprintf("CH{Id=%d,Conn=%v}", ch.Id, ch.Conn.RemoteAddr())
}
