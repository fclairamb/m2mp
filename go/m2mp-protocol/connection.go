package m2mprotocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"
)

const (
	BUFFER_SIZE = 1024 // Buffer size
)

const (
	MODE_CLIENT = iota // Client mode
	MODE_SERVER = iota // Server mode
)

const (
	PROTO_IDENT_REQUEST  = 0x01 // Identification request
	PROTO_IDENT_RESPONSE = 0X01 // Identification response
	PROTO_CLIENT_PING    = 0x02 // Ping request (client to server)
	PROTO_SERVER_PONG    = 0x02 // Pong request (server to client)
	PROTO_SERVER_PING    = 0x03 // Ping request (server to client)
	PROTO_CLIENT_PONG    = 0x03 // Pong request (client to server)
	PROTO_DATA_SIMPLE_1B = 0x21
	PROTO_DATA_SIMPLE_2B = 0x41
	PROTO_DATA_SIMPLE_4B = 0x61
	PROTO_DATA_ARRAY_1B  = 0x22
	PROTO_DATA_ARRAY_2B  = 0x42
	PROTO_DATA_ARRAY_4B  = 0x62

	PROTO_MAX_SIZE_1B = 254     // 255 - 1 (for channel)
	PROTO_MAX_SIZE_2B = 65533   // 64K - 1 (for channel)
	PROTO_MAX_SIZE_4B = 1048576 // 1MB (we don't want to send more than 1MB at this stage)
)

type ProtoHandler struct {
	Conn         net.Conn
	mode         byte
	sendChannels map[string]int
	recvChannels map[int]string
	LogLevel     int
}

// The protocol handler doesn't have any channel handling, it should be done in a higher level

func newProtoHandle(c net.Conn) *ProtoHandler {
	return &ProtoHandler{Conn: c, sendChannels: make(map[string]int), recvChannels: make(map[int]string), LogLevel: 9}
}

func NewProtoHandlerClient(c net.Conn) (pt *ProtoHandler) {
	pt = newProtoHandle(c)
	pt.mode = MODE_CLIENT
	return pt
}

func NewProtoHandlerServer(c net.Conn) (pt *ProtoHandler) {
	pt = newProtoHandle(c)
	pt.mode = MODE_SERVER
	return pt
}

func (pt *ProtoHandler) Send(msg interface{}) error {
	if pt.LogLevel >= 7 {
		log.Printf("%s --> %#v", pt, msg)
	}
	switch m := msg.(type) {
	case *MessageDataSimple:
		{
			return pt.sendData(m)
		}

	case *MessageIdentResponse:
		{
			return pt.sendIdentificationResponse(m)
		}
	case *MessageIdentRequest:
		{
			return pt.sendIdentificationRequest(m)
		}
	case *MessagePingRequest:
		{
			return pt.sendPing(m)
		}
	case *MessagePingResponse:
		{
			return pt.sendPong(m)
		}
	}
	return errors.New(fmt.Sprint("I don't know how to send this : ", msg))
}

func (pt *ProtoHandler) sendData(msg *MessageDataSimple) error {
	//if pt.LogLevel >= 5 {
	//	log.Print(pt, " <-- \"", msg.Channel, "\" : ", msg.Data)
	//}

	var channelId int

	if id, ok := pt.sendChannels[msg.Channel]; ok {
		channelId = id
	} else {
		channelId = len(pt.sendChannels)
		if channelId >= 255 {
			pt.sendChannels = make(map[string]int)
			channelId = 0
		}
		frame := []byte{0x20, byte(1 + len(msg.Channel)), byte(channelId)}
		frame = append(frame, []byte(msg.Channel)...)

		pt.sendChannels[msg.Channel] = channelId

		if pt.LogLevel >= 8 {
			log.Print(pt, " <-- \"", msg.Channel, "\" created on ", channelId)
		}

		_, err := pt.Conn.Write(frame)
		return err
	}

	return nil
}

func (pt *ProtoHandler) sendPing(msg *MessagePingRequest) (err error) {
	if pt.mode == MODE_CLIENT {
		_, err = pt.Conn.Write([]byte{PROTO_CLIENT_PING, msg.Data})
	} else {
		_, err = pt.Conn.Write([]byte{PROTO_SERVER_PING, msg.Data})
	}
	return
}

func (pt *ProtoHandler) sendPong(msg *MessagePingResponse) (err error) {
	if pt.mode == MODE_CLIENT {
		_, err = pt.Conn.Write([]byte{PROTO_CLIENT_PONG, msg.Data})
	} else {
		_, err = pt.Conn.Write([]byte{PROTO_SERVER_PONG, msg.Data})
	}
	return
}

func (pt *ProtoHandler) sendIdentificationResponse(msg *MessageIdentResponse) (err error) {
	var ok byte
	if msg.Ok {
		ok = 0x01
	} else {
		ok = 0x00
	}
	_, err = pt.Conn.Write([]byte{PROTO_IDENT_RESPONSE, ok})
	return
}

func (pt *ProtoHandler) sendIdentificationRequest(msg *MessageIdentRequest) (err error) {
	data := []byte{PROTO_IDENT_REQUEST, byte(len(msg.Ident))}
	data = append(data, []byte(msg.Ident)...)
	_, err = pt.Conn.Write(data)
	return
}

func (pt *ProtoHandler) Recv() interface{} {
	msg := pt.actualRecv()
	if pt.LogLevel >= 7 {
		log.Printf("%s <-- %#v", pt, msg)
	}
	return msg
}

func (pt *ProtoHandler) actualRecv() interface{} {

	// We're currently re-allocating the buffer for every message
	buffer := make([]byte, BUFFER_SIZE)
	for {
		// This will be restored once we keep the buffer between messages
		// if cap(buffer) > BUFFER_SIZE*2 {
		// 	 buffer = make([]byte, BUFFER_SIZE)
		// }

		_, err := pt.Conn.Read(buffer[0:2])

		if err != nil {
			if pt.LogLevel >= 3 {
				log.Print("Problem with ", pt, " : ", err)
			}
			return &EventDisconnected{Error: err}
		}

		switch buffer[0] {

		case 0x01: // Identification
			{
				if pt.mode == MODE_SERVER {
					size := buffer[1]
					pt.Conn.Read(buffer[:size])
					ident := string(buffer[:size])
					return &MessageIdentRequest{Ident: ident}
				} else {
					return &MessageIdentResponse{Ok: (buffer[1] == 0x01)}
				}
			}

		case 0x02, 0x03: // Ping
			{
				if pt.mode == MODE_SERVER { // Server
					if buffer[0] == PROTO_CLIENT_PING { // receives a client ping
						return &MessagePingRequest{Data: buffer[1]}
					} else if buffer[0] == PROTO_CLIENT_PONG { // receives a client pong
						return &MessagePingResponse{Data: buffer[1]}
					}
				} else { // Client
					if buffer[0] == PROTO_SERVER_PING { // receives a server ping
						return &MessagePingRequest{Data: buffer[1]}
					} else if buffer[0] == PROTO_SERVER_PONG {
						return &MessagePingResponse{Data: buffer[1]}
					}
				}
			}

		case 0x20: // Named channel
			{
				size := buffer[1]
				pt.Conn.Read(buffer[:size])
				channelId := int(buffer[0])
				channelName := string(buffer[1:size])
				pt.recvChannels[channelId] = channelName
				if pt.LogLevel >= 7 {
					log.Print(pt, " --> \"", channelName, "\" created on ", channelId)
				}
			}
		// All the data messages
		case 0x21, 0x41, 0x61, 0x22, 0x42, 0x62:
			{
				// We handle all size of messages at the same place
				var sizeLength int
				ft := buffer[0]
				switch ft {
				case 0x21, 0x22:
					sizeLength = 1 // 1 byte sized (0 to 255)
				case 0x41, 0x42:
					sizeLength = 2 // 2 bytes sized (0 to 64K)
				case 0x61, 0x62:
					sizeLength = 4 // 4 bytes sized (0 to 4G)
				}
				// We get the necessary remaining bytes of the size
				pt.Conn.Read(buffer[2 : sizeLength+1])

				// We convert this to a size
				var size int
				{
					s, _ := binary.Uvarint(buffer[1 : sizeLength+1])
					size = int(s)
				}

				// We might have to increase the buffer size
				if size > cap(buffer) {
					if pt.LogLevel >= 7 {
						log.Printf("%s - Increasing buffer size to %d bytes.", pt, size)
					}
					buffer = make([]byte, size)
				}

				// We read everything (we get rid of existing buffer content)
				pt.Conn.Read(buffer[:size])

				// We get the channel id
				channelId := int(buffer[0])
				channelName := pt.recvChannels[channelId]
				switch ft {

				case 0x21, 0x41, 0x61: // Single byte array messages
					{
						return &MessageDataSimple{Channel: channelName, Data: buffer[1 : size-1]}
					}
				case 0x22, 0x42, 0x62: // Array of byte arrays messages
					{
						offset := 1
						data := make([][]byte, 5)
						for offset < size {
							s, _ := binary.Uvarint(buffer[offset : offset+sizeLength])
							subSize := int(s)
							offset += sizeLength
							pt.Conn.Read(buffer[offset : offset+subSize])
							data = append(data, buffer[offset:offset+subSize])
							offset += subSize
						}
						return &MessageDataArray{Channel: channelName, Data: data}
					}
				}
			}
		default:
			{
				log.Printf("%s - Unhandled header: 0x%02X", pt, buffer[0:1])
				pt.Conn.Close()
				return &EventDisconnected{Error: errors.New(fmt.Sprintf("Unhandled header: 0x%02X", buffer[0:1]))}
			}
		}
	}

	log.Fatal("What is happening ?")
	return nil
}

func (pt *ProtoHandler) String() string {
	return fmt.Sprintf("M2MP{Conn=%v}", pt.Conn.RemoteAddr())
}
