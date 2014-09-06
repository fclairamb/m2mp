package main

import (
	"encoding/binary"
	"encoding/hex"
	sjson "github.com/bitly/go-simplejson"
	db "github.com/fclairamb/m2mp/go/m2mp-db"
	mq "github.com/fclairamb/m2mp/go/m2mp-messaging"
	//	"github.com/gocql/gocql"
	"bytes"
	"fmt"
	"os"
	"time"
)

type ConverterService struct {
	mqClient *mq.Client
	quitRc   chan int
	par      *Config
}

func NewConverterService() *ConverterService {
	svc := &ConverterService{quitRc: make(chan int), par: LoadConfig()}
	return svc
}

func (this *ConverterService) convertMessageSpecial(src, store *mq.JsonWrapper, raw []byte) error {
	cmd := string(raw)
	store.Set("data", cmd)

	switch cmd {
	case "disconnect_me":
		{
			send := mq.NewMessage(src.From(), "disconnect")
			if err := this.mqClient.Publish(send); err != nil {
				log.Warning("Error sending disconnect message: %v", err)
			}
		}
	case "disconnect_us":
		{
			if deviceId, err := src.Get("device_id").String(); err != nil {
				log.Warning("Could not get device_id ! \"disconnect_us\" cannot be applied: %v", err)
			} else {
				send := mq.NewMessage(fmt.Sprintf("receivers;device_id=%s", deviceId), "disconnect")
				if err := this.mqClient.Publish(send); err != nil {
					log.Warning("Error sending disconnect message: %v", err)
				}
			}
		}
	default:
		log.Warning("Special command \"%s\" not understood !", cmd)
	}

	return nil
}

func (this *ConverterService) convertMessageLoc(src, store *mq.JsonWrapper, raw []byte) error {
	log.Warning("Reached ?")

	rawlen := len(raw)
	buf := bytes.NewReader(raw)

	data := sjson.New()
	store.Set("data", data)

	if rawlen >= 4 { // time
		rawlen -= 4
		var timestamp uint32
		binary.Read(buf, binary.BigEndian, &timestamp)
		store.Set("date_uuid", mq.UUIDFromTime(time.Unix(int64(timestamp), 0)))
	}
	if rawlen == 1 { // If we have only one byte left it means we only have the number of satellites in view
		var sat uint8
		binary.Read(buf, binary.BigEndian, &sat)
		data.Set("sat", sat)
	}
	if rawlen >= 8 {
		rawlen -= 8
		var lat, lon float32
		binary.Read(buf, binary.BigEndian, &lat)
		binary.Read(buf, binary.BigEndian, &lon)
		data.Set("lat", lat)
		data.Set("lon", lon)
	}
	if rawlen >= 2 {
		rawlen -= 2
		var spd uint16
		binary.Read(buf, binary.BigEndian, &spd)
		data.Set("spd", spd)
	}
	if rawlen >= 2 {
		rawlen -= 2
		var alt uint16
		binary.Read(buf, binary.BigEndian, &alt)
		data.Set("alt", alt)
	}

	return nil
}

func (this *ConverterService) convertMessage(src *mq.JsonWrapper) {
	store := mq.NewMessage(mq.TOPIC_STORAGE, "store_ts")
	channel := src.Get("channel").MustString("")
	store.Set("date_uuid", mq.UUIDFromTime(src.Time()))
	store.Set("key", "dev-"+src.Get("device_id").MustString(""))
	store.Set("type", channel)

	raw, err := hex.DecodeString(src.Get("data").MustString(""))
	if err != nil {
		log.Warning("Wrong data: %v", err)
	}

	if err == nil {
		switch channel {
		case "sen:loc":
			err = this.convertMessageLoc(src, store, raw)
		case "_special":
			err = this.convertMessageSpecial(src, store, raw)
		default:
			store.Set("data", string(raw))
		}
	}

	if err == nil {
		this.mqClient.Publish(store)
	} else {
		log.Warning("Error: %v", err)
	}
}

func (this *ConverterService) handleMessaging(m *mq.JsonWrapper) {
	log.Debug("Handling %v", m)
	call := m.Call()

	switch call {
	case "data_simple":
		this.convertMessage(m)

	case "quit":
		this.quitRc <- 3 // It's not an error, and it's not 0 or 2 so that supervisor restarts it

	default:
		log.Warning("Message could not be handled: %v", m)

	}
}

func (s *ConverterService) runMessaging() error {
	for {
		m := <-s.mqClient.Recv
		if m == nil {
			log.Critical("End of MQ reception")
			return nil
		}
		s.handleMessaging(m)
	}
}

func main() {
	service := NewConverterService()
	log.Debug("Connecting to DB...")
	if err := db.NewSessionSimple("ks_test"); err != nil {
		log.Fatal("DB error: ", err)
	}
	defer db.Close()

	log.Debug("Connecting to NSQ...")
	{
		var err error
		if service.mqClient, err = mq.NewClient(service.par.Mq.Topic, service.par.Mq.Channel); err != nil {
			log.Fatal("MQ error: ", err)
		}
	}

	service.mqClient.Start(service.par.Mq.Server)

	go service.runMessaging()

	log.Debug("OK")

	os.Exit(<-service.quitRc)
}
