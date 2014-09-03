package main

import (
	db "github.com/fclairamb/m2mp/go/m2mp-db"
	mq "github.com/fclairamb/m2mp/go/m2mp-messaging"
	"github.com/gocql/gocql"
	"os"
	"time"
)

type StorageService struct {
	mqClient *mq.Client
	quitRc   chan int
	par      *Parameters
}

func NewStorageService() *StorageService {
	svc := &StorageService{quitRc: make(chan int), par: NewParameters()}
	return svc
}

func (this *StorageService) handleMessaging(m *mq.JsonWrapper) {
	log.Debug("Handling %v", m)
	call := m.Call()

	switch call {
	case "store_ts":
		{
			var key, dataType, data string
			var date_uuid gocql.UUID
			var err error
			if key, err = m.Data.Get("key").String(); err != nil {
				log.Warning("Could not get key: %v", err)
				return
			}
			if dataType, err = m.Data.Get("type").String(); err != nil {
				log.Warning("Could not get dataType: %v", err)
				return
			}
			if binData, err := m.Data.Get("data").MarshalJSON(); err != nil {
				log.Warning("Could not get data: %v", err)
				return
			} else {
				data = string(binData)
			}

			if s_date_uuid, err := m.Data.Get("date_uuid").String(); err != nil {
				log.Warning("Could not get date_uuid: %v", err)
				if date_nano, err := m.Data.Get("date_nano").Int64(); err != nil {
					log.Warning("Could not get date_nano either: %v", err)
					return
				} else {
					date_uuid = gocql.UUIDFromTime(time.Unix(0, date_nano))
				}
			} else {
				if date_uuid, err = gocql.ParseUUID(s_date_uuid); err != nil {
					log.Warning("Could not parse data_uuid: %v", err)
					return
				}
			}

			log.Debug("Saving key=%s, type=%s, time=%s, data=%s", key, dataType, date_uuid.Time(), data)

			for c := 0; c < 3; c++ {
				if err = db.SaveTSUUID(key, dataType, &date_uuid, data); err != nil {
					log.Warning("Problem storing data: %v", err)
				} else {

				}
			}
		}

	case "quit":
		{
			this.quitRc <- 3 // It's not an error, and it's not 0 or 2 so that supervisor restarts it
		}

	default:
		{
			log.Warning("Message could not be handled: %v", m)
		}
	}
}

func (s *StorageService) runMessaging() error {
	for {
		m := <-s.mqClient.Recv
		if m == nil {
			return nil
		}
		s.handleMessaging(m)
	}
}

func main() {
	service := NewStorageService()
	log.Debug("Connecting to DB...")
	if err := db.NewSessionSimple("ks_test"); err != nil {
		log.Fatal("DB error: ", err)
	}
	defer db.Close()

	log.Debug("Connecting to NSQ...")
	{
		var err error
		if service.mqClient, err = mq.NewClient(service.par.MQTopic, service.par.MQChannel); err != nil {
			log.Fatal("MQ error: ", err)
		}
	}

	service.mqClient.Start(service.par.MQServer)

	go service.runMessaging()

	log.Debug("OK")

	os.Exit(<-service.quitRc)
}
