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
	par      *Config
}

func NewStorageService() *StorageService {
	svc := &StorageService{quitRc: make(chan int), par: LoadConfig()}
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
			if key, err = m.Get("key").String(); err != nil {
				log.Warning("Could not get key: %v", err)
				return
			}
			if dataType, err = m.Get("type").String(); err != nil {
				log.Warning("Could not get dataType: %v", err)
				return
			}
			if binData, err := m.Get("data").MarshalJSON(); err != nil {
				log.Warning("Could not get data: %v", err)
				return
			} else {
				data = string(binData)
			}

			if s_date_uuid, err := m.Get("date_uuid").String(); err != nil {
				log.Warning("Could not get date_uuid: %v", err)
				if date_nano, err := m.Get("date_nano").Int64(); err != nil {
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

			{ // We check if the data isn't in the future
				dataTime := date_uuid.Time()
				currentTime := time.Now().UTC()

				log.Debug("Saving key=%s, type=%s, time=%s, data=%s", key, dataType, dataTime, data)

				diff := dataTime.Sub(currentTime)

				if diff > time.Minute {
					log.Warning("Data is %v in the future. We can't save it !", diff)
				}
			}

			for c := 0; c < 10; c++ { // We try to save it up to 10 times
				if err = db.SaveTSUUID(key, dataType, &date_uuid, data); err == nil {
					return
				} else {
					log.Warning("Problem storing data: %v, attempt %d", err, c)
					time.Sleep(time.Millisecond * 500)
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
			log.Warning("Error while receiving message.")
			s.quitRc <- 1
		}
		s.handleMessaging(m)
	}
}

func main() {
	service := NewStorageService()
	log.Debug("Connecting to DB...")
	if err := db.NewSessionSimple(service.par.Db.Keyspace); err != nil {
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

	for i := 0; i < service.par.Storage.Actors; i++ {
		go service.runMessaging()
	}

	log.Debug("OK")

	os.Exit(<-service.quitRc)
}
