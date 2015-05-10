package main

import (
	"errors"
	"fmt"
	db "github.com/fclairamb/m2mp/go/m2mp-db"
	dbent "github.com/fclairamb/m2mp/go/m2mp-db/entities"
	mq "github.com/fclairamb/m2mp/go/m2mp-messaging"
	"github.com/gocql/gocql"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

type StorageService struct {
	mqClient        *mq.Client
	quitRc          chan int
	par             *Config
	nbRequests      int
	nbRequestsMutex sync.Mutex
}

func NewStorageService() *StorageService {
	svc := &StorageService{quitRc: make(chan int), par: LoadConfig()}
	return svc
}

func (this *StorageService) storeTs(m *mq.JsonWrapper) error {
	var key, dataType, data string
	var date_uuid gocql.UUID
	var err error
	if key, err = m.Get("key").String(); err != nil {
		log.Warning("Could not get key: %v", err)
		return err
	}
	if dataType, err = m.Get("type").String(); err != nil {
		log.Warning("Could not get dataType: %v", err)
		return err
	}
	if binData, err := m.Get("data").MarshalJSON(); err != nil {
		log.Warning("Could not get data: %v", err)
		return err
	} else {
		data = string(binData)
	}

	if s_date_uuid, err := m.Get("date_uuid").String(); err != nil {
		log.Warning("Could not get date_uuid: %v", err)
		if date_nano, err := m.Get("date_nano").Int64(); err != nil {
			log.Warning("Could not get date_nano either: %v", err)
			return err
		} else {
			date_uuid = gocql.UUIDFromTime(time.Unix(0, date_nano))
		}
	} else {
		if date_uuid, err = gocql.ParseUUID(s_date_uuid); err != nil {
			log.Warning("Could not parse data_uuid: %v", err)
			return err
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
			break
		} else {
			log.Warning("Problem storing data: %v, attempt %d", err, c)
			time.Sleep(time.Millisecond * 500)
		}
	}

	if err != nil {
		log.Warning("Saving %v triggered this error: %v", m, err)
		return err
	}

	if listedSensor.MatchString(dataType) {
		log.Debug("This is a listed sensor: %s", dataType)
		sub := listedSensor.FindStringSubmatch(dataType)
		if len(sub) > 1 {
			sensorName := sub[1]
			if strings.HasPrefix(key, "dev-") {
				if devId, err := gocql.ParseUUID(key[4:]); err != nil {
					// This is critical because this should never ever happen
					log.Critical("Bad device ID: %s", err)
					return err
				} else {
					device := dbent.NewDeviceById(devId)
					device.MarkListedSensor(sensorName)
					//log.Debug("Wrote to %v", rn)
				}
			} else {
				msg := fmt.Sprintf("Cannot store a %s for key %s as it's not a device !", dataType, key)
				log.Warning(msg)
				return errors.New(msg)
			}
		}
	}

	return nil
}

var listedSensor = regexp.MustCompile("(?i)lsen:([a-z0-9\\-_]+)")

func (this *StorageService) handleMessaging(m *mq.JsonWrapper) {
	log.Debug("Handling %v", m)
	call := m.Call()

	switch call {
	case "store_ts":
		this.storeTs(m)

	case "quit":
		this.quitRc <- 3 // It's not an error, and it's not 0 or 2 so that supervisor restarts it

	default:
		log.Warning("Message could not be handled: %v", m)
	}
}

func (this *StorageService) newRequest() {
	this.nbRequestsMutex.Lock()
	defer this.nbRequestsMutex.Unlock()

	this.nbRequests += 1

	if this.nbRequests%1000 == 0 {
		log.Info("Handled %d requests", this.nbRequests)
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
		s.newRequest()
	}
}

func main() {
	service := NewStorageService()
	LoadLog()
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

	log.Notice("Ready !")

	os.Exit(<-service.quitRc)
}
