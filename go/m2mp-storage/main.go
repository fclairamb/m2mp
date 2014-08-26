package main

import (
	db "github.com/fclairamb/m2mp/go/m2mp-db"
	mq "github.com/fclairamb/m2mp/go/m2mp-messaging"
	"os"
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
	log.Debug("Handling ", m)
	call := m.Call()

	switch call {
	case "store_ts":
		{
			data := m.Data.Get("data")
			log.Debug("Data: %#v", data)
		}

	case "quit":
		{
			this.quitRc <- 3
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
