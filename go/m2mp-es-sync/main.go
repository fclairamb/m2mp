package main

import (
	db "github.com/fclairamb/m2mp/go/m2mp-db"
	ent "github.com/fclairamb/m2mp/go/m2mp-db/entities"
	mq "github.com/fclairamb/m2mp/go/m2mp-messaging"
	"github.com/gocql/gocql"
	es "github.com/olivere/elastic"
	stdlog "log"
	"os"
	"strings"
	"sync"
	"time"
)

type IndexService struct {
	mqClient        *mq.Client // Messaging client
	quitRc          chan int   // Waiting channel
	par             *Config    // Parameters
	nbRequests      int        // Number of processed requests
	nbRequestsMutex sync.Mutex // Requests sync counter
	esClient        *es.Client // Elasticsearch client
}

// These are the only two events we care about
const (
	CALL_DEVICE_IDENTIFIED   = "device_identified"
	CALL_DEVICE_DISCONNECTED = "device_disconnected"
)

func NewIndexService() *IndexService {
	return &IndexService{quitRc: make(chan int), par: LoadConfig()}
}

func (this *IndexService) newRequest() {
	this.nbRequestsMutex.Lock()
	defer this.nbRequestsMutex.Unlock()

	this.nbRequests += 1

	if this.nbRequests%10 == 0 {
		log.Info("Handled %d requests", this.nbRequests)
	}
}

// As olivere/elastic doesn't support mapping change
// it's simpler to save UUID with "_" instead of "-".
type ESDevice struct {
	Ident           string    `json:"ident"`
	Name            string    `json:"name"`
	OwnerId         string    `json:"owner_id"`
	OwnerName       string    `json:"owner_name"`
	DomainId        string    `json:"domain_id"`
	DomainName      string    `json:"domain_name"`
	DistributorId   string    `json:"distributor_user_id"`
	DistributorName string    `json:"distributor_user_name"`
	LastConnection  time.Time `json:"last_connection"`
}

func (this *IndexService) updateDevice(sDeviceId string) error {
	log.Info("Updating device \"%s\"", sDeviceId)
	deviceId, err := gocql.ParseUUID(sDeviceId)
	if err != nil {
		log.Warning("Could not parse \"%s\"", deviceId)
		return err
	}

	device := ent.NewDeviceById(deviceId)

	esDevice := &ESDevice{}

	// We get the basic device's info
	esDevice.Ident = device.Ident()
	esDevice.Name = device.Name()

	// Then we fetch the owner's info
	{
		owner := device.Owner()
		if owner != nil {
			esDevice.OwnerId = strings.Replace(sDeviceId, "-", "_", -1)
			esDevice.OwnerName = owner.Name()
		}
	}

	// Then the domain's info
	{
		domain := device.Domain()
		if domain != nil {
			esDevice.DomainName = domain.Name()
			esDevice.DomainId = strings.Replace(domain.Id(), "-", "_", -1)

			distributor := domain.Distributor()
			if distributor != nil {
				esDevice.DistributorName = distributor.Name()
				esDevice.DistributorId = strings.Replace(distributor.Id(), "-", "_", -1)
			}
		}
	}

	// Then the last connection's time
	{
		if td := device.LastData("_server:connected"); td != nil {
			esDevice.LastConnection = td.Time()
		}
	}

	log.Info("Indexing %s: %#v", sDeviceId, esDevice)
	_, err = this.esClient.Index().
		Index(this.par.Elasticsearch.Index).
		Type("Device").
		Id(sDeviceId).
		BodyJson(esDevice).
		Do()
	if err != nil {
		log.Warning("Error indexing %s: %v", sDeviceId, err)
	}
	return err
}

func (this *IndexService) runMessaging() {
	for {
		msg := <-this.mqClient.Recv
		log.Info("Received: %s", msg)
		this.newRequest()
		//log.Info("call = %s", msg.Call())
		switch msg.Call() {
		case CALL_DEVICE_IDENTIFIED, CALL_DEVICE_DISCONNECTED:
			{
				deviceId := msg.Get("device_id").MustString("")
				if deviceId != "" {
					this.updateDevice(deviceId)
				}
			}
		}
	}
}

func main() {
	service := NewIndexService()
	LoadLog()
	log.Debug("Connecting to DB...")
	var err error
	if err = db.NewSessionSimple(service.par.Db.Keyspace); err != nil {
		log.Fatal("DB error: ", err)
	}
	defer db.Close()

	log.Debug("Connecting to ES...")
	es.SetSniff(false)
	es.SetHealthcheck(false)
	es.SetURL("http://localhost:9200")
	if service.esClient, err = es.NewClient(
		es.SetErrorLog(stdlog.New(os.Stderr, "[ELASTIC] ", stdlog.LstdFlags)),
		es.SetInfoLog(stdlog.New(os.Stdout, "[ELASTIC] ", stdlog.LstdFlags))); err != nil {
		log.Fatal("ES error: ", err)
	}

	log.Debug("Checking index \"%s\" ...", service.par.Elasticsearch.Index)
	if exists, err := service.esClient.IndexExists(service.par.Elasticsearch.Index).Do(); err != nil {
		log.Fatal("ES error: ", err)
	} else {
		if !exists {
			log.Notice("Creating index \"%s\" ...", service.par.Elasticsearch.Index)
			if _, err = service.esClient.CreateIndex(service.par.Elasticsearch.Index).Do(); err != nil {
				log.Info("Index not created: ", err)
			}
		}
	}

	log.Debug("Connecting to NSQ...")
	{
		var err error
		if service.mqClient, err = mq.NewClient(service.par.Mq.Topic, service.par.Mq.Channel); err != nil {
			log.Fatal("MQ error: ", err)
		}
	}

	service.mqClient.Start(service.par.Mq.Server)
	defer service.mqClient.Stop()

	go service.runMessaging()

	log.Notice("Ready !")

	os.Exit(<-service.quitRc)
}
