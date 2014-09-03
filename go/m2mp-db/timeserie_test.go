package m2mpdb

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"
)

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
}

func TestTimeSerieSaving(t *testing.T) {
	NewSessionSimple("ks_test")
	defer Close()
	SaveTSTime("device", "test", time.Now().UTC(), "Hello boy")
}

type Person struct {
	FirstName string `json:"firstName"`
	LastName  string `json:"lastName"`
	Age       int    `json:"age"`
}

type Location struct {
	Latitude  float32 `json:"lat"`
	Longitude float32 `json:"lon"`
	Speed     int     `json:"spd"`
	Altitude  int     `json:"alt"`
}

func TestTimeSerieSavingObj(t *testing.T) {
	NewSessionSimple("ks_test")
	defer Close()

	{ // Someone
		me := &Person{FirstName: "Florent", LastName: "Clairambault", Age: 28}
		if err := SaveTSTimeObj("goaddicts", "new", time.Now().UTC(), me); err != nil {
			log.Fatal(err)
		}
	}

	{ // Some place
		myPlace := &Location{Latitude: 48.83, Longitude: 2.32, Speed: 0, Altitude: 10}
		if err := SaveTSTimeObj("florent", "location", time.Now().UTC(), myPlace); err != nil {
			log.Fatal(err)
		}
	}
}

func TestTimeSerieReading(t *testing.T) {
	NewSessionSimple("ks_test")
	defer Close()

	initialTime := time.Now().UTC()

	id := "dev-" + getUUID()

	const NB = 10

	var i time.Duration
	for i = 0; i < NB; i++ {
		time := initialTime.Add(i * time.Hour)
		log.Println("Inserting", time, "...")
		SaveTSTime(id, "test", time, "Hello you !")
	}

	iter := NewTSDataIterator(id, "", nil, nil, false)
	defer iter.Close()

	var td TimedData
	for c := 0; iter.Scan(&td); c++ {
		log.Println("td: ", td, "(", td.Time(), ")")
		c += 1
	}
	if c != NB {
		t.Fatalf("We got %d rows instead of %d", c, NB)
	}
}

func getUUID() string {
	f, err := os.Open("/dev/urandom")
	if err != nil {
		return ""
	}
	defer f.Close()
	b := make([]byte, 16)
	f.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
