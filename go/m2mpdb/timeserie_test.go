package m2mpdb

import (
	"log"
	"testing"
	"time"
)

func TestTimeSerieSaving(t *testing.T) {
	NewSessionSimple("ks_test")
	SaveTSTime("device", "location", time.Now().UTC(), "Hello boy")
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
