package entities

import (
	db "github.com/fclairamb/m2mp/go/m2mpdb"
	"log"
	"testing"
)

func TestDomainMissing(t *testing.T) {
	db.NewSessionSimple("ks_test")
	defer db.Close()
	d, err := NewDomainByName("not-here")
	if d != nil || err == nil {
		log.Fatal("d=", d, "err=", err)
	}
}

func TestDomainCreate(t *testing.T) {
	db.NewSessionSimple("ks_test")
	db.WipeoutEverything("yes")
	defer db.Close()

	{
		d, err := NewDomainByName("d1")
		if d != nil || err == nil {
			log.Fatal("d=", d, "; err=", err)
		}
	}

	{
		d, err := NewDomainByNameCreate("d1")
		if d == nil || err != nil {
			log.Fatal("d=", d, "; err=", err)
		}
	}

	{
		d, err := NewDomainByName("d1")
		if d == nil || err != nil {
			log.Fatal("d=", d, "; err=", err)
		}
	}
}

func TestDomainDelete(t *testing.T) {
	db.NewSessionSimple("ks_test")
	defer db.Close()

	{
		d, err := NewDomainByNameCreate("d2")
		if d == nil || err != nil {
			log.Fatal("d=", d, "; err=", err)
		}

		d.Delete()
	}

	{
		d, err := NewDomainByName("d2")
		if d != nil || err == nil {
			log.Fatal("d=", d, "; err=", err)
		}
	}
}

func TestDomainRename(t *testing.T) {
	db.NewSessionSimple("ks_test")
	db.WipeoutEverything("yes")
	defer db.Close()

	{ // Creating "d3"
		d, err := NewDomainByNameCreate("d3")
		if d == nil || err != nil {
			log.Fatal("d=", d, "; err=", err)
		}
	}

	{ // Creating "d4"
		d, err := NewDomainByNameCreate("d4")
		if d == nil || err != nil {
			log.Fatal("d=", d, "; err=", err)
		}
	}

	{ // Trying to rename "d3" to "d4" should fail
		d3, err := NewDomainByName("d3")
		err = d3.Rename("d4")

		if err == nil {
			log.Fatal("This should have failed !")
		}
	}

	{ // Trying to rename "d3" to "d5" should work
		d3, err := NewDomainByName("d3")
		err = d3.Rename("d5")

		if err != nil {
			log.Fatal("Failed:", err)
		}
	}

	{ // We should not have "d3" anymore
		d3, err := NewDomainByName("d3")
		if d3 != nil || err == nil {
			log.Fatal("d3=", d3, "; err=", err)
		}
	}

	{ // We should have "d5"
		d5, err := NewDomainByName("d5")
		if d5 == nil || err != nil {
			log.Fatal("d5=", d5, "; err=", err)
		}
	}
}
