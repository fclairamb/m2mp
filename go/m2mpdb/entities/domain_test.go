package domain

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
