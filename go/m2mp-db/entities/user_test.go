package entities

import (
	db "github.com/fclairamb/m2mp/go/m2mp-db"
	"testing"
)

func TestUserMissing(t *testing.T) {
	db.NewSessionSimple("ks_test")
	defer db.Close()
	u, err := NewUserByName("not-here")
	if u != nil || err == nil {
		t.Fatal("u=", u, "err=", err)
	}
}

func TestUserCreate(t *testing.T) {
	db.NewSessionSimple("ks_test")
	db.WipeoutEverything("yes")
	defer db.Close()

	{
		u, err := NewDomainByName("d1")
		if u != nil || err == nil {
			t.Fatal("u=", u, "; err=", err)
		}
	}

	{
		u, err := NewDomainByNameCreate("d1")
		if u == nil || err != nil {
			t.Fatal("u=", u, "; err=", err)
		}
	}

	{
		u, err := NewDomainByName("d1")
		if u == nil || err != nil {
			t.Fatal("u=", u, "; err=", err)
		}
	}
}

func TestUserDelete(t *testing.T) {
	db.NewSessionSimple("ks_test")
	defer db.Close()

	{
		u, err := NewUserByNameCreate("u2")
		if u == nil || err != nil {
			t.Fatal("u=", u, "; err=", err)
		}

		u.Delete()
	}

	{
		u, err := NewUserByName("u2")
		if u != nil || err == nil {
			t.Fatal("u=", u, "; err=", err)
		}
	}
}

func TestUserRename(t *testing.T) {
	db.NewSessionSimple("ks_test")
	db.WipeoutEverything("yes")
	defer db.Close()

	{ // Creating "u3"
		u, err := NewUserByNameCreate("u3")
		if u == nil || err != nil {
			t.Fatal("u=", u, "; err=", err)
		}
	}

	{ // Creating "u4"
		u, err := NewUserByNameCreate("u4")
		if u == nil || err != nil {
			t.Fatal("u=", u, "; err=", err)
		}
	}

	{ // Trying to rename "u3" to "u4" should fail
		u3, err := NewUserByName("u3")
		err = u3.Rename("u4")

		if err == nil {
			t.Fatal("We shouldn't have been able to rename u3 to u4")
		}
	}

	{ // Trying to rename "u3" to "u5" should work
		u3, err := NewUserByName("u3")
		err = u3.Rename("u5")

		if err != nil {
			t.Fatal("Failed:", err)
		}
	}

	{ // We should not have "u3" anymore
		u3, err := NewUserByName("u3")
		if u3 != nil || err == nil {
			t.Fatal("u3=", u3, "; err=", err)
		}
	}

	{ // We should have "u5"
		u5, err := NewUserByName("u5")
		if u5 == nil || err != nil {
			t.Fatal("u5=", u5, "; err=", err)
		}
	}
}

func TestUserOnDomain(t *testing.T) {
	db.NewSessionSimple("ks_test")
	db.WipeoutEverything("yes")
	defer db.Close()
	{ // Creating an user with its domain
		d, err := NewDomainByNameCreate("d3")
		if err != nil {
			t.Fatal(err)
		}
		u, err := NewUserByNameCreate("u6")
		if err != nil {
			t.Fatal(err)
		}
		u.SetDomain(d)
	}

	{ // Getting the domain from the user
		u, err := NewUserByName("u6")
		if err != nil {
			t.Fatal(err)
		}
		d := u.Domain()
		if d == nil {
			t.Fatal("No domain")
		}
		if d.Name() != "d3" {
			t.Fatal("Domain name is", d.Name())
		}
	}

	{ // Getting the user from the domain
		d, err := NewDomainByName("d3")
		if err != nil {
			t.Fatal(err)
		}
		found := false
		nbUsers := 0
		for _, u := range d.Users() {
			nbUsers += 1
			if u.Name() == "u6" {
				found = true
			}
		}
		if !found || nbUsers != 1 {
			t.Fatal("u6 wasn't found ! nbUsers =", nbUsers)
		}
	}
}
