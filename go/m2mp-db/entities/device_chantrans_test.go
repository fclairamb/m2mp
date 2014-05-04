package entities

import (
	db "github.com/fclairamb/m2mp/go/m2mp-db"
	"testing"
)

func TestDeviceChannel(t *testing.T) {
	if err := db.NewSessionSimple("ks_test"); err != nil {
		t.Fatal(err)
	}
	db.WipeoutEverything("yes")
	defer db.Close()

	d1, err := NewDeviceByIdentCreate("imei:492582")

	if err != nil {
		t.Fatal(err)
	}

	d1ct := NewDeviceChannelTrans(d1)

	// sen.* rule with a priority of 100
	if err := d1ct.SetTarget(100, "sen:.*", "temp1"); err != nil {
		t.Fatal(err)
	}

	if target := d1ct.GetTarget("sen:temperature"); target == nil || *target != "temp1" {
		t.Fatal("Wrong target: ", target)
	}

	// sen.* rule with a priority of 99
	d1ct.SetTarget(99, "sen:t.*", "temp2")

	if target := d1ct.GetTarget("sen:temperature"); target == nil || *target != "temp2" {
		t.Fatal("Wrong target: ", target)
	}

	// the old rule must still work
	if target := d1ct.GetTarget("sen:voltage"); target == nil || *target != "temp1" {
		t.Fatal("Wrong target: ", target)
	}

	// now lets apply a new "generic" rule
	dect := NewDeviceChannelTrans(NewDeviceDefault())
	dect.SetTarget(100, "sen:.*", "temp1b")

	// It should not change anything for d1
	// But we have to reload it because the generic rules aren't applied here
	d1ct = NewDeviceChannelTrans(d1)

	if target := d1ct.GetTarget("sen:temperature"); target == nil || *target != "temp2" {
		t.Fatal("Wrong target: ", target)
	}

	// but an other device should be using this new rule
	d2ct := NewDeviceChannelTrans(func() *Device { d, _ := NewDeviceByIdentCreate("imei:52949"); return d }()) // Let's do some unreadable code for once

	if target := d2ct.GetTarget("sen:temperature"); target == nil || *target != "temp1b" {
		t.Fatal("Wrong target: ", target)
	}
}
