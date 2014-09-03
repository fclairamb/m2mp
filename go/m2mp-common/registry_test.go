package m2mp_common

import (
	"fmt"
	"os"
	"testing"
)

type Device struct {
	ConnectionId int
	Name         string
	Type         string
}

func registryInsertion(reg *Registry, dev *Device) {
	primary := fmt.Sprintf("connectionId_%d", dev.ConnectionId)
	reg.AddPrimaryKey(primary, dev)
	reg.AddSecondaryKey(primary, "name_"+dev.Name)
	reg.AddSecondaryKey(primary, "type_"+dev.Type)
}

func registryDeletion(reg *Registry, dev *Device) {
	primary := fmt.Sprintf("connectionId_%d", dev.ConnectionId)
	reg.RemovePrimaryKey(primary)
}

func TestPrimaryDefinition(t *testing.T) {
	reg := NewRegistry()
	dev1 := &Device{ConnectionId: 101}
	registryInsertion(reg, dev1)

	dev2 := &Device{ConnectionId: 102}
	registryInsertion(reg, dev2)

	dev1b := reg.Primary("connectionId_101").(*Device)
	if dev1 != dev1b {
		t.Fatal("Could not find defined device")
	}

	dev2b := reg.Primary("connectionId_102").(*Device)
	if dev2 != dev2b {
		t.Fatal("Could not find defined device")
	}

	if dev1b == dev2b {
		// This would happen if we used Device instead of *Device
		t.Fatal("Test is wrong")
	}

	if notExisting := reg.Primary("connectionId_100"); notExisting != nil {
		t.Fatal("We shoudld not have", notExisting)
	}

	if nb := reg.NbPrimary(); nb != 2 {
		t.Fatal("Nb primaries", nb)
	}
	registryInsertion(reg, dev1)

	if nb := reg.NbPrimary(); nb != 2 {
		t.Fatal("Nb primaries", nb)
	}

	registryInsertion(reg, dev2)

	if nb := reg.NbPrimary(); nb != 2 {
		t.Fatal("Nb primaries", nb)
	}

	if test := reg.Primary("connectionId_101"); test != nil {
		dev := test.(*Device)
		if dev != dev1 {
			t.Fatal("We should have got dev1")
		}
	}

	if nb := reg.NbPrimary(); nb != 2 {
		t.Fatalf("Nb primary = %d", nb)
	}

	reg.RemovePrimaryKey("connectionId_101")

	if nb := reg.NbPrimary(); nb != 1 {
		reg.Dump(os.Stdout)
		t.Fatalf("Nb primary = %d", nb)
	}

	reg.RemovePrimaryKey("connectionId_102")

	if nb := reg.NbPrimary(); nb != 0 {
		t.Fatal("Wrong number of primary keys:", nb)
	}

	if nb := reg.NbSecondary(); nb != 0 {
		t.Fatal("Wrong number of secondary keys:", nb)
	}

}

func TestSecondaryDefinition(t *testing.T) {
	reg := NewRegistry()

	dev1 := &Device{ConnectionId: 101, Name: "dev1", Type: "mytype"}
	registryInsertion(reg, dev1)

	dev2 := &Device{ConnectionId: 102, Name: "dev2", Type: "mytype"}
	registryInsertion(reg, dev2)

	dev3 := &Device{ConnectionId: 103, Name: "dev3", Type: "mytype"}
	registryInsertion(reg, dev3)

	if l := len(reg.Secondary("name_" + dev1.Name)); l != 1 {
		t.Fatal("Wrong number of names:", l)
	}

	if l := len(reg.Secondary("type_" + dev1.Type)); l != 3 {
		for _, d := range reg.Secondary("type_" + dev1.Type) {
			dev := d.(*Device)
			t.Log("Device", dev)
		}
		t.Fatal("Wrong number of types:", l)
	}

	registryInsertion(reg, dev1)
	if l := len(reg.Secondary("name_" + dev1.Name)); l != 1 {
		for _, d := range reg.Secondary("name_" + dev1.Name) {
			dev := d.(*Device)
			t.Log("Device", dev)
		}
		t.Fatal("Wrong number of names:", l)
	}

	dev4 := *dev1
	dev4.Name = "dev4"
	registryInsertion(reg, &dev4)
	if l := len(reg.Secondary("name_" + dev1.Name)); l != 0 {
		for _, d := range reg.Secondary("name_" + dev1.Name) {
			dev := d.(*Device)
			t.Log("Device", dev)
		}
		t.Fatal("Wrong number of names:", l)
	}

	if l := len(reg.Secondary("name_" + dev4.Name)); l != 1 {
		for _, d := range reg.Secondary("name_" + dev4.Name) {
			dev := d.(*Device)
			t.Log("Device", dev)
		}
		t.Fatal("Wrong number of names:", l)
	}

	registryDeletion(reg, dev1)
	registryDeletion(reg, dev2)
	registryDeletion(reg, dev3)
	registryDeletion(reg, &dev4)

	if nb := reg.NbPrimary(); nb != 0 {
		t.Fatal("Wrong number of primary keys:", nb)
	}

	if nb := reg.NbSecondary(); nb != 0 {
		t.Fatal("Wrong number of secondary keys:", nb)
	}
}

func TestInconsistencies(t *testing.T) {
	reg := NewRegistry()

	dev := &Device{ConnectionId: 1, Name: "dev1", Type: "mytype"}
	registryInsertion(reg, dev)

	dev.ConnectionId = 2
	registryInsertion(reg, dev)

	dev.ConnectionId = 3
	registryInsertion(reg, dev)

	if duplicates := reg.CheckConsistency(); len(duplicates) == 0 {
		t.Fatalf("We should have duplicates, instead we have none.")
	}

	if duplicates := reg.CheckConsistency(); len(duplicates) != 0 {
		t.Fatalf("We should have 0 duplicates, instead we have %d : %#v", len(duplicates), duplicates)
	}
}
