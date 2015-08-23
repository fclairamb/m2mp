package entities

import (
	db "github.com/fclairamb/m2mp/go/m2mp-db"
	"testing"
	"time"
)

func TestDeviceCreation(t *testing.T) {
	if err := db.NewSessionSimple("ks_test"); err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.WipeoutEverything("yes")

	{ // Creating device
		dev, err := NewDeviceByIdentCreate("imei:1234")

		dev.SetName("My device")

		if err != nil {
			t.Fatal(err)
		}
		if dev == nil {
			t.Fatal("nil device")
		}
	}

	{ // Fetching it afterwards
		dev, err := NewDeviceByIdent("imei:1234")

		if err != nil {
			t.Fatal(err)
		}
		if dev == nil {
			t.Fatal("nil device")
		}

		if dev.Name() != "My device" {
			t.Fatal("This is not a good name")
		}
	}
}

func TestDeviceSetting(t *testing.T) {
	if err := db.NewSessionSimple("ks_test"); err != nil {
		t.Fatal(err)
	}
	db.WipeoutEverything("yes")
	defer db.Close()
	{ // We define settings
		dev, err := NewDeviceByIdentCreate("imei:5678")

		if err != nil {
			t.Fatal(err)
		}

		dev.SetSetting("set1", "val1")
		dev.SetSetting("set2", "val2")
	}

	{ // We check them
		dev, err := NewDeviceByIdentCreate("imei:5678")

		if err != nil {
			t.Fatal(err)
		}

		size := len(dev.Settings())
		if size != 2 {
			t.Fatal("We have", size, "settings.")
		}

		size = len(dev.SettingsToSend())
		if size != 2 {
			t.Fatal("We have", size, "settings to send.")
		}

		if dev.Setting("set1") != "val1" {
			t.Fatal("Wrong setting value")
		}

		if dev.Setting("set2") != "val2" {
			t.Fatal("Wrong setting value")
		}

	}

	{ // We ack them
		dev, err := NewDeviceByIdentCreate("imei:5678")

		if err != nil {
			t.Fatal(err)
		}

		dev.AckSetting("set1", "val1")
		dev.AckSetting("set2", "val2")
	}

	{ // We check that we don't have anything to send anymore
		dev, err := NewDeviceByIdentCreate("imei:5678")

		if err != nil {
			t.Fatal(err)
		}

		size := len(dev.Settings())
		if size != 2 {
			t.Fatal("We have", size, "settings.")
		}

		size = len(dev.SettingsToSend())
		if size != 0 {
			t.Fatal("We have", size, "settings to send.")
		}
	}
}

func TestDeviceListedSensor(t *testing.T) {
	if err := db.NewSessionSimple("ks_test"); err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.WipeoutEverything("yes")

	{ // We define settings
		dev, err := NewDeviceByIdentCreate("imei:1234")

		if err != nil {
			t.Fatal(err)
		}

		dev.MarkListedSensor("my-sensor")
		sensors := dev.Node.GetChild("listed-sensors")
		if !sensors.Exists() {
			t.Fatalf("Node %s doesn't exist", sensors.Path)
		}
		sensor := sensors.GetChild("my-sensor")
		if !sensor.Exists() {
			t.Fatal("Sensor %s doesn't exist", sensor.Path)
		}
		name := sensor.Value("name")
		if name != "my-sensor" {
			t.Fatalf("Sensor doesn't have a correct name: %s", name)
		}
	}
}

func TestDeviceDataAccess(t *testing.T) {
	if err := db.NewSessionSimple("ks_test"); err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.WipeoutEverything("yes")

	dev, err := NewDeviceByIdentCreate("imei:1234")

	if err != nil {
		t.Fatal(err)
	}

	dev.SaveTSTime("test", time.Now().UTC(), "data1")
	dev.SaveTSTime("test", time.Now().UTC(), "data2")

	last := dev.LastData("test")
	if last == nil {
		t.Fatal("No last data")
	}
	if last.Data != "data2" {
		t.Fatalf("Got %s instead of data2.", last.Data)
	}
}
