package entities

import (
	"crypto/sha1"
	"errors"
	"fmt"
	db "github.com/fclairamb/m2mp/go/m2mp-db"
	"github.com/gocql/gocql"
	"log"
	"time"
)

const (
	DEVICE_NODE_PATH          = "/device/"
	DEVICE_BY_IDENT_NODE_PATH = DEVICE_NODE_PATH + "by-ident/"
	DEVICE_DEFAULT_NODE_PATH  = "/device/default/"
)

// Device
type Device struct {
	// Device's node
	Node *db.RegistryNode

	// I removed all the caching code around settings, settingsToSend, commands, serverSettings, etc. because
	// we don't care about performance at this stage and it might lead to caching issues
}

func NewDeviceDefault() *Device {
	return &Device{Node: db.NewRegistryNode(DEVICE_DEFAULT_NODE_PATH)}
}

// Device by ID
func NewDeviceById(devId gocql.UUID) *Device {
	return &Device{Node: db.NewRegistryNode(DEVICE_NODE_PATH + devId.String() + "/")}
}

// Device by ident
func NewDeviceByIdent(ident string) (*Device, error) {
	node := db.NewRegistryNode(DEVICE_BY_IDENT_NODE_PATH + ident)
	if node.Exists() {
		sid := node.Values()["id"]
		id, err := gocql.ParseUUID(sid)
		if err != nil {
			return nil, err
		}
		return NewDeviceById(id), nil
	}
	return nil, errors.New("No device by that ident")
}

// Device by ident (created if it doesn't exist)
func NewDeviceByIdentCreate(ident string) (*Device, error) {
	d, err := NewDeviceByIdent(ident)
	if d != nil {
		return d, nil
	}

	hasher := sha1.New()
	hasher.Write([]byte(ident))
	sum := hasher.Sum(nil)
	id, err := gocql.UUIDFromBytes(sum[:16])
	if err != nil {
		return nil, err
	}
	node := db.NewRegistryNode(DEVICE_BY_IDENT_NODE_PATH + ident + "/").Create()
	node.SetValue("id", id.String())
	device := NewDeviceById(id)
	device.Node.Create()
	device.Node.SetValue("ident", ident)

	{ // We put it in the default domain
		def, _ := NewDomainByNameCreate("default")
		device.SetDomain(def)
	}

	return device, nil
}

func (d *Device) Delete() error {
	if dom := d.Domain(); dom != nil {
		dom.Node.GetChild("devices").DelValue(d.Id())
	}
	return d.Node.Delete(false)
}

func (d *Domain) Devices() []*Device {
	devices := []*Device{}

	for n, _ := range d.Node.GetChild("devices").Values() {
		devId, err := gocql.ParseUUID(n)
		if err == nil {
			devices = append(devices, NewDeviceById(devId))
		} else {
			// This should never happen
			log.Print("Invalid device id:", err)
		}
	}

	return devices
}

func (d *Device) Id() string {
	return d.Node.Name()
}

func (d *Device) Name() string {
	return d.GetServerSettingsPublicNode().Value("name")
}

func (d *Device) Ident() string {
	return d.Node.Value("ident")
}

func (d *Device) SetName(name string) error {
	return d.GetServerSettingsPublicNode().SetValue("name", name)
}

func (d *Device) getCommandsNode() *db.RegistryNode {
	return d.Node.GetChild("commands").Check()
}

func (d *Device) getCommandsResponseNode() *db.RegistryNode {
	return d.Node.GetChild("commands-response").Check()
}

func (d *Device) getSettingsNode() *db.RegistryNode {
	return d.Node.GetChild("settings").Check()
}

func (d *Device) getSettingsToSendNode() *db.RegistryNode {
	return d.Node.GetChild("settings-to-send").Check()
}

func (d *Device) getSettingsAckTimeNode() *db.RegistryNode {
	return d.Node.GetChild("settings-ack-time").Check()
}

func (d *Device) getStatusNode() *db.RegistryNode {
	return d.Node.GetChild("status").Check()
}

func (d *Device) getServerSettingsNode() *db.RegistryNode {
	return d.Node.GetChild("server-settings").Check()
}

func (d *Device) GetServerSettingsPublicNode() *db.RegistryNode {
	return d.Node.GetChild("server-settings-public").Check()
}

func (d *Device) getListedSensorsNode() *db.RegistryNode {
	return d.Node.GetChild("listed-sensors").Check()
}

func (d *Device) MarkListedSensor(id string) *db.RegistryNode {
	listedSensor := d.getListedSensorsNode().GetChild(id)
	if !listedSensor.Exists() {
		listedSensor.Create()
		listedSensor.SetValue("name", id)
		listedSensor.Create().SetValue("date-create", fmt.Sprintf("%d", time.Now().UTC().Unix()))
	}
	listedSensor.SetValue("date-last", fmt.Sprintf("%d", time.Now().UTC().Unix()))
	return listedSensor
}

// Add a command to send to the device
func (d *Device) AddCommand(cmd string) (string, error) {
	cmdId, err := gocql.RandomUUID()
	if err != nil {
		return "", err
	}
	err = d.getCommandsNode().SetValue(cmdId.String(), cmd)
	if err != nil {
		return "", err
	}
	return cmdId.String(), nil
}

func (d *Device) Status(name string) string {
	return d.getStatusNode().Value(name)
}

func (d *Device) SetStatus(name, value string) error {
	return d.getStatusNode().SetValue(name, value)
}

func (d *Device) Setting(name string) string {
	return d.getSettingsNode().Value(name)
}

// Define a setting
func (d *Device) SetSetting(name, value string) (err error) {
	err = d.getSettingsNode().SetValue(name, value)
	if err == nil {
		err = d.getSettingsToSendNode().SetValue(name, value)
	}
	if err == nil {
		err = d.getSettingsAckTimeNode().DelValue(name)
	}

	return
}

// Delete a setting
func (d *Device) DelSetting(name string) (err error) {
	err = d.getSettingsNode().DelValue(name)
	if err == nil {
		err = d.getSettingsToSendNode().DelValue(name)
	}
	return err
}

func (d *Device) AckSetting(name, ackedValue string) (err error) {
	toSend := d.getSettingsToSendNode()
	defined := d.getSettingsNode()

	{ // Delete the setting to send
		valueToSend := toSend.Value(name)
		if ackedValue == valueToSend {
			err = toSend.DelValue(name)

			if err == nil {
				d.getSettingsAckTimeNode().SetValue(name, fmt.Sprintf("%d", time.Now().UTC().Unix()))
			}
		}
	}

	// Save the setting (whatever happens)
	if err == nil {
		err = defined.SetValue(name, ackedValue)
	}

	return
}

func (d *Device) Settings() map[string]string {
	return d.getSettingsNode().Values()
}

func (d *Device) SettingsToSend() map[string]string {
	return d.getSettingsToSendNode().Values()
}

func (d *Device) Commands() map[string]string {
	return d.getCommandsNode().Values()
}

// Acknowledge a command with its ID
func (d *Device) AckCommand(cmdId string) error {
	return d.getCommandsNode().DelValue(cmdId)
}

func (d *Device) AckCommandWithResponse(cmdId, response string) error {
	d.AckCommand(cmdId)
	return d.getCommandsResponseNode().SetValue(cmdId, response)
}

func (dev *Device) SetDomain(d *Domain) error {
	{ // We remove it from the previous domain
		previousDomain := dev.Domain()
		if previousDomain != nil {
			previousDomain.Node.GetChild("devices").Check().DelValue(dev.Id())
		}
	}

	// We set the domain id
	dev.Node.SetValue("domain", d.Id())

	// And add it to the references of the new domain
	d.Node.GetChild("devices").Check().SetValue(dev.Id(), "")

	return nil
}

func (this *Device) Owner() *User {
	if userId, err := gocql.ParseUUID(this.Node.Value("owner")); err == nil {
		return NewUserById(userId)
	} else {
		return nil
	}
}

func (dev *Device) Domain() *Domain {
	if domainId, err := gocql.ParseUUID(dev.Node.Value("domain")); err == nil {
		return NewDomainById(domainId)
	} else {
		return nil
	}
}

func (d *Device) TSID() string {
	return "dev-" + d.Id()
}

func (d *Device) SaveTSTime(dataType string, time time.Time, data string) error {
	return db.SaveTSTime(d.TSID(), dataType, time, data)
}

func (d *Device) SaveTSTimeObj(dataType string, time time.Time, obj interface{}) error {
	return db.SaveTSTimeObj(d.TSID(), dataType, time, obj)
}

func (this *Device) LastData(dataType string) *db.TimedData {
	return db.GetTSLast(this.TSID(), dataType, nil, nil, true)
}
