package entities

import (
	"crypto/sha1"
	"errors"
	db "github.com/fclairamb/m2mp/go/m2mpdb"
	"github.com/gocql/gocql"
	"time"
)

const (
	DEVICE_NODE_PATH          = "/device/"
	DEVICE_BY_IDENT_NODE_PATH = DEVICE_NODE_PATH + "by-ident/"
)

// Device
type Device struct {
	// Device's node
	Node *db.RegistryNode

	// I removed all the caching code around settings, settingsToSend, commands, serverSettings, etc. because
	// we don't care about performance at this stage and it might lead to caching issues
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

	return device, nil
}

func (d *Device) Id() string {
	return d.Node.Name()
}

func (d *Device) Name() string {
	return d.Node.Value("name")
}

func (d *Device) SetName(name string) error {
	return d.Node.SetValue("name", name)
}

func (d *Device) getCommandsNode() *db.RegistryNode {
	return d.Node.GetChild("commands").Check()
}

func (d *Device) getSettingsNode() *db.RegistryNode {
	return d.Node.GetChild("settings").Check()
}

func (d *Device) getSettingsToSendNode() *db.RegistryNode {
	return d.Node.GetChild("settings-to-send").Check()
}

func (d *Device) getServerSettingsNode() *db.RegistryNode {
	return d.Node.GetChild("server-settings").Check()
}

func (d *Device) getServerSettingsPublicNode() *db.RegistryNode {
	return d.Node.GetChild("server-settings-public").Check()
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

func (d *Device) Setting(name string) string {
	return d.getSettingsNode().Value(name)
}

// Define a setting
func (d *Device) SetSetting(name, value string) (err error) {
	err = d.getSettingsNode().SetValue(name, value)
	if err == nil {
		err = d.getSettingsToSendNode().SetValue(name, value)
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

func (d *Device) TSID() string {
	return "dev-" + d.Id()
}

func (d *Device) SaveTSTime(dataType string, time time.Time, data string) error {
	return db.SaveTSTime(d.TSID(), dataType, time, data)
}

func (d *Device) SaveTSTimeObj(dataType string, time time.Time, obj interface{}) error {
	return db.SaveTSTimeObj(d.TSID(), dataType, time, obj)
}
