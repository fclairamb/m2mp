package entities

import (
	"fmt"
	db "github.com/fclairamb/m2mp/go/m2mp-db"
)

func GetFirmwareFile(name string) *db.RegistryFile {
	nodeByName := db.NewRegistryNode("/firmware/by-name/")
	fwId := nodeByName.Value(name)
	nodeFile := db.NewRegistryNode(fmt.Sprintf("/firmware/%s/file", fwId))
	file := db.NewRegistryFile(nodeFile)

	if file.Exists() {
		return file
	} else {
		return nil
	}
}
