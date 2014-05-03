// Entities handling
package entities

import (
	"errors"
	"fmt"
	db "github.com/fclairamb/m2mp/go/m2mp-db"
	"github.com/gocql/gocql"
)

// Domain
type Domain struct {
	Node *db.RegistryNode
}

const DOMAIN_NODE_PATH = "/domain/"
const DOMAIN_BY_NAME_NODE_PATH = DOMAIN_NODE_PATH + "by-name/"

// Create a new domain instance with its it id
func NewDomainById(id gocql.UUID) (d *Domain) {
	d = &Domain{Node: db.NewRegistryNode(DOMAIN_NODE_PATH+id.String())}
	return
}

// Get a domain by name
func NewDomainByName(name string) (*Domain, error) {
	node := db.NewRegistryNode(DOMAIN_BY_NAME_NODE_PATH + name)
	if node.Exists() {
		sid := node.Values()["id"]
		id, err := gocql.ParseUUID(sid)
		if err != nil {
			return nil, err
		}
		return NewDomainById(id), nil
	}
	return nil, errors.New("No domain by that name")
}

// Create new a domain
func NewDomainByNameCreate(name string) (*Domain, error) {
	d, err := NewDomainByName(name)
	if d != nil {
		return d, nil
	}

	id, err := gocql.RandomUUID()
	if err != nil {
		return nil, err
	}
	node := db.NewRegistryNode(DOMAIN_BY_NAME_NODE_PATH + name + "/").Create()
	node.SetValue("id", id.String())
	domain := NewDomainById(id)
	domain.Node.Create()
	domain.Node.SetValue("name", name)

	return domain, nil
}

// Get the domain's name
func (d *Domain) Name() string {
	return d.Node.Values()["name"]
}

// Get the domain's id
func (d *Domain) Id() string {
	return d.Node.Name()
}

// Delete the domain
func (d *Domain) Delete() error {
	name := d.Name()

	if name != "" { // By name reference
		node := db.NewRegistryNode(DOMAIN_BY_NAME_NODE_PATH + name)
		node.Delete(false)
	}

	{ // Actual node
		d.Node.Delete(false)
	}

	return nil
}

// Rename the domain
func (d *Domain) Rename(name string) error {
	if name == "" {
		return errors.New("Incorrect name")
	}

	{ // We check if a domain by that name doesn't already exist
		if existing, _ := NewDomainByName(name); existing != nil {
			return errors.New(fmt.Sprintf("Domain %s already exists with id %s.", d.Name(), d.Id()))
		}
	}

	{ // We remove the previous name
		previousName := d.Name()
		if previousName != "" {
			node := db.NewRegistryNode(DOMAIN_BY_NAME_NODE_PATH + previousName)
			node.Delete(false)
		}
	}

	{ // We register the new name
		node := db.NewRegistryNode(DOMAIN_BY_NAME_NODE_PATH + name + "/").Create()
		node.SetValue("id", d.Id())
		d.Node.SetValue("name", name)
	}

	return nil
}
