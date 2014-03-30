package domain

import (
	"errors"
	"github.com/fclairamb/m2mp/go/m2mpdb"
	"github.com/gocql/gocql"
)

type Domain struct {
	Node *m2mpdb.RegistryNode
}

const DOMAIN_NODE_PATH = "/domain/"
const DOMAIN_BY_NAME_NODE_PATH = DOMAIN_NODE_PATH + "by-name/"

func NewDomainById(id gocql.UUID) (d *Domain) {
	d = &Domain{Node: m2mpdb.NewRegistryNode(DOMAIN_NODE_PATH + id.String())}
	return
}

func NewDomainByName(name string) (*Domain, error) {
	node := m2mpdb.NewRegistryNode(DOMAIN_BY_NAME_NODE_PATH + name)
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

func NewDomainByNameCreate(name string) (*Domain, error) {
	d, err := NewDomainByName(name)
	if d != nil {
		return d, nil
	}

	id, err := gocql.RandomUUID()
	if err != nil {
		return nil, err
	}
	node := m2mpdb.NewRegistryNode(DOMAIN_BY_NAME_NODE_PATH + name + "/").Create()
	node.SetValue("id", id.String())
	domain := NewDomainById(id)
	domain.Node.Create()
	domain.Node.SetValue("name", name)

	return domain, nil
}

func (d *Domain) Name() string {
	return d.Node.Values()["name"]
}

func (d *Domain) Id() gocql.UUID {
	id, _ := gocql.ParseUUID(d.Node.Name())
	return id
}

func (d *Domain) Delete() error {
	name := d.Name()

	if name != "" { // By name reference
		node := m2mpdb.NewRegistryNode(DOMAIN_BY_NAME_NODE_PATH + d.Name())
		node.Delete(false)
	}

	{ // Actual node
		d.Node.Delete(false)
	}

	return nil
}
