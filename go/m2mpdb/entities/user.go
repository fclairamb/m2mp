package entities

import (
	"errors"
	"fmt"
	"github.com/fclairamb/m2mp/go/m2mpdb"
	"github.com/gocql/gocql"
	"log"
)

// User
type User struct {
	Node *m2mpdb.RegistryNode
}

const USER_NODE_PATH = "/user/"
const USER_BY_NAME_NODE_PATH = USER_NODE_PATH + "by-name/"

// Create a new User instance with its it id
func NewUserById(id gocql.UUID) (u *User) {
	u = &User{Node: m2mpdb.NewRegistryNode(USER_NODE_PATH + id.String())}
	return
}

// Get a user instance by its name
func NewUserByName(name string) (*User, error) {
	node := m2mpdb.NewRegistryNode(USER_BY_NAME_NODE_PATH + name)
	if node.Exists() {
		sid := node.Values()["id"]
		id, err := gocql.ParseUUID(sid)
		if err != nil {
			return nil, err
		}
		return NewUserById(id), nil
	}
	return nil, errors.New("No user by that name")
}

// Create new user
func NewUserByNameCreate(name string) (*User, error) {
	d, err := NewUserByName(name)
	if d != nil {
		return d, nil
	}

	id, err := gocql.RandomUUID()
	if err != nil {
		return nil, err
	}
	node := m2mpdb.NewRegistryNode(USER_BY_NAME_NODE_PATH + name + "/").Create()
	node.SetValue("id", id.String())
	User := NewUserById(id)
	User.Node.Create()
	User.Node.SetValue("name", name)

	return User, nil
}

// Get the User's name
func (d *User) Name() string {
	return d.Node.Values()["name"]
}

// Get the User's id
func (d *User) Id() gocql.UUID {
	id, _ := gocql.ParseUUID(d.Node.Name())
	return id
}

// Delete the User
func (d *User) Delete() error {
	name := d.Name()

	if name != "" { // By name reference
		node := m2mpdb.NewRegistryNode(USER_BY_NAME_NODE_PATH + name)
		node.Delete(false)
	}

	{ // Actual node
		d.Node.Delete(false)
	}

	return nil
}

// Rename the User
func (u *User) Rename(name string) error {
	if name == "" {
		return errors.New("Incorrect name")
	}

	{ // We check if a User by that name doesn't already exist
		if existing, _ := NewUserByName(name); existing != nil {
			return errors.New(fmt.Sprintf("User %s already exists with id %s.", u.Name(), u.Id()))
		}
	}

	{ // We remove the previous name
		previousName := u.Name()
		if previousName != "" {
			node := m2mpdb.NewRegistryNode(USER_BY_NAME_NODE_PATH + previousName)
			node.Delete(false)
		}
	}

	{ // We register the new name
		node := m2mpdb.NewRegistryNode(USER_BY_NAME_NODE_PATH + name + "/").Create()
		node.SetValue("id", u.Id().String())
		u.Node.SetValue("name", name)
	}

	return nil
}

func (u *User) SetDomain(d *Domain) error {
	{ // We remove it from the previous domain
		previousDomain := u.Domain()
		if previousDomain != nil {
			previousDomain.Node.GetChild("users").Check().DelValue(u.Id().String())
		}
	}

	// We set the domain id
	u.Node.SetValue("domain", d.Id().String())

	// And add it to the references of the new domain
	d.Node.GetChild("users").Check().SetValue(u.Id().String(), "")

	return nil
}

func (u *User) Domain() *Domain {
	sDomainId := u.Node.Values()["domain"]
	domainId, err := gocql.ParseUUID(sDomainId)

	if err != nil {
		return nil
	}
	return NewDomainById(domainId)
}

func (d *Domain) GetUsers() []*User {
	users := []*User{}

	for n, _ := range d.Node.GetChild("users").Values() {
		userId, err := gocql.ParseUUID(n)
		if err == nil {
			users = append(users, NewUserById(userId))
		} else {
			log.Print("Err:", err)
		}
	}

	return users
}
