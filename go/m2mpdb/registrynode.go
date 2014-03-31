package m2mpdb

import (
	"path/filepath"
	"strings"
)

type RegistryNode struct {
	Path          string
	values        map[string]string
	status        int
	childrenNames []string
}

const (
	RN_STATUS_CREATED   = 100
	RN_STATUS_DELETED   = 5
	RN_STATUS_UNDEFINED = 0
)

func NewRegistryNode(path string) *RegistryNode {
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}
	return &RegistryNode{Path: path /*, status: RN_STATUS_UNDEFINED, values: nil, childrenNames: nil*/}
}

func (node *RegistryNode) Name() string {
	return filepath.Base(node.Path[:len(node.Path)-1])
}

func (node *RegistryNode) Status() int {
	if node.status == RN_STATUS_UNDEFINED {
		var status int
		if err := shared.session.Query("select status from registrynode where path=?;", node.Path).Scan(&status); err == nil {
			node.status = status
		}
	}
	return node.status
}

func (node *RegistryNode) Values() map[string]string {
	if node.values == nil {
		values := make(map[string]string)
		shared.session.Query("select values from registrynode where path=?;", node.Path).Scan(&values)
		node.values = values
	}
	return node.values
}

func (node *RegistryNode) Value(name string) string {
	return node.Values()[name]
}

func (node *RegistryNode) SetValue(name, value string) error {
	node.values = nil
	return shared.session.Query("update registrynode set values[ ? ] = ? where path = ?;", name, value, node.Path).Exec()
}

func (node *RegistryNode) DelValue(name string) error {
	if node.values != nil {
		delete(node.values, name)
	}
	return shared.session.Query("delete values[ ? ] from registrynode where path = ?;", name, node.Path).Exec()
}

func (node *RegistryNode) setStatus(status int) error {
	node.status = status
	return shared.session.Query("update registrynode set status=? where path=?;", node.status, node.Path).Exec()
}

func (node *RegistryNode) Create() *RegistryNode {
	node.setStatus(RN_STATUS_CREATED)
	if parent := node.Parent(); parent != nil {
		parent.Check()
		parent.addChild(node.Name())
	}
	return node
}

func (node *RegistryNode) Check() *RegistryNode {
	switch node.Status() {
	case RN_STATUS_UNDEFINED, RN_STATUS_DELETED:
		node.Create()
	}
	return node
}

func (node *RegistryNode) Delete(forReal bool) error {
	var err error

	// We remove all the children
	for _, child := range node.Children() {
		if err2 := child.Delete(forReal); err2 != nil {
			err = err2
		}
	}

	// We delete for real (or mark as deleted)
	if forReal {
		if err2 := shared.session.Query("delete from registrynode where path=?;", node.Path).Exec(); err2 != nil {
			err = err2
		}
		// shared.session.Query("delete from registrynodedata where path=?;", path).Exec()
		node.status = RN_STATUS_UNDEFINED
	} else {
		node.setStatus(RN_STATUS_DELETED)
	}

	// We remove ourself from the parent
	if parent := node.Parent(); parent != nil {
		if err2 := parent.removeChild(node.Name()); err2 != nil {
			err = err2
		}
	}

	return err
}

func (node *RegistryNode) Exists() bool {
	return node.Status() == RN_STATUS_CREATED
}

func (node *RegistryNode) Deleted() bool {
	return node.Status() == RN_STATUS_DELETED
}

func (node *RegistryNode) Existed() bool {
	return node.Status() != RN_STATUS_UNDEFINED
}

func (node *RegistryNode) Parent() *RegistryNode {
	if node.Path == "/" {
		return nil
	}
	return NewRegistryNode(filepath.Dir(node.Path[:len(node.Path)-1]))
}

func (node *RegistryNode) addChild(name string) error {
	return shared.session.Query("insert into registrynodechildren (path, name) values (?, ?);", node.Path, name).Exec()
}

func (node *RegistryNode) removeChild(name string) error {
	node.childrenNames = nil
	return shared.session.Query("delete from registrynodechildren where path=? and name=?;", node.Path, name).Exec()
}

func (node *RegistryNode) GetChild(name string) *RegistryNode {
	return NewRegistryNode(node.Path + name + "/")
}

func (node *RegistryNode) ChildrenNames() []string {
	if node.childrenNames == nil {
		iter := shared.session.Query("select name from registrynodechildren where path=?;", node.Path).Iter()
		defer iter.Close()

		names := make([]string, 0, 10) // 10 children is a reasonnable number
		{
			var name string
			for iter.Scan(&name) {
				names = append(names, name)
			}
		}
		node.childrenNames = names
	}
	return node.childrenNames
}

func (node *RegistryNode) Children() []*RegistryNode {
	children := make([]*RegistryNode, 0, 10)

	for _, name := range node.ChildrenNames() {
		children = append(children, NewRegistryNode(node.Path+name))
	}

	return children
}

/*
func (node *RegistryNode) Save() error {
	query := shared.session.Query("insert into registrynode (path, values) values (?, ?); ", node.Path, node.Values)
	return query.Exec()
}
*/

func (node *RegistryNode) String() string {
	return "RN{Path=" + node.Path + "}"
}

func RegistryNodeCleanup() (err error) {
	iter := shared.session.Query("select path from registrynode where status=?;", RN_STATUS_DELETED).Iter()
	defer iter.Close()

	var path string
	for iter.Scan(&path) {
		err2 := NewRegistryNode(path).Delete(true)
		if err2 != nil {
			err = err2
		}
	}
	return err
}
