package m2mpdb

import (
	"testing"
)

func TestRNValues(t *testing.T) {
	NewSessionSimple("ks_test")

	node := NewRegistryNode("/test")
	node.Values["a"] = "bc"
	node.Values["d"] = "ef"
	if err := node.Save(); err != nil {
		t.Fatal(err)
	}
}

func TestRNName(t *testing.T) {
	if n1 := NewRegistryNode("/dir/dir2"); n1.Name() != "dir2" {
		t.Fatal("Wrong name: " + n1.Name())
	}
}

func TestRNCreation(t *testing.T) {

	WipeoutEverything("yes")
	n1 := NewRegistryNode("/dir1/dir2/dir3")

	if n1.Exists() {
		t.Fatalf("%s exists", n1)
	}

	n1 = n1.Check()

	if !n1.Exists() {
		t.Fatalf("%s doesn't exist", n1)
	}
}

func TestRNParent(t *testing.T) {
	n1 := NewRegistryNode("/dir1/dir2")

	if n1.Path != "/dir1/dir2/" {
		t.Fatal("Wrong path: " + n1.Path + " instead of /dir1/dir2/")
	}

	n2 := n1.Parent()

	if n2.Path != "/dir1/" {
		t.Fatal("Wrong path: " + n2.Path + " instead of /dir1/")
	}

	n3 := n2.Parent()

	if n3.Path != "/" {
		t.Fatal("Wrong path: " + n3.Path + " instead of /")
	}

	n4 := n3.Parent()

	if n4 != nil {
		t.Fatalf("%s should not have a parent !", n3)
	}

}

func TestRNDeletion(t *testing.T) {
	NewSessionSimple("ks_test")
	WipeoutEverything("yes")
	n1 := NewRegistryNode("/dir1/dir2")

	if n1.Existed() {
		t.Fatal("n1 should not exist")
	}

	n1.Check()

	if !n1.Exists() {
		t.Fatal("n1 should exist")
	}

	n1.Delete(false)

	if n1.Exists() {
		t.Fatal("n1 should not exist")
	}

	n1 = NewRegistryNode(n1.Path)

	if !n1.Existed() {
		t.Fatalf("%s should have existed / status = %d", n1, n1.Status())
	}

	n1.Delete(true)

	if n1.Existed() {
		t.Fatal("n1 should not exist")
	}

	n1 = NewRegistryNode(n1.Path)

	if n1.Existed() {
		t.Fatal("n1 should not exist")
	}
}

func TestRNCleanup(t *testing.T) {
	NewSessionSimple("ks_test")
	WipeoutEverything("yes")

	n1 := NewRegistryNode("/dir1/dir2").Check()

	n1.Delete(false)

	n1 = NewRegistryNode(n1.Path)

	if !n1.Existed() {
		t.Fatalf("%s should have existed !", n1)
	}

	RegistryNodeCleanup()

	n1 = NewRegistryNode(n1.Path)

	if n1.Existed() {
		t.Fatalf("%s should not have existed !", n1)
	}
}
