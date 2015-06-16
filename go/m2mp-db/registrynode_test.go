package m2mpdb

import (
	"math"
	"testing"
	"time"
)

func TestRNValues(t *testing.T) {
	NewSessionSimple("ks_test")

	path := "/test"

	{ // We define some value
		node := NewRegistryNode(path).Check()
		node.SetValue("a", "bc")
		node.SetValue("d", "ef")

		// We check that the String function works (coverage)
		node.String()
	}

	{ // We check them
		node := NewRegistryNode(path)
		if node.Value("a") != "bc" {
			t.Fatalf("%s:a = %s != bc", node, node.Value("a"))
		}
		if node.Value("d") != "ef" {
			t.Fatalf("%s:d = %s != ef", node, node.Value("d"))
		}
	}

	{ // We delete one
		node := NewRegistryNode(path)
		node.DelValue("d")

		if node.Value("d") != "" {
			t.Fatalf("%s:d != nil (%s)", node, node.Value("d"))
		}
	}

	{ // We delete one after fetching all values (coverage)
		node := NewRegistryNode(path)
		node.Values()
		node.DelValue("a")
	}

	{ // We check that we can't have it
		node := NewRegistryNode(path)
		if node.Value("d") != "" {
			t.Fatalf("%s:d != nil (%s)", node, node.Value("d"))
		}
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

	n1 = NewRegistryNode("/dir1/dir2b").Check()

	// Coverage
	if children := NewRegistryNode("/dir1").Children(); len(children) != 2 {
		t.Fatalf("We should have 2 children instead of %d", len(children))
	}

	// Coverage
	if !NewRegistryNode("/dir1").GetChild("dir2").Exists() {
		t.Fatalf("Node /dir1/dir2 cannot be fetched !")
	}

	{ // Coverage
		NewRegistryNode("/dir1/dir2/dir3").Delete(false)

		if !NewRegistryNode("/dir1/dir2/dir3").Deleted() {
			t.Fatalf("/dir1/dir2/dir3 should be marked as deleted !")
		}
	}

	{ // Coverage
		NewRegistryNode("/dir1").Delete(true)
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

func TestRNTime(t *testing.T) {
	NewSessionSimple("ks_test")

	now := time.Now()

	{
		n1 := NewRegistryNode("/dir1").Check()
		n1.SetValueTime("time", now)
		n1.SetValue("time2", "abc")
	}

	{
		n1 := NewRegistryNode("/dir1")
		// OK
		if saved, err := n1.ValueTime("time"); err == nil {
			if diff := time.Duration(math.Abs(float64(now.Sub(saved).Nanoseconds()))); diff >= time.Millisecond {
				t.Fatalf("Diff %f > 1ms", diff)
			}
		} else {
			t.Fatal(err)
		}

		// Failed
		if _, err := n1.ValueTime("time2"); err == nil {
			t.Fatal("This should have failed !")
		}
	}
}
