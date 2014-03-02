package m2mpdb

import (
	"github.com/gocql/gocql"
	"testing"
)

func TestSessionSimple(t *testing.T) {
	if err := NewSessionSimple("ks_test"); err != nil {
		t.Fatal(err)
	}
	Close()
}

func TestSessionWithGoCQLSession(t *testing.T) {
	cluster := gocql.NewCluster("localhost")
	cluster.Keyspace = "m2mp_v2"
	cluster.Consistency = gocql.One
	session, _ := cluster.CreateSession()
	NewSession(session)
	Close()
}
