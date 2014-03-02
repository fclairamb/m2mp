package m2mpdb

import (
	"github.com/gocql/gocql"
)

// This should become a mixup between PostgreSQL and cassandra.
// SQL should be used for everything that shouldn't grow too much <1M and cassandra should be used for the rest (time series typically)

type Session struct {
	session *gocql.Session
}

var shared *Session

func init() {
	shared = nil
}

func NewSession(session *gocql.Session) {
	Close()
	shared = &Session{session: session}
}

func NewSessionSimple(keyspace string) error {
	cluster := gocql.NewCluster("localhost")
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.One
	session, err := cluster.CreateSession()

	if err == nil {
		NewSession(session)
	}

	return err
}

func Close() {
	if shared != nil {
		shared.session.Close()
	}

}

func WipeoutEverything(sure string) {
	if sure == "yes" {
		shared.session.Query("truncate registrynode;").Exec()
		shared.session.Query("truncate registrynodechildren;").Exec()
	}
}
