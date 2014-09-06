package main

import (
	m2log "github.com/fclairamb/m2mp/go/m2mp-log"
	logging "github.com/op/go-logging"
)

var log *logging.Logger

func init() {
	m2log.NoColor = true
	log = m2log.GetLogger()
}
