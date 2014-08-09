package main

import (
	"github.com/fclairamb/m2mp/go/m2log"
	logging "github.com/op/go-logging"
)

var log *logging.Logger

func init() {
	log = m2log.GetLogger()
}
