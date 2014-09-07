package main

import (
	logging "github.com/op/go-logging"
	glog "log"
	"os"
)

const LOG_FLAGS = glog.Ldate | glog.Ltime | glog.Lmicroseconds | glog.Lshortfile

var log *logging.Logger

func logFile() *os.File {
	if file, err := os.OpenFile("client-alip.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0660); err == nil {
		return file
	} else {
		return nil
	}
}

func init() {
	stdoutBackend := logging.NewLogBackend(os.Stdout, "", glog.LstdFlags|glog.Lshortfile)
	fileBackend := logging.NewLogBackend(logFile(), "", glog.LstdFlags|glog.Lshortfile)
	logging.SetBackend(stdoutBackend, fileBackend)
	glog.SetFlags(LOG_FLAGS)
	log, _ = logging.GetLogger("m2log")
}
