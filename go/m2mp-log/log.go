package m2mplog

import (
	"flag"
	logging "github.com/op/go-logging"
	"log"
	"os"
)

var Level int
var NoColor bool
var stdoutBackend *logging.LogBackend
var logger *logging.Logger

const LOG_FLAGS = log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile

func GetLogger() *logging.Logger {
	if logger == nil {
		logging.SetFormatter(logging.MustStringFormatter("▶ %{level:.1s} 0x%{id:x} %{message}"))

		stdoutBackend = logging.NewLogBackend(os.Stdout, "", LOG_FLAGS)
		stdoutBackend.Color = !NoColor

		logger = logging.MustGetLogger("m2log")
		logging.SetBackend(stdoutBackend)

		log.SetFlags(LOG_FLAGS)
	}
	return logger
}

func GetLoggerAgain() *logging.Logger {
	logger = nil
	return GetLogger()
}

func init() {
	// We setup logging
	flag.BoolVar(&NoColor, "log-nocolor", false, "Do not use color for stdout logging")
	flag.IntVar(&Level, "log-level", 5, "Log level (from 0 to 9)")
	//	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
}
