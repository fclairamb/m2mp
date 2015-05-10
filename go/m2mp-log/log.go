package m2mplog

import (
	"flag"
	"fmt"
	logging "github.com/op/go-logging"
	"log"
	"os"
)

var Level int
var NoColor bool

var logger *logging.Logger

const LOG_FLAGS = log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile

func GetLogger() *logging.Logger {
	if logger == nil {
		logging.SetFormatter(logging.MustStringFormatter("▶ %{level:.1s} 0x%{id:x} %{message}"))

		stdoutBackend := logging.NewLogBackend(os.Stdout, "", LOG_FLAGS)
		stdoutBackend.Color = !NoColor

		stdoutBackendLeveled := logging.AddModuleLevel(stdoutBackend)
		stdoutBackendLeveled.SetLevel(logging.Level(Level), "")

		logger = logging.MustGetLogger("m2log")
		logging.SetBackend(stdoutBackendLeveled)

		log.SetFlags(LOG_FLAGS)
	}
	return logger
}

func GetLoggerAgain() *logging.Logger {
	logger = nil
	return GetLogger()
}

func init() {
	flag.BoolVar(&NoColor, "log-nocolor", false, "Do not use color for stdout logging")
	flag.IntVar(&Level, "log-level", int(logging.INFO), fmt.Sprintf("Log level (NOTICE=%d, INFO=%d, DEBUG=%d)", logging.NOTICE, logging.INFO, logging.DEBUG))
	logger = nil
}
