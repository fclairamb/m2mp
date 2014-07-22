package m2log

import (
	"flag"
	logging "github.com/op/go-logging"
	"io"
	"log"
	"os"
	"time"
)

var file *os.File
var FileName string
var Level int
var stdoutBackend *logging.LogBackend
var fileBackend *logging.LogBackend
var logger *logging.Logger

const LOG_FLAGS = log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile

func GetLogger() *logging.Logger {
	if logger == nil {
		logging.SetFormatter(logging.MustStringFormatter("▶ %{level:.1s} 0x%{id:x} %{message}"))

		stdoutBackend = logging.NewLogBackend(os.Stdout, "", LOG_FLAGS)
		stdoutBackend.Color = true

		reopenLogFile()

		logger = logging.MustGetLogger("m2log")

		// Standard logging
		log.SetFlags(LOG_FLAGS)

		// We reopen the log file very hour
		go func() {
			ticker := time.NewTicker(time.Hour)
			for {
				<-ticker.C // This is what makes us wait
				reopenLogFile()
			}
		}()
	}
	return logger
}

func logFile() *os.File {
	var err error
	if file != nil {
		err = file.Close()
	}

	if file, err = os.OpenFile(FileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0660); err == nil {
		return file
	} else if file, err := os.OpenFile("/tmp/"+os.Args[0], os.O_RDWR|os.O_CREATE|os.O_APPEND, 0660); err == nil {
		return file
	} else {
		return nil
	}
}

func reopenLogFile() {
	if file := logFile(); file != nil {
		fileBackend = logging.NewLogBackend(file, "", log.LstdFlags|log.Lshortfile)
		logging.SetBackend(stdoutBackend, fileBackend)

		// Standard logging
		log.SetOutput(io.MultiWriter(file, os.Stdout))
	} else {
		logging.SetBackend(stdoutBackend)

		log.SetOutput(os.Stdout)
	}
}

func init() {
	// We setup logging
	flag.StringVar(&FileName, "logfile", os.Args[0]+".log", "Log file")
	flag.IntVar(&Level, "loglevel", 5, "Log level (from 0 to 9)")
	//	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
}
