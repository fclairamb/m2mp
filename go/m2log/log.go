package m2log

import (
	"flag"
	"io"
	"log"
	"os"
	"time"
)

var file *os.File
var fileName string
var Level int

func Start() {
	// Whatever
}

func log_set_file() {
	var err error
	if file, err = os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0660); err != nil {
		log.Fatal("Could not open log file", fileName, " ! ", err)
	} else {
		log.SetOutput(io.MultiWriter(file, os.Stdout))
	}
}

func init() {
	// We setup logging
	flag.StringVar(&fileName, "logfile", os.Args[0] + ".log", "Log file")
	flag.IntVar(&Level, "loglevel", 5, "Log level (from 0 to 9)")
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log_set_file()


	// We reopen the log file very hour
	go func() {
		ticker := time.NewTicker(time.Hour)
		for {
			<-ticker.C // This is what makes us wait
			log_set_file()
		}
	}()
}
