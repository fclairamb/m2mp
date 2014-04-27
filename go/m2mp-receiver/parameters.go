package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
)

type Parameters struct {
	LogLevel       int
	ListenPort     int
	HttpListenPort int
	LogFilename    string
	logFile        *os.File
	PprofPrefix    string
}

func NewParameters() *Parameters {
	par := &Parameters{}
	par.parseFromFlag()
	par.setupLog()
	return par
}

func (par *Parameters) parseFromFlag() {
	flag.IntVar(&par.LogLevel, "loglevel", 9, "Logging level")
	flag.StringVar(&par.LogFilename, "logfilename", "receiver.log", "Logging file")
	flag.IntVar(&par.ListenPort, "listen", 3000, "Listening port")
	flag.IntVar(&par.HttpListenPort, "httpListen", 6060, "Http listening port (for profiling)")
	flag.StringVar(&par.PprofPrefix, "pprof", "pp", "pprof prefix")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: receiver")
		flag.PrintDefaults()
		os.Exit(2)
	}

	flag.Parse()
}

func (par *Parameters) setupLog() error {
	var err error
	if par.logFile, err = os.OpenFile(par.LogFilename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0660); err != nil {
		return errors.New(fmt.Sprintf("Could not open log file (%s)", err))
	} else {
		log.SetOutput(io.MultiWriter(par.logFile, os.Stdout))
	}
	return nil
}

func (par *Parameters) Close() error {
	if par.logFile != nil {
		return par.logFile.Close()
	}
	return nil
}

func init() {
	// This is how the logs should look like
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
}
