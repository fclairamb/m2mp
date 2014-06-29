package main

import (
	"flag"
	"fmt"
	"os"
)

type Parameters struct {
	Test           bool
	ListenPort     int
	HttpListenPort int
	LogFilename    string
	logFile        *os.File
	PprofPrefix    string
	MQServer       string
}

func NewParameters() *Parameters {
	par := &Parameters{}
	par.parseFromFlag()
	if par.Test {
		par.SwitchToTestMode()
	}
	return par
}

func (par *Parameters) parseFromFlag() {
	flag.BoolVar(&par.Test, "test", false, "Test")
	flag.IntVar(&par.ListenPort, "listen", 3000, "Listening port")
	flag.IntVar(&par.HttpListenPort, "httpListen", 6060, "Http listening port (for profiling)")
	flag.StringVar(&par.PprofPrefix, "pprof", "pp", "pprof prefix")
	flag.StringVar(&par.MQServer, "mqserver", "nsq:localhost:4150", "NSQ Server")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: "+os.Args[0])
		flag.PrintDefaults()
		os.Exit(2)
	}

	flag.Parse()
}

func (par *Parameters) SwitchToTestMode() {
	par.ListenPort += 10
	par.HttpListenPort += 10
}

func (par *Parameters) Close() {

}
