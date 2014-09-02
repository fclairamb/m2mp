package main

import (
	"flag"
	"fmt"
	msg "github.com/fclairamb/m2mp/go/m2mp-messaging"
	"os"
)

type Parameters struct {
	Test           bool
	HttpListenPort int
	LogFilename    string
	logFile        *os.File
	PprofPrefix    string
	MQServer       string
	MQTopic        string
	MQChannel      string
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
	flag.IntVar(&par.HttpListenPort, "httpListen", 6060, "Http listening port (for profiling)")
	flag.StringVar(&par.PprofPrefix, "pprof", "pp", "pprof prefix")
	flag.StringVar(&par.MQServer, "mqserver", "nsq:localhost:4150", "NSQ server")
	flag.StringVar(&par.MQTopic, "mqtopic", msg.TOPIC_STORAGE, "NSQ topic")
	flag.StringVar(&par.MQChannel, "mqchannel", "shared", "NSQ channel")

	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: "+os.Args[0])
		flag.PrintDefaults()
		os.Exit(2)
	}

	flag.Parse()
}

func (par *Parameters) SwitchToTestMode() {
	par.HttpListenPort += 10
	par.MQTopic += "-test"
}

func (par *Parameters) Close() {

}
