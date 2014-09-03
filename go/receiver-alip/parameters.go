package main

import (
	//"code.google.com/p/gcfg"
	"flag"
	"fmt"
	msg "github.com/fclairamb/m2mp/go/m2mp-messaging"
	"os"
)

type Parameters struct {
	Config         string
	Test           bool
	ListenPort     int
	HttpListenPort int
	LogFilename    string
	logFile        *os.File
	PprofPrefix    string
	MQServer       string
	MQTopic        string
	console        bool
	keyspace       string
	//acceptFailure  bool
}

func NewParameters() *Parameters {
	par := &Parameters{}
	par.parseFromFlag()
	return par
}

func (par *Parameters) parseFromFlag() {
	flag.StringVar(&par.Config, "config", "", "Config file")
	flag.IntVar(&par.ListenPort, "listen", 3050, "Listening port")
	flag.IntVar(&par.HttpListenPort, "httpListen", 0, "Http listening port (for profiling)")
	flag.StringVar(&par.PprofPrefix, "pprof", "pp", "pprof prefix")
	flag.StringVar(&par.MQServer, "mqserver", "nsq:localhost:4150", "NSQ server")
	flag.StringVar(&par.MQTopic, "mqtopic", msg.TOPIC_RECEIVERS, "NSQ topic")
	flag.StringVar(&par.keyspace, "cassandra-keyspace", "ks_test", "Cassandra keyspace")
	flag.BoolVar(&par.console, "console", false, "Provide console to issue commands")
	//flag.BoolVar(&par.acceptFailure, "fail", false, "Accept some failures")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: "+os.Args[0])
		flag.PrintDefaults()
		os.Exit(2)
	}

	flag.Parse()
}

func (par *Parameters) Close() {

}
