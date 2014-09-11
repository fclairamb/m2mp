package main

import (
	"code.google.com/p/gcfg"
	"flag"
	"fmt"
	"os"
)

type Config struct {
	Mq struct {
		Server  string
		Topic   string
		Channel string
	}
	Db struct {
		Keyspace string
	}
	Control struct {
		HttpListenPort int
		Console        bool
	}
}

func LoadConfig() *Config {
	var config Config
	var configFile string

	config.Mq.Topic = "converter-m2mp"
	config.Mq.Channel = "shared"

	flag.StringVar(&configFile, "config", "/etc/m2mp/converter-m2mp.conf", "Config file")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: "+os.Args[0])
		flag.PrintDefaults()
		os.Exit(2)
	}
	flag.Parse()

	if err := gcfg.ReadFileInto(&config, configFile); err != nil {
		log.Fatalf("Could not read \"%s\" : %v", configFile, err)
	}

	return &config
}
