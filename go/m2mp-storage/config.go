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
	Storage struct {
		Actors int
	}
}

func LoadConfig() *Config {
	config := Config{}
	// Default config
	config.Mq.Channel = "shared"
	config.Storage.Actors = 1

	var configFile string

	flag.StringVar(&configFile, "config", "/etc/m2mp/m2mp-storage.conf", "Config file")
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
