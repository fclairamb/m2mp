package main

import (
	"flag"
	"fmt"
	"os"
)

type Parameters struct {
	Console bool
	Server  string
	Ident   string
}

func NewParameters() *Parameters {
	par := &Parameters{}
	par.parseFromFlag()
	return par
}

func (par *Parameters) parseFromFlag() {
	flag.BoolVar(&par.Console, "console", false, "Enabling console")
	flag.StringVar(&par.Server, "target", "localhost:3000", "Target")
	flag.StringVar(&par.Ident, "ident", "imei:1234", "Identification")

	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: "+os.Args[0])
		flag.PrintDefaults()
		os.Exit(2)
	}

	flag.Parse()
}
