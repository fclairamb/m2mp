package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func console_handling() {
	in := bufio.NewReader(os.Stdin)
	for {
		fmt.Printf("> ")
		line, err := in.ReadString('\n')
		if err != nil {
			log.Fatal(err)
			continue
		}
		line = strings.TrimRight(line, "\n")

		tokens := strings.SplitN(line, " ", 2)
		if tokens[0] == "" {
			continue
		} else if tokens[0] == "quit" {
			waitForRc <- 0
		} else {
			log.Debug("\"%s\" not understood !", tokens[0])
		}
	}
}

var waitForRc chan int

func main() {
	par := NewParameters()

	waitForRc = make(chan int)
	clt := NewClient(par.Server, par.Ident)
	clt.Start()

	if par.Console {
		go console_handling()
	}

	rc := <-waitForRc
	if rc == 0 {
		log.Info("Bye bye...")
	} else {
		log.Info("Quitting with RC %d", rc)
	}
	os.Exit(rc)
}
