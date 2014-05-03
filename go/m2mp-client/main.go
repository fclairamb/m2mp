package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"github.com/fclairamb/m2mp/go/m2log"
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
			fmt.Printf("\"%s\" not understood !", tokens[0])
		}
	}
}

var waitForRc chan int

func main() {
	m2log.Start()

	waitForRc = make(chan int)
	clt := NewClient("localhost:3000", "test:1234")
	clt.Start()

	os.Exit(<-waitForRc)
}
