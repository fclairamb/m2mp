package main

import (
	"bufio"
	"fmt"
	db "github.com/fclairamb/m2mp/go/m2mp-db"
	"log"
	"os"
	"strings"
)

var par *Parameters
var waitForRc chan int

func init() {
	waitForRc = make(chan int)
}

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

func main() {
	par = NewParameters()
	defer par.Close()

	if par.LogLevel >= 3 {
		log.Print("Starting !")
	}

	if err := db.NewSessionSimple("ks_test"); err != nil {
		log.Fatal("DB error: ", err)
	}
	defer db.Close()

	server := NewServer()
	defer server.Close()

	if err := server.Listen(); err != nil {
		log.Fatal("Server error: ", err)
	}

	if par.LogLevel >= 2 {
		log.Print("Ready !")
	}

	go console_handling()

	os.Exit(<-waitForRc)

}
