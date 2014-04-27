package main

import (
	"bufio"
	"fmt"
	db "github.com/fclairamb/m2mp/go/m2mp-db"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"
)

var par *Parameters
var waitForRc chan int

func init() {
	waitForRc = make(chan int)
}

func showMemStats() {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	log.Print("Heap in use: ", stats.HeapInuse, ", Heap objects: ", stats.HeapObjects, ", Go routines: ", runtime.NumGoroutine())
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

		tokens := strings.Split(line, " ")
		switch tokens[0] {
		case "":
			continue

		case "quit":
			waitForRc <- 0

		case "gc":
			runtime.GC()
			showMemStats()

		case "pp":
			if len(tokens) < 2 {
				continue
			}
			if tokens[1] == "mem" {
				name := ""
				if len(tokens) == 3 {
					name = strings.Trim(tokens[2], " ")
				}
				if name == "" {
					name = time.Now().UTC().Format("2006-01-02_15-04-05")
				}
				fileName := fmt.Sprintf("%s_%s", par.PprofPrefix, name)
				f, err := os.Create(fileName)
				if err != nil {
					log.Fatal(err)
				}
				runtime.GC()
				pprof.WriteHeapProfile(f)
				f.Close()
				log.Println("Saved", fileName)
			} else if len(tokens) == 3 && tokens[1] == "rate" {
				if rate, err := strconv.Atoi(tokens[2]); err == nil {
					runtime.MemProfileRate = rate
				} else {
					log.Println("Wrong rate:", err)
				}
			} else {
				fmt.Printf("PP command \"%s\" not understood !\n", line)
			}

		case "mem":
			showMemStats()

		default:
			fmt.Printf("\"%s\" not understood !\n", tokens[0])
		}
	}
}

func main() {
	par = NewParameters()
	defer par.Close()

	if par.LogLevel >= 3 {
		log.Print("Starting !")
	}

	if par.HttpListenPort > 0 {
		port := fmt.Sprintf(":%d", par.HttpListenPort)
		go func() {
			if err := http.ListenAndServe(port, nil); err != nil {
				log.Fatal("HTTP listening error: ", err)
			} else {
				fmt.Println("HTTP listening on ", port)
			}
		}()
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
	defer server.Close()

	if par.LogLevel >= 2 {
		log.Print("Ready !")
	}

	go console_handling()

	os.Exit(<-waitForRc)

}
