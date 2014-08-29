package main

import (
	"bufio"
	"fmt"
	db "github.com/fclairamb/m2mp/go/m2mp-db"
	m2log "github.com/fclairamb/m2mp/go/m2mp-log"
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

func getMemStats() string {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return fmt.Sprint("Heap in use: ", stats.HeapInuse, ", Heap objects: ", stats.HeapObjects, ", Go routines: ", runtime.NumGoroutine())
}

func handleSimpleCommand(line string) string {
	tokens := strings.Split(line, " ")
	switch tokens[0] {
	case "":
		return ""

	case "quit":
		waitForRc <- 0
		return "Done !"

	case "gc":
		runtime.GC()
		return getMemStats()

	case "pp":
		if len(tokens) < 2 {
			return ""
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
			return fmt.Sprint("Saved", fileName)
		} else if len(tokens) == 3 && tokens[1] == "rate" {
			if rate, err := strconv.Atoi(tokens[2]); err == nil {
				runtime.MemProfileRate = rate
			} else {
				return fmt.Sprint("Wrong rate: ", err)
			}
		} else {
			return fmt.Sprintf("PP command \"%s\" not understood !\n", line)
		}

	case "mem":
		return getMemStats()

	case "nb":
		return fmt.Sprint("Number of clients: ", server.NbClients())

	case "help":
		log.Info(`
HELP:
=====
* quit            - To quit
* gc              - Trigger garbage collector
* pp mem          - Profile memory
* pp rate <rate>  - Profile at a defined rate
* mem             - Get memory stats
* nb              - Get number of clients
`)
	default:
		return fmt.Sprintf("\"%s\" not understood !\n", tokens[0])
	}

	return ""
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
		if response := handleSimpleCommand(line); response != "" {
			log.Error(response)
		}
	}
}

var server *Server

func main() {
	par = NewParameters()
	defer par.Close()

	// We need to reload logging because it was initialized in init (to have one whatever happens)
	LoadLog()

	if m2log.Level >= 3 {
		log.Debug("Starting !")
	}

	if par.HttpListenPort > 0 {
		port := fmt.Sprintf(":%d", par.HttpListenPort)
		go func() {
			if err := http.ListenAndServe(port, nil); err != nil {
				log.Fatal("HTTP listening error: ", err)
			} else {
				log.Debug("HTTP listening on %d", port)
			}
		}()
	}

	log.Debug("Connecting to DB...")
	if err := db.NewSessionSimple("ks_test"); err != nil {
		log.Fatal("DB error: ", err)
	}
	defer db.Close()

	server = NewServer()
	defer server.Close()

	if err := server.Start(); err != nil {
		log.Fatal("Server error: ", err)
	}

	if m2log.Level >= 2 {
		log.Notice("Ready !")
	}

	if par.console {
		go console_handling()
	}

	os.Exit(<-waitForRc)
}
