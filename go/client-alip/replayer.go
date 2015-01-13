package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

// This replayer replays some raw data files in loop
// at exactly the same time. It helps simulate devices.

type Replayer struct {
	clt      *Client
	Filename string
	Begin    time.Time
	End      time.Time
	Offset   time.Duration
}

type ReplayData struct {
	Date    time.Time
	Type    string
	Content string
}

func ParseReplayData(line string) *ReplayData {
	array := strings.SplitN(line, ",", 3)
	if len(array) == 3 {
		d := &ReplayData{}
		if t, err := doYourBestWithTime(array[0]); err == nil {
			d.Date = t
		} else {
			log.Error("Invalid date: %s: %v", array[0], err)
			return nil
		}
		d.Type = array[1]
		d.Content = array[2]
		return d
	} else {
		log.Error("array.len = %d", len(array))
	}
	return nil
}

func NewReplayer(clt *Client, filename string) *Replayer {
	ins := &Replayer{clt: clt, Filename: filename}
	ins.analyzeFile()
	return ins
}

func (this *Replayer) analyzeFile() {
	if file, err := os.Open(this.Filename); err == nil {
		defer file.Close()
		reader := bufio.NewReader(file)
		lineNb := 0
		for {
			var data *ReplayData = nil
			if line, err := reader.ReadString('\n'); err == nil {
				line := strings.Trim(line, "\n\r")
				data = ParseReplayData(line)
				if data != nil {
					if lineNb == 0 {
						this.Begin = data.Date
					}
					lineNb++
					this.End = data.Date
				}

				//log.Debug("[%d] \"%s\" --> %v", lineNb, line, data)
			} else {
				if err == io.EOF {
					log.Info("Analyzed the begin/end date of file \"%s\".", this.Filename)
				} else {
					log.Error("Error reading line: %v", err)
				}
				break
			}
		}
	}
}

func doYourBestWithTime(input string) (time.Time, error) {
	length := len(input)
	switch {
	// Unix timestamp
	case length >= 10 && length < 12:
		if value, err := strconv.ParseInt(input, 10, 64); err == nil {
			return time.Unix(int64(value), 0), nil
		}
	// Format: YYmmddDDHHMMSS
	case length == 12:
		if t, err := time.Parse("060102150405", input); err == nil {
			return t, nil
		} else {
			return time.Unix(0, 0), errors.New(fmt.Sprintf("GPS1 time: %v", err))
		}
	// Format: YYYYmmddHHMMSS
	case length >= 14:
		if t, err := time.Parse("20060102150405", input); err == nil {
			return t, nil
		} else {
			return time.Unix(0, 0), errors.New(fmt.Sprintf("GPS2 time: %v", err))
		}
	}
	return time.Unix(0, 0), errors.New(fmt.Sprintf("Could not guess time: %v", input))
}

func (this *Replayer) Run() {
	if file, err := os.Open(this.Filename); err == nil {
		log.Info("File start !")
		for {
			file.Seek(0, 0)
			reader := bufio.NewReader(file)
			//lineNb := 0

			this.Offset = time.Duration(time.Now().UTC().UnixNano() - this.Begin.UTC().UnixNano())

			//log.Debug("offset = %d", this.Offset/time.Second)

			for {
				if line, err := reader.ReadString('\n'); err == nil {
					var data *ReplayData = nil
					line := strings.Trim(line, "\n\r")
					data = ParseReplayData(line)

					if data == nil {
						continue
					}

					simulatedTime := time.Unix(0, (data.Date.UTC().UnixNano() + this.Offset.Nanoseconds()))

					data.Date = simulatedTime

					diff := time.Duration(simulatedTime.UnixNano() - time.Now().UTC().UnixNano())

					//log.Debug("diff = %d", diff/time.Second)

					time.Sleep(diff)

					//log.Debug("[%d] \"%s\" --> %v", lineNb, line, data)
					this.clt.Send(fmt.Sprintf("J %s %s %s", data.Date.Format("20060102150405"), data.Type, data.Content))
				} else {
					if err == io.EOF {
						//log.Info("End of file...")
					} else {
						log.Error("Error reading line: %v", err)
					}
					time.Sleep(time.Second * 10)
					break
				}
			}
		}
	} else {
		log.Error("Could not open file %s: %v", this.Filename, err)
	}
}
