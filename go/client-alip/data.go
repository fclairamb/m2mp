package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"
)

type Setting struct {
	Value   string
	Changed bool
}

type Data struct {
	Ident      string
	Settings   map[string]Setting
	ReplayFile string
}

func NewData(file string) (*Data, error) {
	if file == "" {
		data := &Data{
			Ident:    fmt.Sprintf("imei:%d", time.Now().UnixNano()%1000000),
			Settings: make(map[string]Setting),
		}
		return data, nil
	}

	if content, err := ioutil.ReadFile(file); err == nil {
		data := &Data{}
		if err := json.Unmarshal(content, data); err == nil {
			return data, nil
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}

func (this *Data) Init() {

}

func (this *Data) Save(file string) error {
	if content, err := json.Marshal(this); err == nil {
		if err := ioutil.WriteFile(file, content, 0644); err != nil {
			return err
		}
	} else {
		return err
	}
	return nil
}
