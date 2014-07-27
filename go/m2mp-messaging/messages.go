package m2mpmsg

import (
	"errors"
	"fmt"
	simple "github.com/likexian/simplejson"
)

type JsonWrapper struct {
	Data *simple.Json
}

func NewJsonWrapper() *JsonWrapper {
	json, _ := simple.Loads("{}")
	return &JsonWrapper{Data: json}
}

func NewJsonWrapperFromJson(json *simple.Json) *JsonWrapper {
	return &JsonWrapper{Data: json}
}

func (jw *JsonWrapper) From() (out string) {
	out, _ = jw.Data.Get("from").String()
	return
}

func (jw *JsonWrapper) SetFrom(from string) {
	jw.Data.Set("from", from)
}

func (jw *JsonWrapper) To() (out string) {
	out, _ = jw.Data.Get("to").String()
	return
}

// The target of a message should be something like:
// * server                      ie: receiver
// * server:device               ie: receiver:bc86fb50-ce5d-11e3-9c1a-0800200c9a66
// * server:device:connection    ie: receiver:bc86fb50-ce5d-11e3-9c1a-0800200c9a66:100
//
// The only actual rule through is that the first part is the NSQ topic or topic/channel (later).
func (jw *JsonWrapper) SetTo(to string) {
	jw.Data.Set("to", to)
}

func (jw *JsonWrapper) Call() (out string) {
	out, _ = jw.Data.Get("call").String()
	return
}

func (jw *JsonWrapper) SetCall(call string) {
	jw.Data.Set("call", call)
}

func (jw *JsonWrapper) SetVS(key string, value string) {
	jw.Data.Set(key, value)
}

var MANDATORY_FIELDS []string

func init() {
	MANDATORY_FIELDS = []string{"from", "to", "call"}
}

func (jw *JsonWrapper) Check() error {

	for _, field := range MANDATORY_FIELDS {
		if _, err := jw.Data.Get(field).String(); err != nil {
			return errors.New(fmt.Sprint("Invalid field ", field))
		}
	}

	return nil
}

func (jw *JsonWrapper) String() string {
	str, _ := simple.Dumps(jw.Data)
	return str
}

func NewMessageEvent(eventType string) *JsonWrapper {
	msg := NewJsonWrapper()
	msg.SetTo(TOPIC_GENERAL_EVENTS)
	msg.SetCall(eventType)
	return msg
}
