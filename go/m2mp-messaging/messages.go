package m2mpmsg

import (
	"errors"
	"fmt"
	simple "github.com/bitly/go-simplejson"
	"github.com/gocql/gocql"
	"time"
)

const (
	FIELD_FROM = "_from"
	FIELD_TO   = "_to"
	FIELD_CALL = "_call"
	FIELD_TIME = "_time"
)

type JsonWrapper struct {
	Data *simple.Json
}

func NewJsonWrapper() *JsonWrapper {
	json := simple.New()
	return &JsonWrapper{Data: json}
}

func NewJsonWrapperFromJson(json *simple.Json) *JsonWrapper {
	return &JsonWrapper{Data: json}
}

func (jw *JsonWrapper) From() (out string) {
	out, _ = jw.Data.Get(FIELD_FROM).String()
	return
}

func (jw *JsonWrapper) SetFrom(from string) {
	jw.Data.Set(FIELD_FROM, from)
}

func (jw *JsonWrapper) To() (out string) {
	out, _ = jw.Data.Get(FIELD_TO).String()
	return
}

// The target of a message should be something like:
// * server                      ie: receivers
// * server:device               ie: receivers:bc86fb50-ce5d-11e3-9c1a-0800200c9a66
// * server:device:connection    ie: receivers/xps:bc86fb50-ce5d-11e3-9c1a-0800200c9a66:100
//
// The only actual rule through is that the first part is the NSQ topic or topic/channel (later).
func (jw *JsonWrapper) SetTo(to string) {
	jw.Data.Set(FIELD_TO, to)
}

func (jw *JsonWrapper) Call() (out string) {
	out, _ = jw.Data.Get(FIELD_CALL).String()
	return
}

func (jw *JsonWrapper) SetCall(call string) {
	jw.Data.Set(FIELD_CALL, call)
}

func (jw *JsonWrapper) SetVS(key string, value string) {
	jw.Data.Set(key, value)
}

func (jw *JsonWrapper) SetTime() {
	time := time.Now().UTC().Unix()
	jw.Data.Set(FIELD_TIME, time)
}

var MANDATORY_FIELDS []string

func init() {
	MANDATORY_FIELDS = []string{FIELD_FROM, FIELD_TO, FIELD_CALL}
}

func (jw *JsonWrapper) Check() error {

	for _, field := range MANDATORY_FIELDS {
		if _, err := jw.Data.Get(field).String(); err != nil {
			return errors.New(fmt.Sprintf("Missing field \"%s\"", field))
		}
	}

	return nil
}

func (jw *JsonWrapper) String() string {
	json, _ := jw.Data.MarshalJSON()
	return string(json)
}

func NewMessage(to, call string) *JsonWrapper {
	msg := NewJsonWrapper()
	msg.SetTo(to)
	msg.SetCall(call)
	msg.SetTime()
	return msg
}

func UUIDFromTime(time time.Time) string {
	return gocql.UUIDFromTime(time.UTC()).String()
}
