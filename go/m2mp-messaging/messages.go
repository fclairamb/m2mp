package m2mpmsg

import (
	simple "github.com/likexian/simplejson"
)

type JsonWrapper struct {
	From, To, Call string
	Data           *simple.Json
}
