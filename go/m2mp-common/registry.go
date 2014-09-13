package m2mp_common

import (
	"fmt"
	"io"
	"log"
	"sync"
)

const DEBUG = false

type Entry struct {
	primary string
	keys    []string
	value   interface{}
}

func NewEntry(key string, value interface{}) *Entry {
	return &Entry{primary: key, keys: make([]string, 0, 1), value: value}
}

func (this *Entry) addKey(key string) {
	for _, k := range this.keys {
		if k == key {
			return
		}
	}
	this.keys = append(this.keys, key)
}

func (this *Entry) removeKey(key string) {
	for i, k := range this.keys {
		if k == key {
			this.keys = append(this.keys[:i], this.keys[i+1:]...)
			return
		}
	}
}

type Registry struct {
	lock    sync.RWMutex
	core    map[string]*Entry
	indexes map[string][]*Entry
}

func NewRegistry() *Registry {
	reg := &Registry{}
	reg.Prepare()
	return reg
}

func (this *Registry) AddPrimaryKey(key string, value interface{}) {
	this.lock.Lock()
	defer this.lock.Unlock()
	// We cleanup any previous entry
	this.removePrimaryKey(key)

	this.core[key] = NewEntry(key, value)

	if DEBUG {
		log.Println("this.core:", this.core)
	}
}

func (this *Registry) removePrimaryKey(key string) {
	entry := this.core[key]
	if entry != nil { // if it exists
		for _, skey := range entry.keys { // for each of the secondary keys
			this.removeSecondaryKey(key, skey)
		}
	}
	delete(this.core, key)
}

func (this *Registry) RemovePrimaryKey(key string) {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.removePrimaryKey(key)
}

func (this *Registry) AddSecondaryKey(first, second string) {
	this.lock.Lock()
	defer this.lock.Unlock()
	entry := this.core[first]

	// We only care if we have a main enry
	if entry != nil {
		entry.addKey(second)

		entries := this.indexes[second]
		if entries == nil {
			entries = make([]*Entry, 0, 1)
		}
		entries = append(entries, entry)
		this.indexes[second] = entries
	}
}

func (this *Registry) removeSecondaryKey(first, second string) {
	sentries := this.indexes[second] // we get the entries
	if sentries != nil {
		for i, sentry := range sentries { // for each entry
			if DEBUG {
				log.Println("senties / first =", first)
			}
			if sentry.primary == first { // if it contains the key as primary key
				// we remove it
				sentries = append(sentries[:i], sentries[i+1:]...)
				if len(sentries) == 0 {
					if DEBUG {
						log.Println("Deleting ", first, second)
					}
					delete(this.indexes, second)
				} else {
					this.indexes[second] = sentries
				}
				break
			}
		}
	}
}

func (this *Registry) RemoveSecondaryKey(first, second string) {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.removeSecondaryKey(first, second)
}

func (this *Registry) Primary(key string) interface{} {
	this.lock.RLock()
	defer this.lock.RUnlock()
	entry := this.core[key]

	if DEBUG {
		log.Println("core[", key, "] = ", entry)
	}

	if entry != nil {
		return entry.value
	} else {
		return nil
	}
}

func (this *Registry) NbPrimary() int {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return len(this.core)
}

func (this *Registry) Secondary(key string) []interface{} {
	this.lock.RLock()
	defer this.lock.RUnlock()
	entries := this.indexes[key]
	values := make([]interface{}, 0, len(entries))
	for _, e := range entries {
		values = append(values, e.value)
	}
	return values
}

func (this *Registry) NbSecondary() int {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return len(this.indexes)
}

func (this *Registry) Prepare() {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.core = make(map[string]*Entry)
	this.indexes = make(map[string][]*Entry)
}

func (this *Registry) Dump(w io.Writer) {
	for k, v := range this.core {
		fmt.Fprintf(w, "core[\"%s\"] = %#v\n", k, v)
	}
	for k, values := range this.indexes {
		fmt.Fprintf(w, "indexes[\"%s\"] = {", k)
		for i, v := range values {
			if i > 0 {
				fmt.Fprintf(w, ", ")
			}
			fmt.Fprintf(w, "\"%s\"", v.primary)
		}
		fmt.Fprintf(w, "}\n")
	}
}

// Check inconsistency.
// This method should never be used outside development. It allows
// to test if we entered the value with two different unique keys.
func (this *Registry) CheckConsistency() []string {
	this.lock.Lock()
	defer this.lock.Unlock()

	listing := make(map[string]string)

	keysToDelete := make([]string, 0)
	// We read all the primary keys
	for k, v := range this.core {
		// And get the pointer
		sptr := fmt.Sprintf("%v", v.value)

		previous := listing[sptr]

		if previous != "" { // If we have  previous entry
			// We delete it and delete also the current one
			keysToDelete = append(keysToDelete, previous, k)
			continue
		}

		// We save exiting pointers
		listing[sptr] = k
	}

	for _, key := range keysToDelete {
		this.removePrimaryKey(key)
	}

	return keysToDelete
}
