package server

import (
	"bytes"
	"container/list"
	"time"
)

type dbItemT struct {
	Expires bool
	Value   interface{}
}

type dbT struct {
	items   map[string]dbItemT
	expires map[string]time.Time
	aofbuf  bytes.Buffer
}

func NewDB() *dbT {
	return &dbT{
		items:   make(map[string]dbItemT),
		expires: make(map[string]time.Time),
	}
}

func (db *dbT) Len() int {
	return len(db.items)
}

func (db *dbT) Flush() {
	db.items = make(map[string]dbItemT)
	db.expires = make(map[string]time.Time)
}

func (db *dbT) Set(key string, value interface{}) {
	delete(db.expires, key)
	db.items[key] = dbItemT{Value: value}
}

func (db *dbT) Get(key string) (interface{}, bool) {
	item, ok := db.items[key]
	if !ok {
		return nil, false
	}
	if item.Expires {
		if t, ok := db.expires[key]; ok {
			if time.Now().After(t) {
				return nil, false
			}
		}
	}
	return item.Value, true
}

func (db *dbT) GetType(key string) string {
	v, ok := db.Get(key)
	if !ok {
		return "none"
	}
	switch v.(type) {
	default:
		// should not be reached
		return "unknown"
	case int:
		return "string"
	case string:
		return "string"
	case *list.List:
		return "list"
	case *setT:
		return "set"
	}
}

func (db *dbT) Del(key string) (interface{}, bool) {
	item, ok := db.items[key]
	if !ok {
		return nil, false
	}
	delete(db.items, key)
	if item.Expires {
		delete(db.expires, key)
		if t, ok := db.expires[key]; ok {
			if time.Now().After(t) {
				return nil, false
			}
		}
	}
	return item.Value, true
}

func (db *dbT) Expire(key string, when time.Time) bool {
	item, ok := db.items[key]
	if !ok {
		return false
	}
	item.Expires = true
	db.items[key] = item
	db.expires[key] = when
	return true
}

func (db *dbT) GetExpires(key string) (interface{}, time.Time, bool) {
	item, ok := db.items[key]
	if !ok {
		return nil, time.Time{}, false
	}
	var expires time.Time
	if item.Expires {
		if t, ok := db.expires[key]; ok {
			expires = t
			if time.Now().After(t) {
				return nil, time.Time{}, false
			}
		}
	}
	return item.Value, expires, true
}

func (db *dbT) GetList(key string, create bool) (*list.List, bool) {
	value, ok := db.Get(key)
	if ok {
		switch v := value.(type) {
		default:
			return nil, false
		case *list.List:
			return v, true
		}
	}
	if create {
		l := list.New()
		db.Set(key, l)
		return l, true
	}
	return nil, true
}

func (db *dbT) GetSet(key string, create bool) (*setT, bool) {
	value, ok := db.Get(key)
	if ok {
		switch v := value.(type) {
		default:
			return nil, false
		case *setT:
			return v, true
		}
	}
	if create {
		st := NewSet()
		db.Set(key, st)
		return st, true
	}
	return nil, true
}

func (db *dbT) Ascend(iterator func(key string, value interface{}) bool) {
	now := time.Now()
	for key, item := range db.items {
		if item.Expires {
			if t, ok := db.expires[key]; ok {
				if now.After(t) {
					continue
				}
			}
		}
		if !iterator(key, item.Value) {
			return
		}
	}
}

func (db *dbT) Update(key string, value interface{}) {
	item, ok := db.items[key]
	if ok {
		item.Value = value
	} else {
		item = dbItemT{Value: value}
	}
	db.items[key] = item
}

func (db *dbT) DeleteExpires() (deletedKeys []string) {
	now := time.Now()
	for key, t := range db.expires {
		if now.Before(t) {
			continue
		}
		delete(db.items, key)
		delete(db.expires, key)
		deletedKeys = append(deletedKeys, key)
	}
	return deletedKeys
}
