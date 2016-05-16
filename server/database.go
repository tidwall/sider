package server

import (
	"bytes"
	"container/list"
	"time"
)

type dbItem struct {
	expires bool
	value   interface{}
}

type database struct {
	items   map[string]dbItem
	expires map[string]time.Time
	aofbuf  bytes.Buffer
}

func newDB() *database {
	return &database{
		items:   make(map[string]dbItem),
		expires: make(map[string]time.Time),
	}
}

func (db *database) Len() int {
	return len(db.items)
}

func (db *database) Flush() {
	db.items = make(map[string]dbItem)
	db.expires = make(map[string]time.Time)
}

func (db *database) Set(key string, value interface{}) {
	delete(db.expires, key)
	db.items[key] = dbItem{value: value}
}

func (db *database) Get(key string) (interface{}, bool) {
	item, ok := db.items[key]
	if !ok {
		return nil, false
	}
	if item.expires {
		if t, ok := db.expires[key]; ok {
			if time.Now().After(t) {
				return nil, false
			}
		}
	}
	return item.value, true
}

func (db *database) GetType(key string) string {
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

func (db *database) Del(key string) (interface{}, bool) {
	item, ok := db.items[key]
	if !ok {
		return nil, false
	}
	delete(db.items, key)
	if item.expires {
		delete(db.expires, key)
		if t, ok := db.expires[key]; ok {
			if time.Now().After(t) {
				return nil, false
			}
		}
	}
	return item.value, true
}

func (db *database) Expire(key string, when time.Time) bool {
	item, ok := db.items[key]
	if !ok {
		return false
	}
	item.expires = true
	db.items[key] = item
	db.expires[key] = when
	return true
}

func (db *database) GetExpires(key string) (interface{}, time.Time, bool) {
	item, ok := db.items[key]
	if !ok {
		return nil, time.Time{}, false
	}
	var expires time.Time
	if item.expires {
		if t, ok := db.expires[key]; ok {
			expires = t
			if time.Now().After(t) {
				return nil, time.Time{}, false
			}
		}
	}
	return item.value, expires, true
}

func (db *database) GetList(key string, create bool) (*list.List, bool) {
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

func (db *database) GetSet(key string, create bool) (*setT, bool) {
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

func (db *database) Ascend(iterator func(key string, value interface{}) bool) {
	now := time.Now()
	for key, item := range db.items {
		if item.expires {
			if t, ok := db.expires[key]; ok {
				if now.After(t) {
					continue
				}
			}
		}
		if !iterator(key, item.value) {
			return
		}
	}
}

func (db *database) Update(key string, value interface{}) {
	item, ok := db.items[key]
	if ok {
		item.value = value
	} else {
		item = dbItem{value: value}
	}
	db.items[key] = item
}

func (db *database) DeleteExpires() (deletedKeys []string) {
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
