package server

import (
	"bytes"
	"time"
)

type dbItem struct {
	expires bool
	value   interface{}
}

type database struct {
	num     int
	items   map[string]dbItem
	expires map[string]time.Time
	aofbuf  bytes.Buffer
}

func newDB(num int) *database {
	return &database{
		num:     num,
		items:   make(map[string]dbItem),
		expires: make(map[string]time.Time),
	}
}

func (db *database) len() int {
	return len(db.items)
}

func (db *database) flush() {
	db.items = make(map[string]dbItem)
	db.expires = make(map[string]time.Time)
}

func (db *database) set(key string, value interface{}) {
	delete(db.expires, key)
	db.items[key] = dbItem{value: value}
}

func (db *database) get(key string) (interface{}, bool) {
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

func (db *database) getType(key string) string {
	v, ok := db.get(key)
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
	case *list:
		return "list"
	case *set:
		return "set"
	}
}

func (db *database) del(key string) (interface{}, bool) {
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

func (db *database) expire(key string, when time.Time) bool {
	item, ok := db.items[key]
	if !ok {
		return false
	}
	item.expires = true
	db.items[key] = item
	db.expires[key] = when
	return true
}

func (db *database) getExpires(key string) (interface{}, time.Time, bool) {
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

func (db *database) getList(key string, create bool) (*list, bool) {
	value, ok := db.get(key)
	if ok {
		switch v := value.(type) {
		default:
			return nil, false
		case *list:
			return v, true
		}
	}
	if create {
		l := newList()
		db.set(key, l)
		return l, true
	}
	return nil, true
}

func (db *database) getSet(key string, create bool) (*set, bool) {
	value, ok := db.get(key)
	if ok {
		switch v := value.(type) {
		default:
			return nil, false
		case *set:
			return v, true
		}
	}
	if create {
		st := newSet()
		db.set(key, st)
		return st, true
	}
	return nil, true
}

func (db *database) ascend(iterator func(key string, value interface{}) bool) {
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

func (db *database) update(key string, value interface{}) {
	item, ok := db.items[key]
	if ok {
		item.value = value
	} else {
		item = dbItem{value: value}
	}
	db.items[key] = item
}

func (db *database) deleteExpires() (deletedKeys []string) {
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
