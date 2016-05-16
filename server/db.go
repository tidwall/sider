package server

import (
	"container/list"
	"time"
)

type DBItem struct {
	Expires bool
	Value   interface{}
}

type DB struct {
	items   map[string]DBItem
	expires map[string]time.Time
}

func NewDB() *DB {
	return &DB{
		items:   make(map[string]DBItem),
		expires: make(map[string]time.Time),
	}
}

func (db *DB) Len() int {
	return len(db.items)
}

func (db *DB) Flush() {
	db.items = make(map[string]DBItem)
	db.expires = make(map[string]time.Time)
}

func (db *DB) Set(key string, value interface{}) {
	delete(db.expires, key)
	db.items[key] = DBItem{Value: value}
}

func (db *DB) Get(key string) (interface{}, bool) {
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

func (db *DB) Del(key string) (interface{}, bool) {
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

func (db *DB) Expire(key string, when time.Time) bool {
	item, ok := db.items[key]
	if !ok {
		return false
	}
	item.Expires = true
	db.expires[key] = when
	return true
}

func (db *DB) GetExpires(key string) (interface{}, time.Time, bool) {
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

func (db *DB) GetList(key string, create bool) (*list.List, bool) {
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

func (db *DB) GetSet(key string, create bool) (*Set, bool) {
	value, ok := db.Get(key)
	if ok {
		switch v := value.(type) {
		default:
			return nil, false
		case *Set:
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

func (db *DB) Ascend(iterator func(key string, value interface{}) bool) {
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

func (db *DB) Update(key string, value interface{}) {
	item, ok := db.items[key]
	if ok {
		item.Value = value
	} else {
		item = DBItem{Value: value}
	}
	db.items[key] = item
}

func (db *DB) DeleteExpires() (deletedKeys []string) {
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
