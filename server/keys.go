package server

import (
	"sort"
	"strconv"
	"strings"
	"time"
)

func delCommand(c *client) {
	if len(c.args) < 2 {
		c.replyAritryError()
		return
	}
	count := 0
	for i := 1; i < len(c.args); i++ {
		if _, ok := c.db.del(c.args[i]); ok {
			count++
		}
	}
	c.dirty += count
	c.replyInt(count)
}

func renameCommand(c *client) {
	if len(c.args) != 3 {
		c.replyAritryError()
		return
	}
	key, ok := c.db.get(c.args[1])
	if !ok {
		c.replyError("no such key")
		return
	}
	c.db.del(c.args[1])
	c.db.set(c.args[2], key)
	c.dirty++
	c.replyString("OK")
}

func renamenxCommand(c *client) {
	if len(c.args) != 3 {
		c.replyAritryError()
		return
	}
	key, ok := c.db.get(c.args[1])
	if !ok {
		c.replyError("no such key")
		return
	}
	_, ok = c.db.get(c.args[2])
	if ok {
		c.replyInt(0)
		return
	}
	c.db.del(c.args[1])
	c.db.set(c.args[2], key)
	c.replyInt(1)
	c.dirty++
}

func keysCommand(c *client) {
	if len(c.args) != 2 {
		c.replyAritryError()
		return
	}
	var keys []string
	pattern := parsePattern(c.args[1])
	c.db.ascend(func(key string, value interface{}) bool {
		if pattern.match(key) {
			keys = append(keys, key)
		}
		return true
	})
	c.replyMultiBulkLen(len(keys))
	for _, key := range keys {
		c.replyString(key)
	}
}

func typeCommand(c *client) {
	if len(c.args) != 2 {
		c.replyAritryError()
		return
	}
	typ := c.db.getType(c.args[1])
	c.replyString(typ)
}

func randomkeyCommand(c *client) {
	if len(c.args) != 1 {
		c.replyAritryError()
		return
	}
	got := false
	c.db.ascend(func(key string, value interface{}) bool {
		c.replyBulk(key)
		got = true
		return false
	})
	if !got {
		c.replyNull()
	}
}

func existsCommand(c *client) {
	if len(c.args) == 1 {
		c.replyAritryError()
		return
	}
	var count int
	for i := 1; i < len(c.args); i++ {
		if _, ok := c.db.get(c.args[i]); ok {
			count++
		}
	}
	c.replyInt(count)
}
func expireCommand(c *client) {
	if len(c.args) != 3 {
		c.replyAritryError()
		return
	}
	seconds, err := strconv.ParseInt(c.args[2], 10, 64)
	if err != nil {
		c.replyError("value is not an integer or out of range")
		return
	}
	if c.db.expire(c.args[1], time.Now().Add(time.Duration(seconds)*time.Second)) {
		c.replyInt(1)
		c.dirty++
	} else {
		c.replyInt(0)
	}
}
func ttlCommand(c *client) {
	if len(c.args) != 2 {
		c.replyAritryError()
		return
	}
	_, expires, ok := c.db.getExpires(c.args[1])
	if !ok {
		c.replyInt(-2)
	} else if expires.IsZero() {
		c.replyInt(-1)
	} else {
		c.replyInt(int(expires.Sub(time.Now()) / time.Second))
	}
}
func moveCommand(c *client) {
	if len(c.args) != 3 {
		c.replyAritryError()
		return
	}
	num, err := strconv.ParseUint(c.args[2], 10, 32)
	if err != nil {
		c.replyError("index out of range")
		return
	}

	value, ok := c.db.get(c.args[1])
	if !ok {
		c.replyInt(0)
		return
	}
	db := c.s.selectDB(int(num))
	_, ok = db.get(c.args[1])
	if ok {
		c.replyInt(0)
		return
	}
	db.set(c.args[1], value)
	c.db.del(c.args[1])
	c.replyInt(1)
	c.dirty++
}

type sortValues struct {
	db         *database
	asc        bool
	alpha      bool
	arr        []string
	byPrefix   string
	bySuffix   string
	byProvided bool
	invalid    bool
}

func (v *sortValues) Len() int {
	return len(v.arr)
}

func isless(s1, s2 string, alpha bool) (less bool, ok bool) {
	if alpha {
		return s1 < s2, true
	}
	n1, err := strconv.ParseFloat(s1, 64)
	if err != nil {
		return false, false
	}
	n2, err := strconv.ParseFloat(s2, 64)
	if err != nil {
		return false, false
	}
	return n1 < n2, true
}

func (v *sortValues) Less(i, j int) bool {
	if v.invalid {
		return false
	}
	s1, s2 := v.arr[i], v.arr[j]

	// test if the values are less from the 'by' clause
	if v.byProvided {
		v1, _ := v.db.get(v.byPrefix + s1 + v.bySuffix)
		v2, _ := v.db.get(v.byPrefix + s2 + v.bySuffix)
		if vs1, ok := v1.(string); ok {
			if vs2, ok := v2.(string); ok {
				// both exist, compare
				if !v.alpha {
					vn1, err := strconv.ParseFloat(vs1, 64)
					if err != nil {
						v.invalid = true
						return false
					}
					vn2, err := strconv.ParseFloat(vs2, 64)
					if err != nil {
						v.invalid = true
						return false
					}
					if vn1 != vn2 {
						return vn1 < vn2
					}
				} else {
					if vs1 != vs2 {
						return vs1 < vs2
					}
				}
			} else {
				// 1st exists, but not the 2nd, return false
				if !v.alpha {
					_, err := strconv.ParseFloat(vs1, 64)
					if err != nil {
						v.invalid = true
						return false
					}
				}
				return false
			}
		} else if vs2, ok := v2.(string); ok {
			// 2nd exists, but not the 1st, return true
			if !v.alpha {
				_, err := strconv.ParseFloat(vs2, 64)
				if err != nil {
					v.invalid = true
					return false
				}
			}
			return true
		} else {
			// neither exist, noop
		}
	}

	less, ok := isless(s1, s2, v.alpha)
	if !ok {
		v.invalid = true
		return false
	}
	return less
}

func (v *sortValues) Swap(i, j int) {
	v.arr[i], v.arr[j] = v.arr[j], v.arr[i]
}

func sortCommand(c *client) {
	if len(c.args) < 2 {
		c.replyAritryError()
		return
	}
	asc := true
	alpha := false
	store := ""
	storeProvided := false
	offset := 0
	count := 0
	limitProvided := false
	by := ""
	byPrefix := ""
	bySuffix := ""
	byProvided := false
	noSort := false
	gets := []string(nil)
	for i := 2; i < len(c.args); i++ {
		switch strings.ToLower(c.args[i]) {
		default:
			c.replySyntaxError()
			return
		case "get":
			i++
			if i == len(c.args) {
				c.replySyntaxError()
				return
			}
			gets = append(gets, c.args[i])
		case "by":
			i++
			if i == len(c.args) {
				c.replySyntaxError()
				return
			}
			by = c.args[i]
			idx := strings.Index(by, "*")
			if idx != -1 {
				byPrefix = by[:idx]
				bySuffix = by[idx+1:]
				byProvided = true
			} else {
				noSort = true
			}
		case "alpha":
			alpha = true
		case "asc":
			asc = true
		case "desc":
			asc = false
		case "store":
			i++
			if i == len(c.args) {
				c.replySyntaxError()
				return
			}
			storeProvided = true
			store = c.args[i]
		case "limit":
			i += 2
			if i == len(c.args) {
				c.replySyntaxError()
				return
			}
			limitProvided = true
			n1, err := strconv.ParseInt(c.args[i-1], 10, 64)
			if err != nil {
				c.replyError("value is not an integer or out of range")
				return
			}
			n2, err := strconv.ParseInt(c.args[i], 10, 64)
			if err != nil {
				c.replyError("value is not an integer or out of range")
				return
			}
			if n1 < 0 {
				n1 = 0
			}
			if n2 < 0 {
				n2 = 0
			}
			offset = int(n1)
			count = int(n2)
		}
	}
	if false {
		println(asc, alpha, store, storeProvided, offset, count, limitProvided, by, byProvided, gets)
	}
	key, ok := c.db.get(c.args[1])
	if !ok || key == nil {
		c.replyMultiBulkLen(0)
		return
	}

	var arr []string
	switch v := key.(type) {
	default:
		c.replyTypeError()
		return
	case *list:
		arr = v.strArr()
	case *set:
		arr = v.strArr()
	}
	if limitProvided {
		if offset >= len(arr) {
			c.replyMultiBulkLen(0)
			return
		}
		if offset+count > len(arr) {
			count = len(arr) - offset
		}
	}
	if !noSort {
		values := &sortValues{
			db:         c.db,
			asc:        asc,
			alpha:      alpha,
			arr:        arr,
			byPrefix:   byPrefix,
			bySuffix:   bySuffix,
			byProvided: byProvided,
		}
		sort.Sort(values)
		if values.invalid {
			c.replyError("One or more scores can't be converted into double")
			return
		}
	}
	if limitProvided {
		arr = arr[offset : offset+count]
	}
	if storeProvided {
		l := newList()
		l.rpush(arr...)
		c.db.set(store, l)
		c.replyInt(l.len())
		c.dirty++
		return
	}
	c.replyMultiBulkLen(len(arr))
	for _, value := range arr {
		c.replyBulk(value)
	}
}

func expireatCommand(c *client) {
	if len(c.args) != 3 {
		c.replyAritryError()
		return
	}
	seconds, err := strconv.ParseInt(c.args[2], 10, 64)
	if err != nil {
		c.replyError("value is not an integer or out of range")
		return
	}
	if c.db.expire(c.args[1], time.Unix(seconds, 0)) {
		c.replyInt(1)
		c.dirty++
	} else {
		c.replyInt(0)
	}
}
