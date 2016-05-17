package server

import (
	"strconv"
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
