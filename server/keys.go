package server

import (
	"strconv"
	"time"
)

func delCommand(c *client) {
	if len(c.args) < 2 {
		c.ReplyAritryError()
		return
	}
	count := 0
	for i := 1; i < len(c.args); i++ {
		if _, ok := c.db.Del(c.args[i]); ok {
			count++
		}
	}
	c.dirty += count
	c.ReplyInt(count)
}

func renameCommand(c *client) {
	if len(c.args) != 3 {
		c.ReplyAritryError()
		return
	}
	key, ok := c.db.Get(c.args[1])
	if !ok {
		c.ReplyError("no such key")
		return
	}
	c.db.Del(c.args[1])
	c.db.Set(c.args[2], key)
	c.dirty++
	c.ReplyString("OK")
}

func renamenxCommand(c *client) {
	if len(c.args) != 3 {
		c.ReplyAritryError()
		return
	}
	key, ok := c.db.Get(c.args[1])
	if !ok {
		c.ReplyError("no such key")
		return
	}
	_, ok = c.db.Get(c.args[2])
	if ok {
		c.ReplyInt(0)
		return
	}
	c.db.Del(c.args[1])
	c.db.Set(c.args[2], key)
	c.ReplyInt(1)
	c.dirty++
}

func keysCommand(c *client) {
	if len(c.args) != 2 {
		c.ReplyAritryError()
		return
	}
	var keys []string
	pattern := parsePattern(c.args[1])
	c.db.Ascend(func(key string, value interface{}) bool {
		if pattern.Match(key) {
			keys = append(keys, key)
		}
		return true
	})
	c.ReplyMultiBulkLen(len(keys))
	for _, key := range keys {
		c.ReplyString(key)
	}
}

func typeCommand(c *client) {
	if len(c.args) != 2 {
		c.ReplyAritryError()
		return
	}
	typ := c.db.GetType(c.args[1])
	c.ReplyString(typ)
}

func randomkeyCommand(c *client) {
	if len(c.args) != 1 {
		c.ReplyAritryError()
		return
	}
	got := false
	c.db.Ascend(func(key string, value interface{}) bool {
		c.ReplyBulk(key)
		got = true
		return false
	})
	if !got {
		c.ReplyNull()
	}
}

func existsCommand(c *client) {
	if len(c.args) == 1 {
		c.ReplyAritryError()
		return
	}
	var count int
	for i := 1; i < len(c.args); i++ {
		if _, ok := c.db.Get(c.args[i]); ok {
			count++
		}
	}
	c.ReplyInt(count)
}
func expireCommand(c *client) {
	if len(c.args) != 3 {
		c.ReplyAritryError()
		return
	}
	seconds, err := strconv.ParseInt(c.args[2], 10, 64)
	if err != nil {
		c.ReplyError("value is not an integer or out of range")
		return
	}
	if c.db.Expire(c.args[1], time.Now().Add(time.Duration(seconds)*time.Second)) {
		c.ReplyInt(1)
		c.dirty++
	} else {
		c.ReplyInt(0)
	}
}
func ttlCommand(c *client) {
	if len(c.args) != 2 {
		c.ReplyAritryError()
		return
	}
	_, expires, ok := c.db.GetExpires(c.args[1])
	if !ok {
		c.ReplyInt(-2)
	} else if expires.IsZero() {
		c.ReplyInt(-1)
	} else {
		c.ReplyInt(int(expires.Sub(time.Now()) / time.Second))
	}
}
