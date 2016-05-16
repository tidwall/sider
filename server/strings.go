package server

import (
	"strings"
	"time"
)

func getCommand(c *client) {
	if len(c.args) != 2 {
		c.replyAritryError()
		return
	}
	key, ok := c.db.get(c.args[1])
	if !ok {
		c.replyNull()
		return
	}
	switch s := key.(type) {
	default:
		c.replyTypeError()
		return
	case string:
		c.replyBulk(s)
	}
}

func getsetCommand(c *client) {
	if len(c.args) != 3 {
		c.replyAritryError()
		return
	}
	var res string
	key, ok := c.db.get(c.args[1])
	if ok {
		switch s := key.(type) {
		default:
			c.replyTypeError()
			return
		case string:
			res = s
		}
	}
	c.db.set(c.args[1], c.args[2])
	if !ok {
		c.replyNull()
	} else {
		c.replyBulk(res)
	}
	c.dirty++
}

func incrCommand(c *client) {
	if len(c.args) != 2 {
		c.replyAritryError()
		return
	}
	genericIncrbyCommand(c, 1)
}

func incrbyCommand(c *client) {
	if len(c.args) != 3 {
		c.replyAritryError()
		return
	}
	n, err := atoi(c.args[2])
	if err != nil {
		c.replyError("value is not an integer or out of range")
		return
	}
	genericIncrbyCommand(c, n)
}

func decrCommand(c *client) {
	if len(c.args) != 2 {
		c.replyAritryError()
		return
	}
	genericIncrbyCommand(c, -1)
}

func decrbyCommand(c *client) {
	if len(c.args) != 3 {
		c.replyAritryError()
		return
	}
	n, err := atoi(c.args[2])
	if err != nil {
		c.replyError("value is not an integer or out of range")
		return
	}
	genericIncrbyCommand(c, -n)
}

func genericIncrbyCommand(c *client, delta int) {
	var n int
	value, ok := c.db.get(c.args[1])
	if ok {
		switch s := value.(type) {
		default:
			c.replyTypeError()
			return
		case string:
			var err error
			n, err = atoi(s)
			if err != nil {
				c.replyTypeError()
				return
			}
			n += int(delta)
		}
	} else {
		n = 1
	}
	c.db.update(c.args[1], itoa(n))
	c.replyInt(int(n))
	c.dirty++
}

func setCommand(c *client) {
	if len(c.args) < 3 {
		c.replyAritryError()
		return
	}
	var nx, xx bool
	var ex, px time.Time
	var expires bool
	var when time.Time
	for i := 3; i < len(c.args); i++ {
		switch strings.ToLower(c.args[i]) {
		case "nx":
			if xx {
				c.replySyntaxError()
				return
			}
			nx = true
		case "xx":
			if nx {
				c.replySyntaxError()
				return
			}
			xx = true
		case "ex":
			if !px.IsZero() || i == len(c.args)-1 {
				c.replySyntaxError()
				return
			}
			i++
			n, err := atoi(c.args[i])
			if err != nil {
				c.replySyntaxError()
				return
			}
			ex = time.Now().Add(time.Duration(n) * time.Second)
			expires = true
			when = ex
		case "px":
			if !ex.IsZero() || i == len(c.args)-1 {
				c.replySyntaxError()
				return
			}
			i++
			n, err := atoi(c.args[i])
			if err != nil {
				c.replySyntaxError()
				return
			}
			px = time.Now().Add(time.Duration(n) * time.Millisecond)
			expires = true
			when = px
		}
	}
	if nx || xx {
		_, ok := c.db.get(c.args[1])
		if (ok && nx) || (!ok && xx) {
			c.replyNull()
			return
		}
	}
	c.db.set(c.args[1], c.args[2])
	if expires {
		c.db.expire(c.args[1], when)
	}
	c.replyString("OK")
	c.dirty++
}

func setnxCommand(c *client) {
	if len(c.args) != 3 {
		c.replyAritryError()
		return
	}
	_, ok := c.db.get(c.args[1])
	if ok {
		c.replyInt(0)
		return
	}
	c.db.set(c.args[1], c.args[2])
	c.replyInt(1)
	c.dirty++
}

func msetCommand(c *client) {
	if len(c.args) < 3 || (len(c.args)-1)%2 != 0 {
		c.replyAritryError()
		return
	}
	for i := 1; i < len(c.args); i += 2 {
		c.db.set(c.args[i+0], c.args[i+1])
		c.dirty++
	}
	c.replyString("OK")
}

func msetnxCommand(c *client) {
	if len(c.args) < 3 || (len(c.args)-1)%2 != 0 {
		c.replyAritryError()
		return
	}
	for i := 1; i < len(c.args); i += 2 {
		_, ok := c.db.get(c.args[1])
		if ok {
			c.replyInt(0)
			return
		}
	}
	for i := 1; i < len(c.args); i += 2 {
		c.db.set(c.args[i+0], c.args[i+1])
		c.dirty++
	}
	c.replyInt(1)
}

func appendCommand(c *client) {
	if len(c.args) != 3 {
		c.replyAritryError()
		return
	}
	key, ok := c.db.get(c.args[1])
	if !ok {
		c.db.set(c.args[1], c.args[2])
		c.replyInt(len(c.args[2]))
		c.dirty++
		return
	}
	switch s := key.(type) {
	default:
		c.replyTypeError()
		return
	case string:
		s += c.args[2]
		c.db.set(c.args[1], s)
		c.replyInt(len(s))
		c.dirty++
	}
}

func bitcountCommand(c *client) {
	var start, end int
	var all bool
	switch len(c.args) {
	default:
		c.replyAritryError()
	case 2:
		all = true
	case 4:
		n1, err1 := atoi(c.args[2])
		n2, err2 := atoi(c.args[3])
		if err1 != nil || err2 != nil {
			c.replyError("value is not an integer or out of range")
			return
		}
		start, end = int(n1), int(n2)
	}
	key, ok := c.db.get(c.args[1])
	if !ok {
		c.replyInt(0)
		return
	}
	switch s := key.(type) {
	default:
		c.replyTypeError()
		return
	case string:
		var count int
		if all {
			start, end = 0, len(s)
		} else {
			if start < 0 {
				start = len(s) + start
				if start < 0 {
					start = 0
				}
			}
			if end < 0 {
				end = len(s) + end
				if end < 0 {
					end = 0
				}
			}
		}
		for i := start; i <= end && i < len(s); i++ {
			c := s[i]
			for j := 0; j < 8; j++ {
				count += int((c >> uint(j)) & 0x01)
			}
		}
		c.replyInt(count)
	}
}

func mgetCommand(c *client) {
	if len(c.args) < 2 {
		c.replyAritryError()
		return
	}
	c.replyMultiBulkLen(len(c.args) - 1)
	for i := 1; i < len(c.args); i++ {
		key, ok := c.db.get(c.args[i])
		if !ok {
			c.replyNull()
		} else if s, ok := key.(string); ok {
			c.replyBulk(s)
		} else {
			c.replyNull()
		}
	}
}
