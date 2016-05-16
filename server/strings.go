package server

import (
	"strings"
	"time"
)

func getCommand(c *client) {
	if len(c.args) != 2 {
		c.ReplyAritryError()
		return
	}
	key, ok := c.db.Get(c.args[1])
	if !ok {
		c.ReplyNull()
		return
	}
	switch s := key.(type) {
	default:
		c.ReplyTypeError()
		return
	case string:
		c.ReplyBulk(s)
	}
}

func getsetCommand(c *client) {
	if len(c.args) != 3 {
		c.ReplyAritryError()
		return
	}
	var res string
	key, ok := c.db.Get(c.args[1])
	if ok {
		switch s := key.(type) {
		default:
			c.ReplyTypeError()
			return
		case string:
			res = s
		}
	}
	c.db.Set(c.args[1], c.args[2])
	if !ok {
		c.ReplyNull()
	} else {
		c.ReplyBulk(res)
	}
	c.dirty++
}

func incrCommand(c *client) {
	if len(c.args) != 2 {
		c.ReplyAritryError()
		return
	}
	genericIncrbyCommand(c, 1)
}

func incrbyCommand(c *client) {
	if len(c.args) != 3 {
		c.ReplyAritryError()
		return
	}
	n, err := atoi(c.args[2])
	if err != nil {
		c.ReplyError("value is not an integer or out of range")
		return
	}
	genericIncrbyCommand(c, n)
}

func decrCommand(c *client) {
	if len(c.args) != 2 {
		c.ReplyAritryError()
		return
	}
	genericIncrbyCommand(c, -1)
}

func decrbyCommand(c *client) {
	if len(c.args) != 3 {
		c.ReplyAritryError()
		return
	}
	n, err := atoi(c.args[2])
	if err != nil {
		c.ReplyError("value is not an integer or out of range")
		return
	}
	genericIncrbyCommand(c, -n)
}

func genericIncrbyCommand(c *client, delta int) {
	var n int
	value, ok := c.db.Get(c.args[1])
	if ok {
		switch s := value.(type) {
		default:
			c.ReplyTypeError()
			return
		case string:
			var err error
			n, err = atoi(s)
			if err != nil {
				c.ReplyTypeError()
				return
			}
			n += int(delta)
		}
	} else {
		n = 1
	}
	c.db.Update(c.args[1], itoa(n))
	c.ReplyInt(int(n))
	c.dirty++
}

func setCommand(c *client) {
	if len(c.args) < 3 {
		c.ReplyAritryError()
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
				c.ReplySyntaxError()
				return
			}
			nx = true
		case "xx":
			if nx {
				c.ReplySyntaxError()
				return
			}
			xx = true
		case "ex":
			if !px.IsZero() || i == len(c.args)-1 {
				c.ReplySyntaxError()
				return
			}
			i++
			n, err := atoi(c.args[i])
			if err != nil {
				c.ReplySyntaxError()
				return
			}
			ex = time.Now().Add(time.Duration(n) * time.Second)
			expires = true
			when = ex
		case "px":
			if !ex.IsZero() || i == len(c.args)-1 {
				c.ReplySyntaxError()
				return
			}
			i++
			n, err := atoi(c.args[i])
			if err != nil {
				c.ReplySyntaxError()
				return
			}
			px = time.Now().Add(time.Duration(n) * time.Millisecond)
			expires = true
			when = px
		}
	}
	if nx || xx {
		_, ok := c.db.Get(c.args[1])
		if (ok && nx) || (!ok && xx) {
			c.ReplyNull()
			return
		}
	}
	c.db.Set(c.args[1], c.args[2])
	if expires {
		c.db.Expire(c.args[1], when)
	}
	c.ReplyString("OK")
	c.dirty++
}

func setnxCommand(c *client) {
	if len(c.args) != 3 {
		c.ReplyAritryError()
		return
	}
	_, ok := c.db.Get(c.args[1])
	if ok {
		c.ReplyInt(0)
		return
	}
	c.db.Set(c.args[1], c.args[2])
	c.ReplyInt(1)
	c.dirty++
}

func msetCommand(c *client) {
	if len(c.args) < 3 || (len(c.args)-1)%2 != 0 {
		c.ReplyAritryError()
		return
	}
	for i := 1; i < len(c.args); i += 2 {
		c.db.Set(c.args[i+0], c.args[i+1])
		c.dirty++
	}
	c.ReplyString("OK")
}

func msetnxCommand(c *client) {
	if len(c.args) < 3 || (len(c.args)-1)%2 != 0 {
		c.ReplyAritryError()
		return
	}
	for i := 1; i < len(c.args); i += 2 {
		_, ok := c.db.Get(c.args[1])
		if ok {
			c.ReplyInt(0)
			return
		}
	}
	for i := 1; i < len(c.args); i += 2 {
		c.db.Set(c.args[i+0], c.args[i+1])
		c.dirty++
	}
	c.ReplyInt(1)
}

func appendCommand(c *client) {
	if len(c.args) != 3 {
		c.ReplyAritryError()
		return
	}
	key, ok := c.db.Get(c.args[1])
	if !ok {
		c.db.Set(c.args[1], c.args[2])
		c.ReplyInt(len(c.args[2]))
		c.dirty++
		return
	}
	switch s := key.(type) {
	default:
		c.ReplyTypeError()
		return
	case string:
		s += c.args[2]
		c.db.Set(c.args[1], s)
		c.ReplyInt(len(s))
		c.dirty++
	}
}

func bitcountCommand(c *client) {
	var start, end int
	var all bool
	switch len(c.args) {
	default:
		c.ReplyAritryError()
	case 2:
		all = true
	case 4:
		n1, err1 := atoi(c.args[2])
		n2, err2 := atoi(c.args[3])
		if err1 != nil || err2 != nil {
			c.ReplyError("value is not an integer or out of range")
			return
		}
		start, end = int(n1), int(n2)
	}
	key, ok := c.db.Get(c.args[1])
	if !ok {
		c.ReplyInt(0)
		return
	}
	switch s := key.(type) {
	default:
		c.ReplyTypeError()
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
		c.ReplyInt(count)
	}
}

func mgetCommand(c *client) {
	if len(c.args) < 2 {
		c.ReplyAritryError()
		return
	}
	c.ReplyMultiBulkLen(len(c.args) - 1)
	for i := 1; i < len(c.args); i++ {
		key, ok := c.db.Get(c.args[i])
		if !ok {
			c.ReplyNull()
		} else if s, ok := key.(string); ok {
			c.ReplyBulk(s)
		} else {
			c.ReplyNull()
		}
	}
}
