package server

import (
	"container/list"
	"strconv"
)

func lpushCommand(c *client) {
	if len(c.args) < 3 {
		c.ReplyAritryError()
		return
	}

	var l *list.List
	key, ok := c.db.Get(c.args[1])
	if ok {
		switch v := key.(type) {
		default:
			c.ReplyTypeError()
			return
		case *list.List:
			l = v
		}
	} else {
		l = list.New()
		c.db.Set(c.args[1], l)
	}
	for i := 2; i < len(c.args); i++ {
		l.PushFront(c.args[i])
	}
	c.ReplyInt(l.Len())
	c.dirty++
}

func rpushCommand(c *client) {
	if len(c.args) < 3 {
		c.ReplyAritryError()
		return
	}
	l, ok := c.db.GetList(c.args[1], true)
	if !ok {
		c.ReplyTypeError()
		return
	}
	for i := 2; i < len(c.args); i++ {
		l.PushBack(c.args[i])
	}
	c.ReplyInt(l.Len())
	c.dirty++
}

func lrangeCommand(c *client) {
	if len(c.args) != 4 {
		c.ReplyAritryError()
		return
	}
	sn, err := strconv.ParseInt(c.args[2], 10, 64)
	if err != nil {
		c.ReplyInvalidIntError()
		return
	}
	en, err := strconv.ParseInt(c.args[3], 10, 64)
	if err != nil {
		c.ReplyInvalidIntError()
		return
	}

	l, ok := c.db.GetList(c.args[1], false)
	if !ok {
		c.ReplyTypeError()
		return
	}
	if l == nil {
		c.ReplyMultiBulkLen(0)
		return
	}

	llen := l.Len()

	var start, stop int
	if sn < 0 {
		start = llen + int(sn)
		if start < 0 {
			start = 0
		}
	} else {
		start = int(sn)
	}
	if en < 0 {
		stop = llen + int(en)
		if stop < 0 {
			c.ReplyMultiBulkLen(0)
			return
		}
	} else {
		stop = int(en)
	}
	if start > stop || start >= llen || llen == 0 {
		c.ReplyMultiBulkLen(0)
		return
	}

	var i int
	var el *list.Element
	if start > llen/2 {
		// read from back
		i = llen - 1
		el = l.Back()
		for el != nil {
			if i == start {
				break
			}
			el = el.Prev()
			i--
		}
	} else {
		// read from front
		i = 0
		el = l.Front()
		for el != nil {
			if i == start {
				break
			}
			el = el.Next()
			i++
		}
	}
	var res []string
	for el != nil {
		if i > stop {
			break
		}
		res = append(res, el.Value.(string))
		el = el.Next()
		i++
	}

	c.ReplyMultiBulkLen(len(res))
	for _, s := range res {
		c.ReplyBulk(s)
	}
}

func llenCommand(c *client) {
	if len(c.args) != 2 {
		c.ReplyAritryError()
		return
	}
	l, ok := c.db.GetList(c.args[1], false)
	if !ok {
		c.ReplyTypeError()
		return
	}
	if l == nil {
		c.ReplyInt(0)
		return
	}
	c.ReplyInt(l.Len())
}

func lpopCommand(c *client) {
	if len(c.args) != 2 {
		c.ReplyAritryError()
		return
	}
	l, ok := c.db.GetList(c.args[1], false)
	if !ok {
		c.ReplyTypeError()
		return
	}
	if l == nil {
		c.ReplyNull()
	} else if l.Len() > 0 {
		el := l.Front()
		l.Remove(el)
		if l.Len() == 0 {
			c.db.Del(c.args[1])
		}
		c.ReplyBulk(el.Value.(string))
		c.dirty++
	} else {
		c.ReplyNull()
	}
}

func rpopCommand(c *client) {
	if len(c.args) != 2 {
		c.ReplyAritryError()
		return
	}
	l, ok := c.db.GetList(c.args[1], false)
	if !ok {
		c.ReplyTypeError()
		return
	}
	if l == nil {
		c.ReplyNull()
	} else if l.Len() > 0 {
		el := l.Back()
		l.Remove(el)
		if l.Len() == 0 {
			c.db.Del(c.args[1])
		}
		c.ReplyBulk(el.Value.(string))
		c.dirty++
	} else {
		c.ReplyNull()
	}
}

func lindexCommand(c *client) {
	if len(c.args) != 3 {
		c.ReplyAritryError()
		return
	}
	sn, err := strconv.ParseInt(c.args[2], 10, 64)
	if err != nil {
		c.ReplyInvalidIntError()
		return
	}

	l, ok := c.db.GetList(c.args[1], false)
	if !ok {
		c.ReplyTypeError()
		return
	}
	if l == nil {
		c.ReplyNull()
		return
	}
	llen := l.Len()
	var start int
	if sn < 0 {
		start = llen + int(sn)
	} else {
		start = int(sn)
	}

	if start < 0 || start >= llen || llen == 0 {
		c.ReplyNull()
		return
	}

	var i int
	var el *list.Element
	if start > llen/2 {
		// read from back
		i = llen - 1
		el = l.Back()
		for el != nil {
			if i == start {
				c.ReplyBulk(el.Value.(string))
				return
			}
			el = el.Prev()
			i--
		}
	} else {
		// read from front
		i = 0
		el = l.Front()
		for el != nil {
			if i == start {
				c.ReplyBulk(el.Value.(string))
				return
			}
			el = el.Next()
			i++
		}
	}
	c.ReplyNull()
}

func lremCommand(c *client) {
	if len(c.args) != 4 {
		c.ReplyAritryError()
		return
	}
	n, err := strconv.ParseInt(c.args[2], 10, 64)
	if err != nil {
		c.ReplyInvalidIntError()
		return
	}
	l, ok := c.db.GetList(c.args[1], false)
	if !ok {
		c.ReplyTypeError()
		return
	}
	if l == nil {
		c.ReplyInt(0)
		return
	}
	count := 0
	v := c.args[3]
	if n == 0 {
		el := l.Front()
		for el != nil {
			next := el.Next()
			if el.Value.(string) == v {
				l.Remove(el)
				count++
				c.dirty++
			}
			el = next
		}
	} else if n > 0 {
		el := l.Front()
		for el != nil {
			if n == 0 {
				break
			}
			next := el.Next()
			if el.Value.(string) == v {
				l.Remove(el)
				count++
				c.dirty++
				n--
			}
			el = next
		}
	} else if n < 0 {
		n *= -1
		el := l.Back()
		for el != nil {
			if n == 0 {
				break
			}
			next := el.Prev()
			if el.Value.(string) == v {
				l.Remove(el)
				count++
				c.dirty++
				n--
			}
			el = next
		}
	}
	c.ReplyInt(count)
}

func lsetCommand(c *client) {
	if len(c.args) != 4 {
		c.ReplyAritryError()
		return
	}
	sn, err := strconv.ParseInt(c.args[2], 10, 64)
	if err != nil {
		c.ReplyInvalidIntError()
		return
	}
	l, ok := c.db.GetList(c.args[1], false)
	if !ok {
		c.ReplyTypeError()
		return
	}
	if l == nil {
		c.ReplyNoSuchKeyError()
		return
	}

	llen := l.Len()
	var start int
	if sn < 0 {
		start = llen + int(sn)
	} else {
		start = int(sn)
	}
	if start < 0 || start >= llen || llen == 0 {
		c.ReplyError("index out of range")
		return
	}

	var i int
	var el *list.Element
	if start > llen/2 {
		// read from back
		i = llen - 1
		el = l.Back()
		for el != nil {
			if i == start {
				el.Value = c.args[3]
				c.ReplyString("OK")
				c.dirty++
				return
			}
			el = el.Prev()
			i--
		}
	} else {
		// read from front
		i = 0
		el = l.Front()
		for el != nil {
			if i == start {
				el.Value = c.args[3]
				c.ReplyString("OK")
				c.dirty++
				return
			}
			el = el.Next()
			i++
		}
	}
	c.ReplyError("index out of range")
}

func ltrimCommand(c *client) {
	if len(c.args) != 4 {
		c.ReplyAritryError()
		return
	}
	sn, err := strconv.ParseInt(c.args[2], 10, 64)
	if err != nil {
		c.ReplyInvalidIntError()
		return
	}
	en, err := strconv.ParseInt(c.args[3], 10, 64)
	if err != nil {
		c.ReplyInvalidIntError()
		return
	}

	l, ok := c.db.GetList(c.args[1], false)
	if !ok {
		c.ReplyTypeError()
		return
	}
	if l == nil {
		c.ReplyString("OK")
		return
	}

	llen := l.Len()

	var start, stop int
	if sn < 0 {
		start = llen + int(sn)
	} else {
		start = int(sn)
	}
	if en < 0 {
		stop = llen + int(en)
	} else {
		stop = int(en)
	}

	var i int
	var el *list.Element
	// delete from front
	i = 0
	el = l.Front()
	for el != nil {
		if i >= start {
			break
		}
		next := el.Next()
		l.Remove(el)
		c.dirty++
		el = next
		i++
	}
	// delete from back
	i = l.Len() - 1
	el = l.Back()
	for el != nil {
		if i < stop-1 {
			break
		}
		prev := el.Prev()
		l.Remove(el)
		c.dirty++
		el = prev
		i--
	}
	if l.Len() == 0 {
		c.db.Del(c.args[1])
	}
	c.ReplyString("OK")
}
