package server

import (
	"container/list"
	"strconv"
)

func lpushCommand(client *Client) {
	if len(client.args) < 3 {
		client.ReplyAritryError()
		return
	}

	var l *list.List
	key, ok := client.server.GetKey(client.args[1])
	if ok {
		switch v := key.(type) {
		default:
			client.ReplyTypeError()
			return
		case *list.List:
			l = v
		}
	} else {
		l = list.New()
		client.server.SetKey(client.args[1], l)
	}
	for i := 2; i < len(client.args); i++ {
		l.PushFront(scopy(client.args[i]))
	}
	client.ReplyInt(l.Len())
	client.dirty++
}

func rpushCommand(client *Client) {
	if len(client.args) < 3 {
		client.ReplyAritryError()
		return
	}
	l, ok := client.server.GetKeyList(client.args[1], true)
	if !ok {
		client.ReplyTypeError()
		return
	}
	for i := 2; i < len(client.args); i++ {
		l.PushBack(scopy(client.args[i]))
	}
	client.ReplyInt(l.Len())
	client.dirty++
}

func lrangeCommand(client *Client) {
	if len(client.args) != 4 {
		client.ReplyAritryError()
		return
	}
	sn, err := strconv.ParseInt(client.args[2], 10, 64)
	if err != nil {
		client.ReplyInvalidIntError()
		return
	}
	en, err := strconv.ParseInt(client.args[3], 10, 64)
	if err != nil {
		client.ReplyInvalidIntError()
		return
	}

	l, ok := client.server.GetKeyList(client.args[1], false)
	if !ok {
		client.ReplyTypeError()
		return
	}
	if l == nil {
		client.ReplyMultiBulkLen(0)
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
			client.ReplyMultiBulkLen(0)
			return
		}
	} else {
		stop = int(en)
	}
	if start > stop || start >= llen || llen == 0 {
		client.ReplyMultiBulkLen(0)
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

	client.ReplyMultiBulkLen(len(res))
	for _, s := range res {
		client.ReplyBulk(s)
	}
}

func llenCommand(client *Client) {
	if len(client.args) != 2 {
		client.ReplyAritryError()
		return
	}
	l, ok := client.server.GetKeyList(client.args[1], false)
	if !ok {
		client.ReplyTypeError()
		return
	}
	if l == nil {
		client.ReplyInt(0)
		return
	}
	client.ReplyInt(l.Len())
}

func lpopCommand(client *Client) {
	if len(client.args) != 2 {
		client.ReplyAritryError()
		return
	}
	l, ok := client.server.GetKeyList(client.args[1], false)
	if !ok {
		client.ReplyTypeError()
		return
	}
	if l == nil {
		client.ReplyNull()
	} else if l.Len() > 0 {
		el := l.Front()
		l.Remove(el)
		if l.Len() == 0 {
			client.server.DelKey(client.args[1])
		}
		client.ReplyBulk(el.Value.(string))
		client.dirty++
	} else {
		client.ReplyNull()
	}
}

func rpopCommand(client *Client) {
	if len(client.args) != 2 {
		client.ReplyAritryError()
		return
	}
	l, ok := client.server.GetKeyList(client.args[1], false)
	if !ok {
		client.ReplyTypeError()
		return
	}
	if l == nil {
		client.ReplyNull()
	} else if l.Len() > 0 {
		el := l.Back()
		l.Remove(el)
		if l.Len() == 0 {
			client.server.DelKey(client.args[1])
		}
		client.ReplyBulk(el.Value.(string))
		client.dirty++
	} else {
		client.ReplyNull()
	}
}

func lindexCommand(client *Client) {
	if len(client.args) != 3 {
		client.ReplyAritryError()
		return
	}
	sn, err := strconv.ParseInt(client.args[2], 10, 64)
	if err != nil {
		client.ReplyInvalidIntError()
		return
	}

	l, ok := client.server.GetKeyList(client.args[1], false)
	if !ok {
		client.ReplyTypeError()
		return
	}
	if l == nil {
		client.ReplyNull()
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
		client.ReplyNull()
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
				client.ReplyBulk(el.Value.(string))
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
				client.ReplyBulk(el.Value.(string))
				return
			}
			el = el.Next()
			i++
		}
	}
	client.ReplyNull()
}

func lremCommand(client *Client) {
	if len(client.args) != 4 {
		client.ReplyAritryError()
		return
	}
	n, err := strconv.ParseInt(client.args[2], 10, 64)
	if err != nil {
		client.ReplyInvalidIntError()
		return
	}
	l, ok := client.server.GetKeyList(client.args[1], false)
	if !ok {
		client.ReplyTypeError()
		return
	}
	if l == nil {
		client.ReplyInt(0)
		return
	}
	count := 0
	v := client.args[3]
	if n == 0 {
		el := l.Front()
		for el != nil {
			next := el.Next()
			if el.Value.(string) == v {
				l.Remove(el)
				count++
				client.dirty++
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
				client.dirty++
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
				client.dirty++
				n--
			}
			el = next
		}
	}
	client.ReplyInt(count)
}

func lsetCommand(client *Client) {
	if len(client.args) != 4 {
		client.ReplyAritryError()
		return
	}
	sn, err := strconv.ParseInt(client.args[2], 10, 64)
	if err != nil {
		client.ReplyInvalidIntError()
		return
	}
	l, ok := client.server.GetKeyList(client.args[1], false)
	if !ok {
		client.ReplyTypeError()
		return
	}
	if l == nil {
		client.ReplyNoSuchKeyError()
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
		client.ReplyError("index out of range")
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
				el.Value = scopy(client.args[3])
				client.ReplyString("OK")
				client.dirty++
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
				el.Value = scopy(client.args[3])
				client.ReplyString("OK")
				client.dirty++
				return
			}
			el = el.Next()
			i++
		}
	}
	client.ReplyError("index out of range")
}

func ltrimCommand(client *Client) {
	if len(client.args) != 4 {
		client.ReplyAritryError()
		return
	}
	sn, err := strconv.ParseInt(client.args[2], 10, 64)
	if err != nil {
		client.ReplyInvalidIntError()
		return
	}
	en, err := strconv.ParseInt(client.args[3], 10, 64)
	if err != nil {
		client.ReplyInvalidIntError()
		return
	}

	l, ok := client.server.GetKeyList(client.args[1], false)
	if !ok {
		client.ReplyTypeError()
		return
	}
	if l == nil {
		client.ReplyString("OK")
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
		client.dirty++
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
		client.dirty++
		el = prev
		i--
	}
	client.ReplyString("OK")
}
