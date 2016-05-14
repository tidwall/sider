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
		l.PushFront(client.args[i])
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
		l.PushBack(client.args[i])
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
