package server

import (
	"strings"
	"time"
)

func getCommand(client *Client) {
	if len(client.args) != 2 {
		client.ReplyAritryError()
		return
	}
	key, ok := client.server.GetKey(client.args[1])
	if !ok {
		client.ReplyNull()
		return
	}
	switch s := key.(type) {
	default:
		client.ReplyTypeError()
		return
	case string:
		client.ReplyBulk(s)
	}
}

func getsetCommand(client *Client) {
	if len(client.args) != 3 {
		client.ReplyAritryError()
		return
	}
	var res string
	key, ok := client.server.GetKey(client.args[1])
	if ok {
		switch s := key.(type) {
		default:
			client.ReplyTypeError()
			return
		case string:
			res = s
		}
	}
	client.server.SetKey(client.args[1], client.args[2])
	if !ok {
		client.ReplyNull()
	} else {
		client.ReplyBulk(res)
	}
	client.dirty++
}

func incrCommand(client *Client) {
	if len(client.args) != 2 {
		client.ReplyAritryError()
		return
	}
	genericIncrbyCommand(client, 1)
}

func incrbyCommand(client *Client) {
	if len(client.args) != 3 {
		client.ReplyAritryError()
		return
	}
	n, err := atoi(client.args[2])
	if err != nil {
		client.ReplyError("value is not an integer or out of range")
		return
	}
	genericIncrbyCommand(client, n)
}

func decrCommand(client *Client) {
	if len(client.args) != 2 {
		client.ReplyAritryError()
		return
	}
	genericIncrbyCommand(client, -1)
}

func decrbyCommand(client *Client) {
	if len(client.args) != 3 {
		client.ReplyAritryError()
		return
	}
	n, err := atoi(client.args[2])
	if err != nil {
		client.ReplyError("value is not an integer or out of range")
		return
	}
	genericIncrbyCommand(client, -n)
}

func genericIncrbyCommand(client *Client, delta int) {
	var n int
	key, ok := client.server.GetKey(client.args[1])
	if ok {
		switch s := key.(type) {
		default:
			client.ReplyTypeError()
			return
		case string:
			var err error
			n, err = atoi(s)
			if err != nil {
				client.ReplyTypeError()
				return
			}
			n += int(delta)
		}
	} else {
		n = 1
	}
	client.server.UpdateKey(client.args[1], itoa(n))
	client.ReplyInt(int(n))
	client.dirty++
}

func setCommand(client *Client) {
	if len(client.args) < 3 {
		client.ReplyAritryError()
		return
	}
	var nx, xx bool
	var ex, px time.Time
	var expires bool
	var when time.Time
	for i := 3; i < len(client.args); i++ {
		switch strings.ToLower(client.args[i]) {
		case "nx":
			if xx {
				client.ReplySyntaxError()
				return
			}
			nx = true
		case "xx":
			if nx {
				client.ReplySyntaxError()
				return
			}
			xx = true
		case "ex":
			if !px.IsZero() || i == len(client.args)-1 {
				client.ReplySyntaxError()
				return
			}
			i++
			n, err := atoi(client.args[i])
			if err != nil {
				client.ReplySyntaxError()
				return
			}
			ex = time.Now().Add(time.Duration(n) * time.Second)
			expires = true
			when = ex
		case "px":
			if !ex.IsZero() || i == len(client.args)-1 {
				client.ReplySyntaxError()
				return
			}
			i++
			n, err := atoi(client.args[i])
			if err != nil {
				client.ReplySyntaxError()
				return
			}
			px = time.Now().Add(time.Duration(n) * time.Millisecond)
			expires = true
			when = px
		}
	}
	if nx || xx {
		_, ok := client.server.GetKey(client.args[1])
		if (ok && nx) || (!ok && xx) {
			client.ReplyNull()
			return
		}
	}
	client.server.SetKey(client.args[1], client.args[2])
	if expires {
		client.server.Expire(client.args[1], when)
	}
	client.ReplyString("OK")
	client.dirty++
}

func setnxCommand(client *Client) {
	if len(client.args) != 3 {
		client.ReplyAritryError()
		return
	}
	_, ok := client.server.GetKey(client.args[1])
	if ok {
		client.ReplyInt(0)
		return
	}
	client.server.SetKey(client.args[1], client.args[2])
	client.ReplyInt(1)
	client.dirty++
}

func msetCommand(client *Client) {
	if len(client.args) < 3 || (len(client.args)-1)%2 != 0 {
		client.ReplyAritryError()
		return
	}
	for i := 1; i < len(client.args); i += 2 {
		client.server.SetKey(client.args[i+0], client.args[i+1])
		client.dirty++
	}
	client.ReplyString("OK")
}

func msetnxCommand(client *Client) {
	if len(client.args) < 3 || (len(client.args)-1)%2 != 0 {
		client.ReplyAritryError()
		return
	}
	for i := 1; i < len(client.args); i += 2 {
		_, ok := client.server.GetKey(client.args[1])
		if ok {
			client.ReplyInt(0)
			return
		}
	}
	for i := 1; i < len(client.args); i += 2 {
		client.server.SetKey(client.args[i+0], client.args[i+1])
		client.dirty++
	}
	client.ReplyInt(1)
}

func appendCommand(client *Client) {
	if len(client.args) != 3 {
		client.ReplyAritryError()
		return
	}
	key, ok := client.server.GetKey(client.args[1])
	if !ok {
		client.server.SetKey(client.args[1], client.args[2])
		client.ReplyInt(len(client.args[2]))
		client.dirty++
		return
	}
	switch s := key.(type) {
	default:
		client.ReplyTypeError()
		return
	case string:
		s += client.args[2]
		client.server.SetKey(client.args[1], s)
		client.ReplyInt(len(s))
		client.dirty++
	}
}

func bitcountCommand(client *Client) {
	var start, end int
	var all bool
	switch len(client.args) {
	default:
		client.ReplyAritryError()
	case 2:
		all = true
	case 4:
		n1, err1 := atoi(client.args[2])
		n2, err2 := atoi(client.args[3])
		if err1 != nil || err2 != nil {
			client.ReplyError("value is not an integer or out of range")
			return
		}
		start, end = int(n1), int(n2)
	}
	key, ok := client.server.GetKey(client.args[1])
	if !ok {
		client.ReplyInt(0)
		return
	}
	switch s := key.(type) {
	default:
		client.ReplyTypeError()
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
		client.ReplyInt(count)
	}
}

func mgetCommand(client *Client) {
	if len(client.args) < 2 {
		client.ReplyAritryError()
		return
	}
	client.ReplyMultiBulkLen(len(client.args) - 1)
	for i := 1; i < len(client.args); i++ {
		key, ok := client.server.GetKey(client.args[i])
		if !ok {
			client.ReplyNull()
		} else if s, ok := key.(string); ok {
			client.ReplyBulk(s)
		} else {
			client.ReplyNull()
		}
	}
}
