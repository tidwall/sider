package server

import (
	"strconv"
	"time"
)

func delCommand(client *Client) {
	if len(client.args) < 2 {
		client.ReplyAritryError()
		return
	}
	count := 0
	for i := 1; i < len(client.args); i++ {
		if _, ok := client.server.DelKey(client.args[i]); ok {
			count++
			client.dirty++
		}
	}
	client.ReplyInt(count)

}

func renameCommand(client *Client) {
	if len(client.args) != 3 {
		client.ReplyAritryError()
		return
	}
	key, ok := client.server.GetKey(client.args[1])
	if !ok {
		client.ReplyError("no such key")
		return
	}
	client.server.DelKey(client.args[1])
	client.server.SetKey(client.args[2], key)
	client.ReplyString("OK")
	client.dirty++
}

func renamenxCommand(client *Client) {
	if len(client.args) != 3 {
		client.ReplyAritryError()
		return
	}
	key, ok := client.server.GetKey(client.args[1])
	if !ok {
		client.ReplyError("no such key")
		return
	}
	_, ok = client.server.GetKey(client.args[2])
	if ok {
		client.ReplyInt(0)
		return
	}
	client.server.DelKey(client.args[1])
	client.server.SetKey(client.args[2], key)
	client.ReplyInt(1)
	client.dirty++
}

func keysCommand(client *Client) {
	if len(client.args) != 2 {
		client.ReplyAritryError()
		return
	}
	client.ReplyMultiBulkLen(len(client.server.keys))
	for name := range client.server.keys {
		client.ReplyBulk(name)
	}

	// var keys []string
	// pattern := parsePattern(client.args[1])
	// if pattern.All {
	// 	for name := range client.server.keys {
	// 		keys = append(keys, name)
	// 	}
	// 	// client.server.keys.Ascend(
	// 	// 	func(item btree.Item) bool {
	// 	// 		keys = append(keys, item.(*Key).Name)
	// 	// 		return true
	// 	// 	},
	// 	// )
	// } else if !pattern.Glob {
	// 	// item := client.server.keys.Get(&Key{Name: pattern.Value})
	// 	// if item != nil {
	// 	// 	keys = append(keys, item.(*Key).Name)
	// 	// }
	// } else if pattern.GreaterOrEqual != "" {
	// 	// client.server.keys.AscendRange(
	// 	// 	&Key{Name: pattern.GreaterOrEqual},
	// 	// 	&Key{Name: pattern.LessThan},
	// 	// 	func(item btree.Item) bool {
	// 	// 		if pattern.Match(item.(*Key).Name) {
	// 	// 			keys = append(keys, item.(*Key).Name)
	// 	// 		}
	// 	// 		return true
	// 	// 	},
	// 	// )
	// } else {
	// 	// client.server.keys.Ascend(
	// 	// 	func(item btree.Item) bool {
	// 	// 		if pattern.Match(item.(*Key).Name) {
	// 	// 			keys = append(keys, item.(*Key).Name)
	// 	// 		}
	// 	// 		return true
	// 	// 	},
	// 	// )
	// }
	// client.ReplyMultiBulkLen(len(keys))
	// for _, key := range keys {
	// 	client.ReplyBulk(key)
	// }
}

func typeCommand(client *Client) {
	if len(client.args) != 2 {
		client.ReplyAritryError()
		return
	}
	key, ok := client.server.GetKey(client.args[1])
	if !ok {
		client.ReplyString("none")
		return
	}
	switch key.(type) {
	default:
		client.ReplyString("unknown") // should not be reached
	case string:
		client.ReplyString("string")
	}
}

func randomkeyCommand(client *Client) {
	if len(client.args) != 1 {
		client.ReplyAritryError()
		return
	}
	for name := range client.server.keys {
		client.ReplyBulk(name)
	}
	client.ReplyNull()
}

func existsCommand(client *Client) {
	if len(client.args) == 1 {
		client.ReplyAritryError()
		return
	}
	var count int
	for i := 1; i < len(client.args); i++ {
		if _, ok := client.server.GetKey(client.args[i]); ok {
			count++
		}
	}
	client.ReplyInt(count)
}
func expireCommand(client *Client) {
	if len(client.args) != 3 {
		client.ReplyAritryError()
		return
	}
	seconds, err := strconv.ParseInt(client.args[2], 10, 64)
	if err != nil {
		client.ReplyError("value is not an integer or out of range")
		return
	}
	if client.server.Expire(client.args[1], time.Now().Add(time.Duration(seconds)*time.Second)) {
		client.ReplyInt(1)
		client.dirty++
	} else {
		client.ReplyInt(0)
	}
}
func ttlCommand(client *Client) {
	if len(client.args) != 2 {
		client.ReplyAritryError()
		return
	}
	_, expires, ok := client.server.GetKeyExpires(client.args[1])
	if !ok {
		client.ReplyInt(-2)
	} else if expires.IsZero() {
		client.ReplyInt(-1)
	} else {
		client.ReplyInt(int(expires.Sub(time.Now()) / time.Second))
	}
}
