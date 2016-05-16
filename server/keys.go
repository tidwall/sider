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
		if _, ok := client.server.db.Del(client.args[i]); ok {
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
	key, ok := client.server.db.Get(client.args[1])
	if !ok {
		client.ReplyError("no such key")
		return
	}
	client.server.db.Del(client.args[1])
	client.server.db.Set(client.args[2], key)
	client.ReplyString("OK")
	client.dirty++
}

func renamenxCommand(client *Client) {
	if len(client.args) != 3 {
		client.ReplyAritryError()
		return
	}
	key, ok := client.server.db.Get(client.args[1])
	if !ok {
		client.ReplyError("no such key")
		return
	}
	_, ok = client.server.db.Get(client.args[2])
	if ok {
		client.ReplyInt(0)
		return
	}
	client.server.db.Del(client.args[1])
	client.server.db.Set(client.args[2], key)
	client.ReplyInt(1)
	client.dirty++
}

func keysCommand(client *Client) {
	if len(client.args) != 2 {
		client.ReplyAritryError()
		return
	}
	client.ReplyMultiBulkLen(client.server.db.Len())
	client.server.db.Ascend(func(key string, value interface{}) bool {
		client.ReplyBulk(key)
		return true
	})

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
	key, ok := client.server.db.Get(client.args[1])
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
	got := false
	client.server.db.Ascend(func(key string, value interface{}) bool {
		client.ReplyBulk(key)
		got = true
		return false
	})
	if !got {
		client.ReplyNull()
	}
}

func existsCommand(client *Client) {
	if len(client.args) == 1 {
		client.ReplyAritryError()
		return
	}
	var count int
	for i := 1; i < len(client.args); i++ {
		if _, ok := client.server.db.Get(client.args[i]); ok {
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
	if client.server.db.Expire(client.args[1], time.Now().Add(time.Duration(seconds)*time.Second)) {
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
	_, expires, ok := client.server.db.GetExpires(client.args[1])
	if !ok {
		client.ReplyInt(-2)
	} else if expires.IsZero() {
		client.ReplyInt(-1)
	} else {
		client.ReplyInt(int(expires.Sub(time.Now()) / time.Second))
	}
}
