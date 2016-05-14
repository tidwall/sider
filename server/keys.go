package server

import "github.com/google/btree"

func delCommand(client *Client) {
	if len(client.args) < 2 {
		client.ReplyAritryError()
		return
	}
	count := 0
	for i := 1; i < len(client.args); i++ {
		if _, ok := client.server.DelKey(client.args[i]); ok {
			count++
		}
	}
	client.ReplyInt(count)
}

func keysCommand(client *Client) {
	if len(client.args) != 2 {
		client.ReplyAritryError()
		return
	}
	var keys []string
	pattern := parsePattern(client.args[1])
	if pattern.All {
		client.server.keys.Ascend(
			func(item btree.Item) bool {
				keys = append(keys, item.(*Key).Name)
				return true
			},
		)
	} else if !pattern.Glob {
		item := client.server.keys.Get(&Key{Name: pattern.Value})
		if item != nil {
			keys = append(keys, item.(*Key).Name)
		}
	} else if pattern.GreaterOrEqual != "" {
		client.server.keys.AscendRange(
			&Key{Name: pattern.GreaterOrEqual},
			&Key{Name: pattern.LessThan},
			func(item btree.Item) bool {
				if pattern.Match(item.(*Key).Name) {
					keys = append(keys, item.(*Key).Name)
				}
				return true
			},
		)
	} else {
		client.server.keys.Ascend(
			func(item btree.Item) bool {
				if pattern.Match(item.(*Key).Name) {
					keys = append(keys, item.(*Key).Name)
				}
				return true
			},
		)
	}
	client.ReplyMultiBulkLen(len(keys))
	for _, key := range keys {
		client.ReplyBulk(key)
	}
}
