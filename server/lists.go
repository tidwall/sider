package server

import "container/list"

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
		l.PushBack(client.args[i])
	}
	client.ReplyInt(l.Len())
	client.dirty++
}
