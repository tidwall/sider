package server

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
	case string:
		client.ReplyBulk(s)
	}
}

func setCommand(client *Client) {
	if len(client.args) != 3 {
		client.ReplyAritryError()
		return
	}
	client.server.SetKey(client.args[1], client.args[2])
	client.ReplyString("OK")
}

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
