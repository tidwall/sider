package server

func echoCommand(client *Client) {
	if len(client.args) != 2 {
		client.ReplyAritryError()
		return
	}
	client.ReplyBulk(client.args[1])
}

func pingCommand(client *Client) {
	switch len(client.args) {
	default:
		client.ReplyAritryError()
	case 1:
		client.ReplyString("PONG")
	case 2:
		client.ReplyBulk(client.args[1])
	}
}
