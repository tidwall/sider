package server

import "strconv"

func echoCommand(c *client) {
	if len(c.args) != 2 {
		c.ReplyAritryError()
		return
	}
	c.ReplyBulk(c.args[1])
}

func pingCommand(c *client) {
	switch len(c.args) {
	default:
		c.ReplyAritryError()
	case 1:
		c.ReplyString("PONG")
	case 2:
		c.ReplyBulk(c.args[1])
	}
}

func selectCommand(c *client) {
	if len(c.args) != 2 {
		c.ReplyAritryError()
		return
	}
	num, err := strconv.ParseUint(c.args[1], 10, 32)
	if err != nil {
		c.ReplyError("invalid DB index")
		return
	}
	c.db = c.server.selectDB(int(num))
	c.ReplyString("OK")
}
