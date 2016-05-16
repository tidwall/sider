package server

import "strconv"

func echoCommand(c *client) {
	if len(c.args) != 2 {
		c.replyAritryError()
		return
	}
	c.replyBulk(c.args[1])
}

func pingCommand(c *client) {
	switch len(c.args) {
	default:
		c.replyAritryError()
	case 1:
		c.replyString("PONG")
	case 2:
		c.replyBulk(c.args[1])
	}
}

func selectCommand(c *client) {
	if len(c.args) != 2 {
		c.replyAritryError()
		return
	}
	num, err := strconv.ParseUint(c.args[1], 10, 32)
	if err != nil {
		c.replyError("invalid DB index")
		return
	}
	c.db = c.s.selectDB(int(num))
	c.replyString("OK")
}
