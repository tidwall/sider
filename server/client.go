package server

import "io"

type Client struct {
	wr     io.Writer
	server *Server
	args   []string
}

func (c *Client) ReplyString(s string) {
	io.WriteString(c.wr, "+"+s+"\r\n")
}

func (c *Client) ReplyError(s string) {
	io.WriteString(c.wr, "-ERR "+s+"\r\n")
}
