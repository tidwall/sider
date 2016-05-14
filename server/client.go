package server

import (
	"io"
	"strconv"
)

type Client struct {
	wr     io.Writer // client writer
	server *Server   // shared server
	args   []string  // command arguments
	raw    []byte    // the raw command bytes
}

func (c *Client) ReplyString(s string) {
	io.WriteString(c.wr, "+"+s+"\r\n")
}
func (c *Client) ReplyError(s string) {
	io.WriteString(c.wr, "-ERR "+s+"\r\n")
}
func (c *Client) ReplyAritryError() {
	io.WriteString(c.wr, "-ERR wrong number of arguments for '"+c.args[0]+"'\r\n")
}
func (c *Client) ReplyTypeError() {
	io.WriteString(c.wr, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
}
func (c *Client) ReplyBulk(s string) {
	io.WriteString(c.wr, "$"+strconv.FormatInt(int64(len(s)), 10)+"\r\n"+s+"\r\n")
}
func (c *Client) ReplyNull() {
	io.WriteString(c.wr, "$-1\r\n")
}
func (c *Client) ReplyInt(n int) {
	io.WriteString(c.wr, ":"+strconv.FormatInt(int64(n), 10)+"\r\n")
}
func (c *Client) ReplyMultiBulkLen(n int) {
	io.WriteString(c.wr, "*"+strconv.FormatInt(int64(n), 10)+"\r\n")
}
