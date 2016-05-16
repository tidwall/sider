package server

import (
	"io"
	"strconv"
)

type client struct {
	wr     io.Writer // client writer
	server *Server   // shared server
	db     *dbT      // the active database
	args   []string  // command arguments
	raw    []byte    // the raw command bytes
	dirty  int       // the number of changes made by the client
}

// flushAOF checks if the the client has any dirty markers and
// if so calls server.flushAOF
func (c *client) flushAOF() error {
	if c.dirty > 0 {
		c.server.mu.Lock()
		defer c.server.mu.Unlock()
		if err := c.server.flushAOF(); err != nil {
			c.server.fatalError(err)
			return err
		}
		c.dirty = 0
	}
	return nil
}

func (c *client) ReplyString(s string) {
	io.WriteString(c.wr, "+"+s+"\r\n")
}
func (c *client) ReplyError(s string) {
	io.WriteString(c.wr, "-ERR "+s+"\r\n")
}
func (c *client) ReplyAritryError() {
	io.WriteString(c.wr, "-ERR wrong number of arguments for '"+c.args[0]+"'\r\n")
}
func (c *client) ReplyTypeError() {
	io.WriteString(c.wr, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
}
func (c *client) ReplyBulk(s string) {
	io.WriteString(c.wr, "$"+strconv.FormatInt(int64(len(s)), 10)+"\r\n"+s+"\r\n")
}
func (c *client) ReplyNull() {
	io.WriteString(c.wr, "$-1\r\n")
}
func (c *client) ReplyInt(n int) {
	io.WriteString(c.wr, ":"+strconv.FormatInt(int64(n), 10)+"\r\n")
}
func (c *client) ReplyMultiBulkLen(n int) {
	io.WriteString(c.wr, "*"+strconv.FormatInt(int64(n), 10)+"\r\n")
}
func (c *client) ReplySyntaxError() {
	io.WriteString(c.wr, "-ERR syntax error\r\n")
}
func (c *client) ReplyInvalidIntError() {
	io.WriteString(c.wr, "-ERR value is not an integer or out of range\r\n")
}
func (c *client) ReplyNoSuchKeyError() {
	io.WriteString(c.wr, "-ERR no such key\r\n")
}
