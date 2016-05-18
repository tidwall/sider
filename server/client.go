package server

import (
	"io"
	"strconv"
)

type client struct {
	wr      io.Writer // client writer
	s       *Server   // shared server
	db      *database // the active database
	args    []string  // command arguments
	raw     []byte    // the raw command bytes
	dirty   int       // the number of changes made by the client
	monitor bool      // the client is in monitor mode
	errd    bool      // flag that indicates that the last command was an error
}

// flushAOF checks if the the client has any dirty markers and
// if so calls server.flushAOF
func (c *client) flushAOF() error {
	if c.dirty > 0 {
		c.s.mu.Lock()
		defer c.s.mu.Unlock()
		if err := c.s.flushAOF(); err != nil {
			c.s.fatalError(err)
			return err
		}
		c.dirty = 0
	}
	return nil
}

func (c *client) replyString(s string) {
	io.WriteString(c.wr, "+"+s+"\r\n")
}
func (c *client) replyUniqueError(s string) {
	io.WriteString(c.wr, "-"+s+"\r\n")
	c.errd = true
}
func (c *client) replyBulk(s string) {
	io.WriteString(c.wr, "$"+strconv.FormatInt(int64(len(s)), 10)+"\r\n"+s+"\r\n")
}
func (c *client) replyNull() {
	io.WriteString(c.wr, "$-1\r\n")
}
func (c *client) replyInt(n int) {
	io.WriteString(c.wr, ":"+strconv.FormatInt(int64(n), 10)+"\r\n")
}
func (c *client) replyMultiBulkLen(n int) {
	io.WriteString(c.wr, "*"+strconv.FormatInt(int64(n), 10)+"\r\n")
}
func (c *client) replyError(s string) {
	c.replyUniqueError("ERR " + s)
}
func (c *client) replyAritryError() {
	c.replyError("wrong number of arguments for '" + c.args[0] + "'")
}
func (c *client) replyTypeError() {
	c.replyUniqueError("WRONGTYPE Operation against a key holding the wrong kind of value")
}
func (c *client) replySyntaxError() {
	c.replyError("syntax error")
}
func (c *client) replyInvalidIntError() {
	c.replyError("value is not an integer or out of range")
}
func (c *client) replyNoSuchKeyError() {
	c.replyError("no such key")
}
