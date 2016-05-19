package server

import (
	"io"
	"strconv"
	"strings"
)

type client struct {
	wr      io.Writer // client writer
	s       *Server   // shared server
	db      *database // the active database
	args    []string  // command arguments
	raw     []byte    // the raw command bytes
	addr    string    // the address of the client
	dirty   int       // the number of changes made by the client
	monitor bool      // the client is in monitor mode
	errd    bool      // flag that indicates that the last command was an error
	authd   int       // 0 = no auth checked, 1 = protected checked, 2 = pass checked

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

func (c *client) authenticate(cmd *command) bool {
	if c.authd == 2 {
		return true
	}
	c.s.mu.RLock()
	defer c.s.mu.RUnlock()
	if c.authd == 0 {
		if c.s.protected() {
			if !strings.HasPrefix(c.addr, "127.0.0.1:") && !strings.HasPrefix(c.addr, "[::1]:") {
				c.replyProtectedError()
				return false
			}
		}
		c.authd = 1
	}
	if c.s.cfg.requirepass == "" {
		return true
	}
	if cmd.name != "auth" {
		c.replyNoAuthError()
		return false
	}
	cmd.funct(c)
	return false
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
func (c *client) replyNoAuthError() {
	c.replyUniqueError("NOAUTH Authentication required.")
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

func (c *client) replyProtectedError() {
	c.replyUniqueError(`` +
		`DENIED ` + c.s.options.AppName + ` is running in protected ` +
		`mode because protected mode is enabled, no bind address was ` +
		`specified, no authentication password is requested to clients. ` +
		`In this mode connections are only accepted from the loopback ` +
		`interface. If you want to connect from external computers to ` +
		c.s.options.AppName + ` you may adopt one of the following ` +
		`solutions: 1) Just disable protected mode sending the command ` +
		`'CONFIG SET protected-mode no' from the loopback interface by ` +
		`connecting to ` + c.s.options.AppName + ` from the same host ` +
		`the server is running, however MAKE SURE ` + c.s.options.AppName +
		` is not publicly accessible from internet if you do so. Use ` +
		`CONFIG REWRITE to make this change permanent. 2) Alternatively ` +
		`you can just disable the protected mode by editing the Redis ` +
		`configuration file, and setting the protected mode option to ` +
		`'no', and then restarting the server. 3) If you started the ` +
		`server manually just for testing, restart it with the ` +
		`'--protected-mode no' option. 4) Setup a bind address or an ` +
		`authentication password. NOTE: You only need to do one of the ` +
		`above things in order for the server to start accepting ` +
		`connections from the outside.`)
}
