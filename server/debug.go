package server

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"syscall"
)

func replyArgsError(c *client) {
	c.replyError("Unknown DEBUG subcommand or wrong number of arguments for '" + c.args[1] + "'")
}

func debugCommand(c *client) {
	if len(c.args) == 1 {
		c.replyError("You must specify a subcommand for DEBUG. Try DEBUG HELP for info.")
		return
	}
	switch strings.ToLower(c.args[1]) {
	default:
		replyArgsError(c)
		return
	case "help":
		msgs := []string{
			"DEBUG <subcommand> arg arg ... arg. Subcommands:",
			"segfault -- Crash the server with sigsegv.",
			"object <key> -- Show low level info about key and associated value.",
			"gc -- Force a garbage collection.",
		}
		c.replyMultiBulkLen(len(msgs))
		for _, msg := range msgs {
			c.replyBulk(msg)
		}
	case "segfault":
		syscall.Kill(os.Getpid(), syscall.SIGSEGV)
	case "object":
		debugObjectCommand(c)
	case "gc":
		runtime.GC()
		c.replyString("OK")
	}
}

func debugObjectCommand(c *client) {
	if len(c.args) != 3 {
		replyArgsError(c)
		return
	}
	typ := c.db.getType(c.args[2])
	_, ok := c.db.get(c.args[2])
	if !ok {
		c.replyError("no such key")
		return
	}
	res := fmt.Sprintf("Value at:0x0 refcount:1 encoding:%s", typ)
	c.replyString(res)
}
