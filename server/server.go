package server

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

func (s *Server) commandTable() {
	// "+" append aof
	// "w" write lock
	// "r" read lock
	s.register("get", getCommand, "r")           // Strings
	s.register("getset", getsetCommand, "w+")    // Strings
	s.register("set", setCommand, "w+")          // Strings
	s.register("append", appendCommand, "w+")    // Strings
	s.register("bitcount", bitcountCommand, "r") // Strings
	s.register("incr", incrCommand, "w+")        // Strings
	s.register("incrby", incrbyCommand, "w+")    // Strings
	s.register("decr", decrCommand, "w+")        // Strings
	s.register("decrby", decrbyCommand, "w+")    // Strings
	s.register("mget", mgetCommand, "r")         // Strings
	s.register("setnx", setnxCommand, "w+")      // Strings
	s.register("mset", msetCommand, "w+")        // Strings
	s.register("msetnx", msetnxCommand, "w+")    // Strings

	s.register("lpush", lpushCommand, "w+")  // Lists
	s.register("rpush", rpushCommand, "w+")  // Lists
	s.register("lrange", lrangeCommand, "r") // Lists
	s.register("llen", llenCommand, "r")     // Lists
	s.register("lpop", lpopCommand, "w+")    // Lists
	s.register("rpop", rpopCommand, "w+")    // Lists
	s.register("lindex", lindexCommand, "r") // Lists
	s.register("lrem", lremCommand, "w+")    // Lists
	s.register("lset", lsetCommand, "w+")    // Lists
	s.register("ltrim", ltrimCommand, "w+")  // Lists

	s.register("sadd", saddCommand, "w+")               // Sets
	s.register("scard", scardCommand, "r")              // Sets
	s.register("smembers", smembersCommand, "r")        // Sets
	s.register("sismember", sismembersCommand, "r")     // Sets
	s.register("sdiff", sdiffCommand, "r")              // Sets
	s.register("sinter", sinterCommand, "r")            // Sets
	s.register("sunion", sunionCommand, "r")            // Sets
	s.register("sdiffstore", sdiffstoreCommand, "w+")   // Sets
	s.register("sinterstore", sinterstoreCommand, "w+") // Sets
	s.register("sunionstore", sunionstoreCommand, "w+") // Sets
	s.register("spop", spopCommand, "w+")               // Sets
	s.register("srandmember", srandmemberCommand, "r")  // Sets
	s.register("srem", sremCommand, "w+")               // Sets
	s.register("smove", smoveCommand, "w+")             // Sets

	s.register("echo", echoCommand, "")      // Connection
	s.register("ping", pingCommand, "")      // Connection
	s.register("select", selectCommand, "w") // Connection

	s.register("flushdb", flushdbCommand, "w+")   // Server
	s.register("flushall", flushallCommand, "w+") // Server
	s.register("dbsize", dbsizeCommand, "r")      // Server
	s.register("debug", debugCommand, "w")        // Server

	s.register("del", delCommand, "w+")            // Keys
	s.register("keys", keysCommand, "r")           // Keys
	s.register("rename", renameCommand, "w+")      // Keys
	s.register("renamenx", renamenxCommand, "w+")  // Keys
	s.register("type", typeCommand, "r")           // Keys
	s.register("randomkey", randomkeyCommand, "r") // Keys
	s.register("exists", existsCommand, "r")       // Keys
	s.register("expire", expireCommand, "w+")      // Keys
	s.register("ttl", ttlCommand, "r")             // Keys
	s.register("move", moveCommand, "w+")          // Keys
	s.register("sort", sortCommand, "w+")          // Keys

}

type command struct {
	name  string
	aof   bool
	read  bool
	write bool
	funct func(c *client)
}

// Options alter the behavior of the server.
type Options struct {
	LogWriter        io.Writer
	IgnoreLogDebug   bool
	IgnoreLogVerbose bool
	IgnoreLogNotice  bool
	IgnoreLogWarning bool
}

// Server represents a server object.
type Server struct {
	mu       sync.RWMutex
	l        net.Listener
	options  *Options
	cmds     map[string]*command
	dbs      map[int]*database
	follower bool

	expires     map[string]time.Time
	expiresdone bool

	aof       *os.File // the aof file handle
	aofdbnum  int      // the db num of the last "select" written to the aof
	aofclosed bool     // flag for when the aof file is closed

	ferr     error      // a fatal error. setting this should happen in the fatalError function
	ferrcond *sync.Cond // synchronize the watch
	ferrdone bool       // flag for when the fatal error watch is complete
}

// register is called from the commandTable() function. The command map will contains
// two entries assigned to the same command. One with an all uppercase key and one with
// an all lower case key.
func (s *Server) register(commandName string, f func(c *client), opts string) {
	var cmd command
	cmd.name = commandName
	cmd.funct = f
	for _, c := range []byte(opts) {
		switch c {
		case '+':
			cmd.aof = true
		case 'r':
			cmd.read = true
		case 'w':
			cmd.write = true
		}
	}
	s.cmds[strings.ToLower(commandName)] = &cmd
	s.cmds[strings.ToUpper(commandName)] = &cmd
}

// The log format is described at http://build47.com/redis-log-format-levels/
func (s *Server) lf(c byte, format string, args ...interface{}) {
	fmt.Fprintf(
		s.options.LogWriter,
		"%d:M %s %c %s\n",
		os.Getpid(),
		time.Now().Format("2 Jan 15:04:05.000"),
		c,
		fmt.Sprintf(format, args...),
	)
}
func (s *Server) ldebugf(format string, args ...interface{}) {
	if !s.options.IgnoreLogDebug {
		s.lf('.', format, args...)
	}
}
func (s *Server) lverbosf(format string, args ...interface{}) {
	if !s.options.IgnoreLogVerbose {
		s.lf('-', format, args...)
	}
}
func (s *Server) lnoticef(format string, args ...interface{}) {
	if !s.options.IgnoreLogNotice {
		s.lf('*', format, args...)
	}
}
func (s *Server) lwarningf(format string, args ...interface{}) {
	if !s.options.IgnoreLogWarning {
		s.lf('#', format, args...)
	}
}

func (s *Server) fatalError(err error) {
	s.ferrcond.L.Lock()
	if s.ferr == nil {
		s.ferr = err
	}
	s.ferrcond.Broadcast()
	s.ferrcond.L.Unlock()
}

//debug, verbose, notice, and warning.
// // startExpireLoop runs a background routine which watches for exipred keys
// // and forces their removal from the database. 100ms resolution.
// func (s *Server) startExpireLoop() {
// 	go func() {
// 		t := time.NewTicker(time.Second / 10)
// 		defer t.Stop()
// 		for range t.C {
// 			s.mu.Lock()
// 			if s.expiresdone {
// 				s.mu.Unlock()
// 				return
// 			}
// 			s.forceDeleteExpires()
// 			s.mu.Unlock()
// 		}
// 	}()
// }

// func (s *Server) forceDeleteExpires() {
// 	if len(s.expires) == 0 || s.follower {
// 		return
// 	}
// 	now := time.Now()
// 	var aofbuf bytes.Buffer
// 	for key, expires := range s.expires {
// 		if now.After(expires) {
// 			delete(s.keys, key)
// 			aofbuf.WriteString("*2\r\n$3\r\ndel\r\n$")
// 			aofbuf.WriteString(strconv.FormatInt(int64(len(key)), 10))
// 			aofbuf.WriteString("\r\n")
// 			aofbuf.WriteString(key)
// 			aofbuf.WriteString("\r\n")
// 			delete(s.expires, key)
// 		}
// 	}
// 	if aofbuf.Len() > 0 {
// 		if _, err := s.aof.Write(aofbuf.Bytes()); err != nil {
// 			panic(err)
// 		}
// 	}
// }

// // stopExpireLoop will force delete all expires and stop the background routine
// func (s *Server) stopExpireLoop() {
// 	s.mu.Lock()
// 	s.forceDeleteExpires()
// 	s.expiresdone = true
// 	s.mu.Unlock()
// }

// startFatalErrorWatch
func (s *Server) startFatalErrorWatch() {
	go func() {
		for {
			s.ferrcond.L.Lock()
			if s.ferrdone {
				s.ferrcond.L.Unlock()
				return
			}
			if s.ferr != nil {
				s.l.Close()
				s.ferrdone = true
				return
			}
			s.ferrcond.Wait()
			s.ferrcond.L.Unlock()
		}
	}()
}

func (s *Server) stopFatalErrorWatch() {
	s.ferrcond.L.Lock()
	s.ferrdone = true
	s.ferrcond.Broadcast()
	s.ferrcond.L.Unlock()
}

func (s *Server) selectDB(num int) *database {
	db, ok := s.dbs[num]
	if !ok {
		db = newDB()
		s.dbs[num] = db
	}
	return db
}

func fillOptions(options *Options) *Options {
	if options == nil {
		options = &Options{}
	}
	if options.LogWriter == nil {
		options.LogWriter = os.Stderr
	}
	return options
}

func Start(addr string, options *Options) (err error) {
	s := &Server{
		cmds:     make(map[string]*command),
		dbs:      make(map[int]*database),
		expires:  make(map[string]time.Time),
		aofdbnum: -1,
		options:  fillOptions(options),
		ferrcond: sync.NewCond(&sync.Mutex{}),
	}
	defer func() {
		if err == nil && s.ferr != nil {
			err = s.ferr
		}
	}()
	s.lwarningf("Server started, Sider version 999.999.9999")
	s.commandTable()
	if err = s.openAOF(); err != nil {
		s.lwarningf("%v", err)
		return err
	}
	defer s.closeAOF()
	defer s.flushAOF()

	// s.startExpireLoop()
	// defer s.stopExpireLoop()

	s.l, err = net.Listen("tcp", addr)
	if err != nil {
		s.lwarningf("%v", err)
		return err
	}
	defer s.l.Close()

	s.lnoticef("The server is now ready to accept connections on port %s", s.l.Addr().String()[strings.LastIndex(s.l.Addr().String(), ":")+1:])

	// Start watching for fatal errors.
	s.startFatalErrorWatch()
	defer s.stopFatalErrorWatch()

	for {
		conn, err := s.l.Accept()
		if err != nil {
			s.lwarningf("%v", err)
			continue
		}
		go handleConn(conn, s)
	}
}

// autocase will return an ascii string in uppercase or lowercase, but never
// mixed case. It's quicker than calling strings.(ToLower/ToUpper).
// The thinking is that commands are usually sent in all upper or
// all lower case, such as 'GET' or 'get'. But, rarely 'Get'.
func autocase(command string) string {
	for i := 0; i < len(command); i++ {
		c := command[i]
		if c >= 'A' && c <= 'Z' {
			i++
			for ; i < len(command); i++ {
				c = command[i]
				if c >= 'a' && c <= 'z' {
					return strings.ToUpper(command)
				}
			}
			break
		} else if c >= 'a' && c <= 'z' {
			i++
			for ; i < len(command); i++ {
				c = command[i]
				if c >= 'A' && c <= 'Z' {
					return strings.ToLower(command)
				}
			}
			break
		}
	}
	return command
}

func handleConn(conn net.Conn, s *Server) {
	defer conn.Close()
	rd := newCommandReader(conn)
	wr := bufio.NewWriter(conn)
	defer wr.Flush()
	c := &client{wr: wr, s: s}
	defer c.flushAOF()
	s.mu.Lock()
	c.db = s.selectDB(0)
	s.mu.Unlock()
	var flush bool
	var err error
	for {
		c.raw, c.args, flush, err = rd.readCommand()
		if err != nil {
			if err, ok := err.(*protocolError); ok {
				c.replyError(err.Error())
			}
			return
		}
		if len(c.args) == 0 {
			continue
		}
		commandName := autocase(c.args[0])
		if cmd, ok := s.cmds[commandName]; ok {
			if cmd.write {
				s.mu.Lock()
			} else if cmd.read {
				s.mu.RLock()
			}
			cmd.funct(c)
			if c.dirty > 0 && cmd.aof {
				c.db.aofbuf.Write(c.raw)
			}
			if cmd.write {
				s.mu.Unlock()
			} else if cmd.read {
				s.mu.RUnlock()
			}
		} else {
			switch commandName {
			default:
				c.replyError("unknown command '" + c.args[0] + "'")
			case "quit":
				c.replyString("OK")
				return
			}
		}
		if flush {
			if err := c.flushAOF(); err != nil {
				return
			}
			if err := wr.Flush(); err != nil {
				return
			}
		}
	}
}

/* Commands */
func flushdbCommand(c *client) {
	if len(c.args) != 1 {
		c.replyAritryError()
		return
	}
	c.db.flush()
	c.replyString("OK")
	c.dirty++
}

func flushallCommand(c *client) {
	if len(c.args) != 1 {
		c.replyAritryError()
		return
	}
	for _, db := range c.s.dbs {
		db.flush()
	}
	c.replyString("OK")
	c.dirty++
}

func dbsizeCommand(c *client) {
	if len(c.args) != 1 {
		c.replyAritryError()
		return
	}
	c.replyInt(c.db.len())
}
