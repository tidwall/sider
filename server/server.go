package server

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"strconv"
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

	s.register("lpush", lpushCommand, "w+")         // Lists
	s.register("rpush", rpushCommand, "w+")         // Lists
	s.register("lrange", lrangeCommand, "r")        // Lists
	s.register("llen", llenCommand, "r")            // Lists
	s.register("lpop", lpopCommand, "w+")           // Lists
	s.register("rpop", rpopCommand, "w+")           // Lists
	s.register("lindex", lindexCommand, "r")        // Lists
	s.register("lrem", lremCommand, "w+")           // Lists
	s.register("lset", lsetCommand, "w+")           // Lists
	s.register("ltrim", ltrimCommand, "w+")         // Lists
	s.register("rpoplpush", rpoplpushCommand, "w+") // Lists

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

	s.register("flushdb", flushdbCommand, "w+")          // Server
	s.register("flushall", flushallCommand, "w+")        // Server
	s.register("dbsize", dbsizeCommand, "r")             // Server
	s.register("debug", debugCommand, "w")               // Server
	s.register("bgrewriteaof", bgrewriteaofCommand, "w") // Server
	s.register("bgsave", bgsaveCommand, "w")             // Server
	s.register("save", saveCommand, "w")                 // Server
	s.register("lastsave", lastsaveCommand, "r")         // Server
	s.register("shutdown", shutdownCommand, "w")         // Server
	s.register("info", infoCommand, "r")                 // Server
	s.register("monitor", monitorCommand, "w")           // Server
	s.register("config", configCommand, "w")             // Server
	s.register("auth", authCommand, "r")                 // Server

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
	s.register("expireat", expireatCommand, "w+")  // Keys
}

var errShutdownSave = errors.New("shutdown and save")
var errShutdownNoSave = errors.New("shutdown and nosave")

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
	AppendOnlyPath   string
	AppName, Version string
	Args             []string
}

// Server represents a server object.
type Server struct {
	mu      sync.RWMutex
	l       net.Listener
	options *Options // options that are passed from the caller
	cfg     *config  // server configuration
	cmds    map[string]*command
	dbs     map[int]*database
	started time.Time

	clients  map[*client]bool // connected clients
	monitors map[*client]bool // clients monitoring

	follower   bool
	mode       string
	executable string

	expiresdone bool // flag for when the expires loop ends

	aof        *os.File // the aof file handle
	aofdbnum   int      // the db num of the last "select" written to the aof
	aofclosed  bool     // flag for when the aof file is closed
	aofrewrite bool     // flag for when the aof is in the process of being rewritten
	aofPath    string   // the full absolute path to the aof file

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
func log(w io.Writer, c byte, format string, args ...interface{}) {
	fmt.Fprintf(
		w,
		"%d:M %s %c %s\n",
		os.Getpid(),
		time.Now().Format("2 Jan 15:04:05.000"),
		c,
		fmt.Sprintf(format, args...),
	)
}

func (s *Server) ldebugf(format string, args ...interface{}) {
	if !s.options.IgnoreLogDebug {
		log(s.options.LogWriter, '.', format, args...)
	}
}
func (s *Server) lverbosf(format string, args ...interface{}) {
	if !s.options.IgnoreLogVerbose {
		log(s.options.LogWriter, '-', format, args...)
	}
}
func (s *Server) lnoticef(format string, args ...interface{}) {
	if !s.options.IgnoreLogNotice {
		log(s.options.LogWriter, '*', format, args...)
	}
}
func (s *Server) lwarningf(format string, args ...interface{}) {
	if !s.options.IgnoreLogWarning {
		log(s.options.LogWriter, '#', format, args...)
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

func (s *Server) getFatalError() error {
	s.ferrcond.L.Lock()
	defer s.ferrcond.L.Unlock()
	return s.ferr
}

// startExpireLoop runs a background routine which watches for exipred keys
// and forces their removal from the database. One second resolution.
func (s *Server) startExpireLoop() {
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for range t.C {
			s.mu.Lock()
			if s.expiresdone {
				s.mu.Unlock()
				return
			}
			s.forceDeleteExpires()
			s.mu.Unlock()
		}
	}()
}

func (s *Server) forceDeleteExpires() {
	if s.follower {
		return
	}
	deleted := false
	for _, db := range s.dbs {
		if db.deleteExpires() {
			deleted = true
		}
	}
	if deleted {
		if err := s.flushAOF(); err != nil {
			s.fatalError(err)
			return
		}
	}
}

// stopExpireLoop will force delete all expires and stop the background routine
func (s *Server) stopExpireLoop() {
	s.mu.Lock()
	s.forceDeleteExpires()
	s.expiresdone = true
	s.mu.Unlock()
}

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
				s.ferrcond.L.Unlock()
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
		db = newDB(num)
		s.dbs[num] = db
	}
	return db
}

func Start(options *Options) (err error) {
	s := &Server{
		cmds:     make(map[string]*command),
		dbs:      make(map[int]*database),
		clients:  make(map[*client]bool),
		monitors: make(map[*client]bool),
		aofdbnum: -1,
		ferrcond: sync.NewCond(&sync.Mutex{}),
		started:  time.Now(),
		mode:     "standalone",
		follower: false,
	}
	var ready bool
	defer func() {
		if err == nil && s.ferr != nil {
			err = s.ferr
		}
		switch err {
		case errShutdownSave, errShutdownNoSave:
			err = nil
		}
		if ready {
			s.lwarningf("%s is now ready to exit, bye bye...", s.options.AppName)
		}
	}()
	options, configMap, configFile, ok := fillOptions(options)
	s.options = options // this should be set even if there's an error.
	if !ok {
		err = errors.New("options failure")
		return
	}
	s.cfg, err = fillConfig(configMap, configFile)
	if err != nil {
		err = errors.New("config failure")
		//s.lwarningf("%v", err)
		return
	}
	s.lwarningf("Server started, %s version %s", s.options.AppName, s.options.Version)
	s.commandTable()
	ready = true

	var wd string
	wd, err = os.Getwd()
	if err != nil {
		s.lwarningf("%v", err)
		return err
	}
	s.executable = path.Join(wd, os.Args[0])
	s.aofPath = s.options.AppendOnlyPath
	if !path.IsAbs(s.aofPath) {
		s.aofPath = path.Join(wd, s.aofPath)
	}
	if err = s.openAOF(); err != nil {
		s.lwarningf("%v", err)
		return err
	}
	defer func() {
		switch s.getFatalError() {
		case errShutdownSave:
			s.lnoticef("DB saved on disk")
		}
	}()
	defer s.closeAOF()
	defer s.flushAOF()
	s.startExpireLoop()
	defer s.stopExpireLoop()
	addr := s.cfg.kvm["bind"] + ":" + s.cfg.kvm["port"]
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

	conns := make(map[net.Conn]bool)
	defer func() {
		for conn := range conns {
			conn.Close()
			delete(conns, conn)
		}
	}()
	defer func() {
		switch s.getFatalError() {
		case errShutdownSave, errShutdownNoSave:
			s.lwarningf("User requested shutdown...")
		}
	}()

	for {
		conn, err := s.l.Accept()
		if err != nil {
			ferr := s.getFatalError()
			if ferr != errShutdownSave && ferr != errShutdownNoSave {
				return err
			} else {
				return nil
			}
		}
		conns[conn] = true
		go handleConn(conn, s)

	}
}

func (s *Server) broadcastMonitors(dbnum int, addr string, args []string) {
	s.mu.Lock()
	t := float64(time.Now().UnixNano()) / float64(time.Second)
	s.mu.Unlock()
	w := &bytes.Buffer{}
	fmt.Fprintf(w, "+%.6f [%d %s]", t, dbnum, addr)
	for _, arg := range args {
		w.WriteByte(' ')
		w.WriteByte('"')
		for i := 0; i < len(arg); i++ {
			ch := arg[i]
			switch {
			default:
				w.WriteByte('\\')
				w.WriteByte('x')
				hex := strconv.FormatUint(uint64(ch), 16)
				if len(hex) == 1 {
					w.WriteByte('0')
				}
				w.WriteString(hex)
			case ch > 31 && ch < 127:
				w.WriteByte(ch)
			}
		}
		w.WriteByte('"')
	}
	w.WriteByte('\r')
	w.WriteByte('\n')
	s.mu.Lock()
	for c := range s.monitors {
		if wr, ok := c.wr.(*bufio.Writer); ok {
			wr.WriteString(w.String())
			wr.Flush()
		}
	}
	s.mu.Unlock()
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

func (s *Server) protected() bool {
	if !s.cfg.protectedMode {
		return false
	}
	if !s.cfg.bindIsLocal {
		return false
	}
	return s.cfg.protectedMode && s.cfg.requirepass == ""
}

func handleConn(conn net.Conn, s *Server) {
	defer conn.Close()
	rd := newCommandReader(conn)
	wr := bufio.NewWriter(conn)
	defer wr.Flush()
	c := &client{wr: wr, s: s}
	c.addr = conn.RemoteAddr().String()
	defer c.flushAOF()
	s.mu.Lock()
	s.clients[c] = true
	c.db = s.selectDB(0)
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.clients, c)
		delete(s.monitors, c)
		s.mu.Unlock()
	}()
	var flush bool
	var err error
	for {
		dbnum := c.db.num
		c.errd = false
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
			if c.authenticate(cmd) {
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
				if !c.errd && cmd.name != "monitor" {
					s.broadcastMonitors(dbnum, c.addr, c.args)
				}
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

func bgrewriteaofCommand(c *client) {
	if len(c.args) != 1 {
		c.replyAritryError()
		return
	}
	if ok := c.s.rewriteAOF(); !ok {
		c.replyError("Background append only file rewriting already in progress")
		return
	}
	c.replyString("Background append only file rewriting started")
}

func bgsaveCommand(c *client) {
	if len(c.args) != 1 {
		c.replyAritryError()
		return
	}
	if ok := c.s.rewriteAOF(); !ok {
		c.replyError("Background save already in progress")
		return
	}
	c.replyString("Background saving started")
}

func lastsaveCommand(c *client) {
	if len(c.args) != 1 {
		c.replyAritryError()
		return
	}
	fi, err := c.s.aof.Stat()
	if err != nil {
		c.replyError("Could not get the UNIX timestamp")
		return
	}
	c.replyInt(int(fi.ModTime().Unix()))
}

func saveCommand(c *client) {
	if len(c.args) != 1 {
		c.replyAritryError()
		return
	}
	if !c.s.rewriteAOF() {
		c.replyError("Background save already in progress")
		return
	}
	c.s.mu.Unlock()
	t := time.NewTicker(time.Millisecond * 50)
	defer t.Stop()
	for range t.C {
		c.s.mu.Lock()
		res := c.s.aofrewrite
		c.s.mu.Unlock()
		if !res {
			break
		}
	}
	c.s.mu.Lock()
	c.replyString("OK")
}

func shutdownCommand(c *client) {
	if len(c.args) != 1 && len(c.args) != 2 {
		c.replyAritryError()
		return
	}
	save := true
	if len(c.args) == 2 {
		switch strings.ToLower(c.args[1]) {
		default:
			c.replySyntaxError()
			return
		case "save":
			save = true
		case "nosave":
			save = false
		}
	}

	if save {
		c.s.fatalError(errShutdownSave)
	} else {
		c.s.fatalError(errShutdownNoSave)
	}
}

func monitorCommand(c *client) {
	if len(c.args) != 1 {
		c.replyAritryError()
		return
	}
	if c.monitor {
		return
	}
	c.monitor = true
	c.s.monitors[c] = true
	c.replyString("OK")
}

func configCommand(c *client) {
	if len(c.args) < 2 {
		c.replyAritryError()
		return
	}
	switch strings.ToLower(c.args[1]) {
	default:
		c.replyError("CONFIG subcommand must be one of GET, SET, RESETSTAT, REWRITE")
	case "get":
		configGetCommand(c)
	case "set":
		configSetCommand(c)
	case "resetstat":
		configResetStatCommand(c)
	case "rewrite":
		configRewriteCommand(c)
	}
}
func configGetCommand(c *client) {
	if len(c.args) != 3 {
		c.replyError("Wrong number of arguments for CONFIG " + c.args[1])
		return
	}
	switch c.args[2] {
	default:
		c.replyMultiBulkLen(0)
		return
	case "port", "bind", "protected-mode", "requirepass":
	}
	c.replyMultiBulkLen(2)
	c.replyBulk(c.args[2])
	c.replyBulk(c.s.cfg.kvm[c.args[2]])

}
func configSetCommand(c *client) {
	if len(c.args) != 4 {
		c.replyError("Wrong number of arguments for CONFIG " + c.args[1])
		return
	}
	switch strings.ToLower(c.args[2]) {
	default:
		c.replyError("Unsupported CONFIG parameter: " + c.args[2])
		return
	case "requirepass":
		c.s.cfg.kvm["requirepass"] = c.args[3]
		c.s.cfg.requirepass = c.args[3]
	case "protected-mode":
		switch strings.ToLower(c.args[3]) {
		default:
			c.replyError("Invalid argument '" + c.args[3] + "' for CONFIG SET '" + c.args[2] + "'")
			return
		case "yes":
			c.s.cfg.kvm["protected-mode"] = "yes"
			c.s.cfg.protectedMode = true
		case "no":
			c.s.cfg.kvm["protected-mode"] = "no"
			c.s.cfg.protectedMode = false
		}
	}
	c.replyString("OK")
}
func configResetStatCommand(c *client) {
	c.replyString("OK")
}
func configRewriteCommand(c *client) {
	if len(c.args) != 2 {
		c.replyError("Wrong number of arguments for CONFIG " + c.args[1])
		return
	}
	if c.s.cfg.file == "" {
		c.replyError("The server is running without a config file")
		return
	}
	if err := mergeConfigFile(c.s.cfg.file, c.s.cfg.kvm); err != nil {
		c.replyError(fmt.Sprintf("Rewriting config: %v", err))
		return
	}
	c.replyString("OK")
}

func authCommand(c *client) {
	if len(c.args) != 2 {
		c.replyAritryError()
		return
	}
	if c.s.cfg.requirepass != c.args[1] {
		c.replyError("invalid password")
		return
	}
	c.authd = 2
	c.replyString("OK")
}
