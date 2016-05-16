package server

import (
	"bufio"
	"bytes"
	"container/list"
	"log"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/btree"
)

func (s *Server) commandTable() {
	// "r" lock for reading
	// "w" lock for writing
	// "+" write to aof
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

	s.register("echo", echoCommand, "") // Connection
	s.register("ping", pingCommand, "") // Connection

	s.register("flushdb", flushdbCommand, "w+")   // Server
	s.register("flushall", flushallCommand, "w+") // Server
	s.register("dbsize", dbsizeCommand, "r")      // Server

	s.register("del", delCommand, "w+")            // Keys
	s.register("keys", keysCommand, "r")           // Keys
	s.register("rename", renameCommand, "w+")      // Keys
	s.register("renamenx", renamenxCommand, "w+")  // Keys
	s.register("type", typeCommand, "r")           // Keys
	s.register("randomkey", randomkeyCommand, "r") // Keys
	s.register("exists", existsCommand, "r")       // Keys
	s.register("expire", expireCommand, "w+")      // Keys
	s.register("ttl", ttlCommand, "r")             // Keys

}

type Key struct {
	Name    string
	Expires time.Time
	Value   interface{}
}

func (key *Key) Less(item btree.Item) bool {
	return key.Name < item.(*Key).Name
}

type Command struct {
	Name  string
	Write bool
	Read  bool
	AOF   bool
	Func  func(client *Client)
}

type Config struct {
	AOFSync int // 0 = never, 1 = everysecond, 2 = always
}

type Server struct {
	mu       sync.RWMutex
	commands map[string]*Command
	//keys        *btree.BTree
	keys        map[string]*Key
	config      Config
	aof         *os.File
	aofbuf      bytes.Buffer
	aofclosed   bool
	follower    bool
	expires     map[string]time.Time
	expiresdone bool
}

func (s *Server) register(commandName string, f func(client *Client), opts string) {
	var cmd Command
	cmd.Name = commandName
	cmd.Func = f
	for _, c := range []byte(opts) {
		switch c {
		case 'r':
			if !cmd.Write {
				cmd.Read = true
			}
		case 'w':
			cmd.Write = true
			cmd.Read = false
		case '+':
			cmd.Write = true
			cmd.Read = false
			cmd.AOF = true
		}
	}
	s.commands[strings.ToLower(commandName)] = &cmd
	s.commands[strings.ToUpper(commandName)] = &cmd
}

func (s *Server) GetKey(name string) (interface{}, bool) {
	key, ok := s.keys[name]
	if !ok {
		return nil, false
	}
	if !key.Expires.IsZero() && time.Now().After(key.Expires) {
		return nil, false
	}
	return key.Value, true
}

func (s *Server) GetKeyExpires(name string) (interface{}, time.Time, bool) {
	key, ok := s.keys[name]
	if !ok {
		return nil, time.Time{}, false
	}
	if !key.Expires.IsZero() && time.Now().After(key.Expires) {
		return nil, time.Time{}, false
	}
	return key.Value, key.Expires, true
}

func (s *Server) GetKeyList(name string, create bool) (*list.List, bool) {
	key, ok := s.GetKey(name)
	if ok {
		switch v := key.(type) {
		default:
			return nil, false
		case *list.List:
			return v, true
		}
	}
	if create {
		l := list.New()
		s.SetKey(name, l)
		return l, true
	}
	return nil, true
}

func (s *Server) GetKeySet(name string, create bool) (*Set, bool) {
	key, ok := s.GetKey(name)
	if ok {
		switch v := key.(type) {
		default:
			return nil, false
		case *Set:
			return v, true
		}
	}
	if create {
		st := NewSet()
		s.SetKey(name, st)
		return st, true
	}
	return nil, true
}

func (s *Server) SetKey(name string, value interface{}) {
	delete(s.expires, name)
	s.keys[name] = &Key{Name: name, Value: value}
}

func (s *Server) UpdateKey(name string, value interface{}) {
	key, ok := s.keys[name]
	if ok {
		key.Value = value
	} else {
		s.SetKey(name, value)
	}
}

func (s *Server) DelKey(name string) (interface{}, bool) {
	key, ok := s.keys[name]
	if !ok {
		return nil, false
	}
	delete(s.keys, name)
	if !key.Expires.IsZero() && time.Now().After(key.Expires) {
		return nil, false
	}
	delete(s.expires, name)
	return key.Value, true
}

func (s *Server) Expire(name string, when time.Time) bool {
	key, ok := s.keys[name]
	if !ok {
		return false
	}
	key.Expires = when
	s.expires[name] = when
	return true
}

// startExpireLoop runs a background routine which watches for exipred keys
// and forces their removal from the database. 100ms resolution.
func (s *Server) startExpireLoop() {
	go func() {
		t := time.NewTicker(time.Second / 10)
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
	if len(s.expires) == 0 || s.follower {
		return
	}
	now := time.Now()
	var aofbuf bytes.Buffer
	for key, expires := range s.expires {
		if now.After(expires) {
			delete(s.keys, key)
			aofbuf.WriteString("*2\r\n$3\r\ndel\r\n$")
			aofbuf.WriteString(strconv.FormatInt(int64(len(key)), 10))
			aofbuf.WriteString("\r\n")
			aofbuf.WriteString(key)
			aofbuf.WriteString("\r\n")
			delete(s.expires, key)
		}
	}
	if aofbuf.Len() > 0 {
		if _, err := s.aof.Write(aofbuf.Bytes()); err != nil {
			panic(err)
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

func Start(addr string) {
	s := &Server{
		commands: make(map[string]*Command),
		//keys:     btree.New(16),
		keys:    make(map[string]*Key),
		expires: make(map[string]time.Time),
	}
	s.commandTable()
	s.openAOF()
	defer s.closeAOF()
	s.startExpireLoop()
	defer s.stopExpireLoop()

	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("# %v", err)
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Printf("# %v", err)
			continue
		}
		go handleConn(conn, s)
	}
}

func autoCase(command string) string {
	for i := 0; i < len(command); i++ {
		c := command[i]
		if c >= 'A' && c <= 'Z' {
			for ; i < len(command); i++ {
				c = command[i]
				if c >= 'a' && c <= 'z' {
					return strings.ToLower(command)
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

func handleConn(conn net.Conn, server *Server) {
	defer conn.Close()
	rd := NewCommandReader(conn)
	wr := bufio.NewWriter(conn)
	defer wr.Flush()
	c := &Client{wr: wr, server: server}
	defer c.flushAOF()
	var flush bool
	var err error
	for {
		c.raw, c.args, flush, err = rd.ReadCommand()
		if err != nil {
			if err, ok := err.(*protocolError); ok {
				c.ReplyError(err.Error())
			}
			return
		}
		if len(c.args) == 0 {
			continue
		}
		command := autoCase(c.args[0])
		if cmd, ok := server.commands[command]; ok {
			server.mu.Lock()
			cmd.Func(c)
			if c.dirty > 0 && cmd.AOF {
				server.aofbuf.Write(c.raw)
			}
			server.mu.Unlock()
		} else {
			switch command {
			default:
				c.ReplyError("unknown command '" + c.args[0] + "'")
			case "quit":
				c.ReplyString("OK")
				return
			}
		}
		if flush {
			c.flushAOF()
			if err := wr.Flush(); err != nil {
				return
			}
		}
	}
}

/* Commands */
func flushdbCommand(client *Client) {
	if len(client.args) != 1 {
		client.ReplyAritryError()
		return
	}
	client.server.keys = make(map[string]*Key)
	client.ReplyString("OK")
	client.dirty++
	go runtime.GC()
}

func flushallCommand(client *Client) {
	if len(client.args) != 1 {
		client.ReplyAritryError()
		return
	}
	client.server.keys = make(map[string]*Key)
	client.ReplyString("OK")
	client.dirty++
	go runtime.GC()
}

func dbsizeCommand(client *Client) {
	if len(client.args) != 1 {
		client.ReplyAritryError()
		return
	}
	client.ReplyInt(len(client.server.keys))
}
