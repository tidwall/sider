package server

import (
	"bufio"
	"bytes"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/btree"
)

func (s *Server) commandTable() {
	// r - lock for reading.
	// w - lock for writing.
	// + - write to aof
	s.register("get", getCommand, "r")             // Strings
	s.register("getset", getsetCommand, "w+")      // Strings
	s.register("set", setCommand, "w+")            // Strings
	s.register("append", appendCommand, "w+")      // Strings
	s.register("bitcount", bitcountCommand, "r")   // Strings
	s.register("incr", incrCommand, "w+")          // Strings
	s.register("echo", echoCommand, "")            // Connection
	s.register("ping", pingCommand, "")            // Connection
	s.register("flushdb", flushdbCommand, "w+")    // Server
	s.register("flushall", flushallCommand, "w+")  // Server
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
	Write bool
	Read  bool
	AOF   bool
	Func  func(client *Client)
}

type Config struct {
	AOFSync int // 0 = never, 1 = everysecond, 2 = always
}

type Server struct {
	mu          sync.RWMutex
	commands    map[string]*Command
	keys        *btree.BTree
	config      Config
	aof         *os.File
	aofbuf      bytes.Buffer
	aofmu       sync.Mutex
	aofclosed   bool
	follower    bool
	expires     map[string]time.Time
	expiresdone bool
}

func (s *Server) register(commandName string, f func(client *Client), opts string) {
	var cmd Command
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
	s.commands[commandName] = &cmd
}

func (s *Server) GetKey(name string) (interface{}, bool) {
	item := s.keys.Get(&Key{Name: name})
	if item == nil {
		return nil, false
	}
	key := item.(*Key)
	if !key.Expires.IsZero() && time.Now().After(key.Expires) {
		return nil, false
	}
	return key.Value, true
}

func (s *Server) GetKeyExpires(name string) (interface{}, time.Time, bool) {
	item := s.keys.Get(&Key{Name: name})
	if item == nil {
		return nil, time.Time{}, false
	}
	key := item.(*Key)
	if !key.Expires.IsZero() && time.Now().After(key.Expires) {
		return nil, time.Time{}, false
	}
	return key.Value, key.Expires, true
}

func (s *Server) SetKey(name string, value interface{}) {
	delete(s.expires, name)
	s.keys.ReplaceOrInsert(&Key{Name: name, Value: value})
}

func (s *Server) UpdateKey(name string, value interface{}) {
	item := s.keys.Get(&Key{Name: name})
	if item != nil {
		item.(*Key).Value = value
	}
}

func (s *Server) DelKey(name string) (interface{}, bool) {
	item := s.keys.Delete(&Key{Name: name})
	if item == nil {
		return nil, false
	}
	key := item.(*Key)
	if !key.Expires.IsZero() && time.Now().After(key.Expires) {
		return nil, false
	}
	delete(s.expires, name)
	return item.(*Key).Value, true
}

func (s *Server) Expire(name string, when time.Time) bool {
	item := s.keys.Get(&Key{Name: name})
	if item == nil {
		return false
	}
	key := item.(*Key)
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
			s.keys.Delete(&Key{Name: key})
			aofbuf.WriteString("*2\r\n$3\r\ndel\r\n$")
			aofbuf.WriteString(strconv.FormatInt(int64(len(key)), 10))
			aofbuf.WriteString("\r\n")
			aofbuf.WriteString(key)
			aofbuf.WriteString("\r\n")
			delete(s.expires, key)
		}
	}
	if aofbuf.Len() > 0 {
		s.aofmu.Lock()
		if _, err := s.aof.Write(aofbuf.Bytes()); err != nil {
			panic(err)
		}
		s.aofmu.Unlock()
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
		keys:     btree.New(16),
		expires:  make(map[string]time.Time),
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

func handleConn(conn net.Conn, server *Server) {
	defer conn.Close()
	wr := bufio.NewWriter(conn)
	defer wr.Flush()
	rd := &CommandReader{rd: conn, rbuf: make([]byte, 64*1024)}
	c := &Client{wr: wr, server: server}
	for {
		raw, args, flush, err := rd.ReadCommand()
		if err != nil {
			if err, ok := err.(*protocolError); ok {
				c.ReplyError(err.Error())
			}
			return
		}
		if len(args) == 0 {
			continue
		}
		c.args = args
		c.raw = raw
		command := strings.ToLower(args[0])
		switch command {
		case "quit":
			c.ReplyString("OK")
			return
		default:
			if cmd, ok := server.commands[command]; ok {
				if cmd.Write {
					server.mu.Lock()
				} else if cmd.Read {
					server.mu.RLock()
				}
				cmd.Func(c)
				if c.dirty > 0 {
					if cmd.AOF {
						server.aofbuf.Write(c.raw)
					}
					c.dirty = 0
				}
				if cmd.Write {
					server.mu.Unlock()
				} else if cmd.Read {
					server.mu.RUnlock()
				}
			} else {
				c.ReplyError("unknown command '" + args[0] + "'")
			}
		}
		if flush {
			server.mu.Lock()
			if server.aofbuf.Len() > 0 {
				b := server.aofbuf.Bytes()
				server.aofbuf.Reset()
				server.mu.Unlock()
				server.aofmu.Lock()
				if _, err := server.aof.Write(b); err != nil {
					panic(err)
				}
				server.aofmu.Unlock()
			} else {
				server.mu.Unlock()
			}

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
	client.server.keys = btree.New(16)
	client.ReplyString("OK")
}

func flushallCommand(client *Client) {
	if len(client.args) != 1 {
		client.ReplyAritryError()
		return
	}
	client.server.keys = btree.New(16)
	client.ReplyString("OK")
}
