package server

import (
	"bufio"
	"bytes"
	"log"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/google/btree"
)

func (s *Server) commandTable() {
	s.register("get", getCommand, "r")
	s.register("set", setCommand, "w")
	s.register("del", delCommand, "w")
	s.register("flushdb", flushdbCommand, "w")
	s.register("keys", keysCommand, "r")
}

type Key struct {
	Name  string
	Value interface{}
}

func (key *Key) Less(item btree.Item) bool {
	return key.Name < item.(*Key).Name
}

type Command struct {
	Write bool
	Read  bool
	Func  func(client *Client)
}

type Config struct {
	AOFSync int // 0 = never, 1 = everysecond, 2 = always
}

type Server struct {
	mu        sync.RWMutex
	commands  map[string]*Command
	keys      *btree.BTree
	config    Config
	aof       *os.File
	aofbuf    bytes.Buffer
	aofmu     sync.Mutex
	aofclosed bool
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
		}
	}
	s.commands[commandName] = &cmd
}

func (s *Server) GetKey(name string) (interface{}, bool) {
	item := s.keys.Get(&Key{Name: name})
	if item == nil {
		return nil, false
	}
	return item.(*Key).Value, true
}

func (s *Server) SetKey(name string, value interface{}) {
	s.keys.ReplaceOrInsert(&Key{Name: name, Value: value})
}
func (s *Server) DelKey(name string) (interface{}, bool) {
	item := s.keys.Delete(&Key{Name: name})
	if item == nil {
		return nil, false
	}
	return item.(*Key).Value, true
}

func Start(addr string) {
	s := &Server{
		commands: make(map[string]*Command),
		keys:     btree.New(16),
	}
	s.commandTable()
	s.openAOF()
	defer s.closeAOF()

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
		case "ping":
			c.ReplyString("PONG")
		default:
			if cmd, ok := server.commands[command]; ok {
				if cmd.Write {
					server.mu.Lock()
				} else if cmd.Read {
					server.mu.RLock()
				}
				cmd.Func(c)
				if cmd.Write {
					server.aofbuf.Write(c.raw)
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
