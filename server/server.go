package server

import (
	"bufio"
	"io"
	"log"
	"net"
	"strconv"
	"strings"

	"github.com/google/btree"
)

func (s *Server) commandTable() {
	s.register("get", getCommand, "r")
	s.register("set", setCommand, "w")

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

type Server struct {
	commands map[string]*Command
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

func Start(addr string) {
	s := &Server{
		commands: make(map[string]*Command),
	}
	s.commandTable()
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
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
		_, args, flush, err := rd.ReadCommand()
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
		command := strings.ToLower(args[0])
		switch command {
		case "quit":
			c.ReplyString("OK")
			return
		case "ping":
			c.ReplyString("PONG")
		default:
			if cmd, ok := server.commands[command]; ok {
				cmd.Func(c)
			} else {
				c.ReplyError("unknown command '" + args[0] + "'")
			}
		}

		if flush {
			wr.Flush()
		}
	}
}

type protocolError struct {
	msg string
}

func (err *protocolError) Error() string {
	return "Protocol error: " + err.msg
}

type CommandReader struct {
	rd     io.Reader
	rbuf   []byte
	buf    []byte
	copied bool
}

func (rd *CommandReader) ReadCommand() (raw []byte, args []string, flush bool, err error) {
	if len(rd.buf) > 0 {
		// there is already data in the buffer, do we have enough to make a full command?
		raw, args, err := readBufferedCommand(rd.buf)
		if err != nil {
			return nil, nil, false, err
		}
		if len(raw) == len(rd.buf) {
			// we have a command and it's exactly the size of the buffer.
			// clear out the buffer and return the command
			// notify the caller that we should flush after this command.
			rd.buf = nil
			return raw, args, true, nil
		} else if len(raw) > 0 {
			// have a command, but there's still data in the buffer.
			// notify the caller that we should flush *only* when there's copied data.
			rd.buf = rd.buf[len(raw):]
			return raw, args, rd.copied, nil
		} else if raw != nil {
			// empty command
			return raw, args, true, nil
		}
		// only have a partial command, read more data
	}
	if len(rd.buf) > 0 && !rd.copied {
		// make sure to copy the buffer to a new array prior to reading from conn
		nbuf := make([]byte, len(rd.buf))
		copy(nbuf, rd.buf)
		rd.buf = nbuf
		rd.copied = true
	}
	n, err := rd.rd.Read(rd.rbuf)
	if err != nil {
		return nil, nil, false, err
	}
	if len(rd.buf) == 0 {
		rd.buf = rd.rbuf[:n]
		rd.copied = false
	} else {
		rd.buf = append(rd.buf, rd.rbuf[:n]...)
		rd.copied = true
	}
	return rd.ReadCommand()
}

func readBufferedCommand(data []byte) ([]byte, []string, error) {
	var args []string
	if data[0] != '*' {
		return readBufferedTelnetCommand(data)
	}
	for i := 1; i < len(data); i++ {
		if data[i] == '\n' {
			if data[i-1] != '\r' {
				return nil, nil, &protocolError{"invalid multibulk length"}
			}
			n, err := strconv.ParseInt(string(data[1:i-1]), 10, 64)
			if err != nil {
				return nil, nil, &protocolError{"invalid multibulk length"}
			}
			if n < 0 {
				return []byte{}, []string{}, nil
			}
			i++
			for j := int64(0); j < n; j++ {
				if i == len(data) {
					return nil, nil, nil
				}
				if data[i] != '$' {
					return nil, nil, &protocolError{"expected '$', got '" + string(data[i]) + "'"}
				}
				ii := i + 1
				for ; i < len(data); i++ {
					if data[i] == '\n' {
						if data[i-1] != '\r' {
							return nil, nil, &protocolError{"invalid bulk length"}
						}
						n2, err := strconv.ParseUint(string(data[ii:i-1]), 10, 64)
						if err != nil {
							return nil, nil, &protocolError{"invalid bulk length"}
						}
						i++
						if len(data)-i < int(n2+2) {
							return nil, nil, nil // more data
						}
						args = append(args, string(data[i:i+int(n2)]))
						i += int(n2 + 2)
						if j == int64(n-1) {
							return data[:i], args, nil
						}
					}
				}
			}
			break
		}
	}
	return nil, nil, nil // more data
}

func readBufferedTelnetCommand(data []byte) ([]byte, []string, error) {
	for i := 1; i < len(data); i++ {
		if data[i] == '\n' {
			var line []byte
			if data[i-1] == '\r' {
				line = data[:i-1]
			} else {
				line = data[:i]
			}
			return data[:i+1], []string{string(line)}, nil
		}
	}
	return nil, nil, nil
}
