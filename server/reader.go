package server

import (
	"bytes"
	"errors"
	"io"
	"strconv"
)

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
	args   []string
}

func NewCommandReader(rd io.Reader) *CommandReader {
	return &CommandReader{
		rd:   rd,
		rbuf: make([]byte, 64*1024),
	}
}

// autoConvertArgsToMultiBulk converts telnet style commands to resp autobulk commands.
func autoConvertArgsToMultiBulk(raw []byte, args []string, telnet, flush bool) ([]byte, []string, bool, error) {
	if telnet {
		var buf bytes.Buffer
		buf.WriteString("*" + strconv.FormatInt(int64(len(args)), 10) + "\r\n")
		for _, arg := range args {
			buf.WriteString("$" + strconv.FormatInt(int64(len(arg)), 10) + "\r\n")
			buf.WriteString(arg + "\r\n")
		}
		raw = buf.Bytes()
	}
	return raw, args, flush, nil
}

func (rd *CommandReader) ReadCommand() (raw []byte, args []string, flush bool, err error) {
	if len(rd.buf) > 0 {
		// there is already data in the buffer, do we have enough to make a full command?
		raw, args, telnet, err := rd.readBufferedCommand(rd.buf)
		if err != nil {
			return nil, nil, false, err
		}
		if len(raw) == len(rd.buf) {
			// we have a command and it's exactly the size of the buffer.
			// clear out the buffer and return the command
			// notify the caller that we should flush after this command.
			rd.buf = nil
			if telnet {
				return autoConvertArgsToMultiBulk(raw, args, telnet, true)
			}
			return raw, args, true, nil
		}
		if len(raw) > 0 {
			// have a command, but there's still data in the buffer.
			// notify the caller that we should flush *only* when there's copied data.
			rd.buf = rd.buf[len(raw):]
			if telnet {
				return autoConvertArgsToMultiBulk(raw, args, telnet, rd.copied)
			}
			return raw, args, rd.copied, nil
		}
		// only have a partial command, read more data
	}
	n, err := rd.rd.Read(rd.rbuf)
	if err != nil {
		return nil, nil, false, err
	}
	if len(rd.buf) == 0 {
		// copy the data rather than assign a slice, otherwise string
		// corruption may occur on the next network read.
		rd.buf = append([]byte(nil), rd.rbuf[:n]...)
		rd.copied = false
	} else {
		rd.buf = append(rd.buf, rd.rbuf[:n]...)
		rd.copied = true
	}
	return rd.ReadCommand()
}

func (rd *CommandReader) readBufferedCommand(data []byte) ([]byte, []string, bool, error) {
	if data[0] != '*' {
		return readBufferedTelnetCommand(data)
	}
	for i := 1; i < len(data); i++ {
		if data[i] == '\n' {
			if data[i-1] != '\r' {
				return nil, nil, false, &protocolError{"invalid multibulk length"}
			}
			n, err := atoi(string(data[1 : i-1]))
			if err != nil {
				return nil, nil, false, &protocolError{"invalid multibulk length"}
			}
			if n <= 0 {
				return data[:i+1], []string{}, false, nil
			}

			// grow the args array
			if n > len(rd.args) {
				nlen := len(rd.args)
				if nlen == 0 {
					nlen = 1
				}
				for n > nlen {
					nlen *= 2
				}
				rd.args = make([]string, nlen)
			}
			i++
			for j := 0; j < n; j++ {
				if i == len(data) {
					return nil, nil, false, nil
				}
				if data[i] != '$' {
					return nil, nil, false, &protocolError{"expected '$', got '" + string(data[i]) + "'"}
				}
				ii := i + 1
				for ; i < len(data); i++ {
					if data[i] == '\n' {
						if data[i-1] != '\r' {
							return nil, nil, false, &protocolError{"invalid bulk length"}
						}
						n2, err := atoui(string(data[ii : i-1]))
						if err != nil {
							return nil, nil, false, &protocolError{"invalid bulk length"}
						}
						i++
						if len(data)-i < n2+2 {
							return nil, nil, false, nil // more data
						}
						rd.args[j] = string(data[i : i+n2])
						i += n2 + 2
						if j == n-1 {
							return data[:i], rd.args[:n], false, nil
						}
						break
					}
				}
			}
			break
		}
	}
	return nil, nil, false, nil // more data
}

func readBufferedTelnetCommand(data []byte) ([]byte, []string, bool, error) {
	for i := 1; i < len(data); i++ {
		if data[i] == '\n' {
			var line []byte
			if data[i-1] == '\r' {
				line = data[:i-1]
			} else {
				line = data[:i]
			}
			if len(line) == 0 {
				return data[:i+1], []string{}, true, nil
			}
			args, err := parseArgsFromTelnetLine(line)
			if err != nil {
				return nil, nil, true, err
			}
			return data[:i+1], args, true, nil
		}
	}
	return nil, nil, true, nil
}

func parseArgsFromTelnetLine(line []byte) ([]string, error) {
	var args []string
	var s int
	lspace := true
	quote := false
	lquote := false
	for i := 0; i < len(line); i++ {
		switch line[i] {
		default:
			lspace = false
		case '"':
			if quote {
				args = append(args, string(line[s+1:i]))
				quote = false
				s = i + 1
				lquote = true
				continue
			}
			if !lspace {
				return nil, &protocolError{"unbalanced quotes in request"}
			}
			lspace = false
			quote = true
		case ' ':
			if lquote {
				s++
				continue
			}
			args = append(args, string(line[s:i]))
			s = i + 1
			lspace = true
		}
	}
	if quote {
		return nil, &protocolError{"unbalanced quotes in request"}
	}
	if s < len(line) {
		args = append(args, string(line[s:]))
	}
	return args, nil
}

func atoi(s string) (int, error) {
	if len(s) == 0 {
		return 0, errors.New("invalid integer")
	}
	var sign bool
	var n int
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= '0' && c <= '9' {
			n = n*10 + int(c-'0')
		} else if c == '-' {
			if i != 0 || len(s) == 1 {
				return 0, errors.New("invalid integer")
			}
			sign = true
		} else {
			return 0, errors.New("invalid integer")
		}
	}
	if sign {
		n *= -1
	}
	return n, nil
}

func atoui(s string) (int, error) {
	if len(s) == 0 {
		return 0, errors.New("invalid integer")
	}
	var n int
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= '0' && c <= '9' {
			n = n*10 + int(s[i]-'0')
		} else {
			return 0, errors.New("invalid integer")
		}
	}
	return n, nil
}
