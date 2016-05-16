package server

import (
	"errors"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"time"
)

func (s *Server) openAOF() error {
	f, err := os.OpenFile("appendonly.aof", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	s.aof = f
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for range t.C {
			s.mu.Lock()
			if s.aofclosed {
				s.mu.Unlock()
				return
			}
			s.aof.Sync()
			s.mu.Unlock()
		}
	}()
	return s.loadAOF()
}

func (s *Server) flushAOF() error {
	if s.dbs[s.aofdbnum] != nil {
		db := s.dbs[s.aofdbnum]
		if db.aofbuf.Len() > 0 {
			if _, err := s.aof.Write(db.aofbuf.Bytes()); err != nil {
				return err
			}
			db.aofbuf.Reset()
		}
	}
	for num, db := range s.dbs {
		if db.aofbuf.Len() > 0 {
			selstr := strconv.FormatInt(int64(num), 10)
			lenstr := strconv.FormatInt(int64(len(selstr)), 10)
			if _, err := s.aof.WriteString("*2\r\n$6\r\nselect\r\n$" + lenstr + "\r\n" + selstr + "\r\n"); err != nil {
				return err
			}
			if _, err := s.aof.Write(db.aofbuf.Bytes()); err != nil {
				return err
			}
			db.aofbuf.Reset()
			s.aofdbnum = num
		}
	}
	return nil
}

func (s *Server) closeAOF() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushAOF()
	s.aof.Sync()
	s.aof.Close()
	s.aofclosed = true
}

func (s *Server) loadAOF() error {
	start := time.Now()
	rd := &commandReader{rd: s.aof, rbuf: make([]byte, 64*1024)}
	c := &client{wr: ioutil.Discard, s: s}
	var read int
	for {
		raw, args, _, err := rd.ReadCommand()
		if err != nil {
			if err == io.EOF {
				break
			}
			s.lwarningf("%v", err)
			return err
		}
		c.args = args
		c.raw = raw
		c.db = s.selectDB(0)
		commandName := autocase(args[0])
		if cmd, ok := s.cmds[commandName]; ok {
			cmd.funct(c)
		} else {
			return errors.New("unknown command '" + args[0] + "'")
		}
		read++
	}
	s.lnoticef("DB loaded from disk: %.3f seconds", float64(time.Now().Sub(start))/float64(time.Second))
	return nil
}
