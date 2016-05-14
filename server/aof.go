package server

import (
	"io"
	"io/ioutil"
	"log"
	"os"
	"time"
)

func (s *Server) openAOF() {
	f, err := os.OpenFile("appendonly.aof", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatalf("# %v", err)
	}
	s.aof = f
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for range t.C {
			s.aofmu.Lock()
			if s.aofclosed {
				s.aofmu.Unlock()
				return
			}
			s.aof.Sync()
			s.aofmu.Unlock()
		}
	}()
	s.loadAOF()
}

func (s *Server) closeAOF() {
	s.aofmu.Lock()
	defer s.aofmu.Unlock()
	s.aof.Sync()
	s.aof.Close()
	s.aofclosed = true
}

func (s *Server) loadAOF() {
	start := time.Now()
	rd := &CommandReader{rd: s.aof, rbuf: make([]byte, 64*1024)}
	c := &Client{wr: ioutil.Discard, server: s}
	var read int
	for {
		raw, args, _, err := rd.ReadCommand()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("# %v", err)
		}
		c.args = args
		c.raw = raw
		if cmd, ok := s.commands[args[0]]; ok {
			cmd.Func(c)
		} else {
			c.ReplyError("unknown command '" + args[0] + "'")
		}
		read++
	}
	log.Printf("* AOF loaded %d commands from disk: %.3f seconds", read, float64(time.Now().Sub(start))/float64(time.Second))
}
