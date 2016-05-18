package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"time"
)

// openAOF opens the appendonly.aof file and loads it.
// There is also a background goroutine that syncs every seconds.
func (s *Server) openAOF() error {
	f, err := os.OpenFile(s.aofPath, os.O_CREATE|os.O_RDWR, 0644)
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

func writeBulk(wr io.Writer, arg string) {
	fmt.Fprintf(wr, "$%d\r\n%s\r\n", len(arg), arg)
}
func writeMultiBulkLen(wr io.Writer, n int) {
	fmt.Fprintf(wr, "*%d\r\n", n)
}
func writeMultiBulk(wr io.Writer, args ...interface{}) {
	writeMultiBulkLen(wr, len(args))
	for _, arg := range args {
		var sarg string
		switch v := arg.(type) {
		default:
			sarg = fmt.Sprintf("%v", v)
		case string:
			sarg = v
		}
		writeBulk(wr, sarg)
	}
}

type dbsByNumber []*database

func (a dbsByNumber) Len() int {
	return len(a)
}

func (a dbsByNumber) Less(i, j int) bool {
	return a[i].num < a[j].num
}

func (a dbsByNumber) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

type keyItemByKey struct {
	keys  []string
	items []dbItem
}

func (a *keyItemByKey) Len() int {
	return len(a.keys)
}

func (a *keyItemByKey) Less(i, j int) bool {
	return a.keys[i] < a.keys[j]
}

func (a *keyItemByKey) Swap(i, j int) {
	a.keys[i], a.keys[j] = a.keys[j], a.keys[i]
	a.items[i], a.items[j] = a.items[j], a.items[i]
}

// rewriteAOF triggers a background rewrite of the AOF file.
// Returns true if the process was started, or false if the the process a
// rewrite is already in progress. There are a number of locks and unlocks which
// is required to keep the server running in the foreground and background. We
// process in chunks and try not to hang on to locks for longer than we need.
// The rewrite process will slow down the main server a little bit but it
// shouldn't be too noticeable.
func (s *Server) rewriteAOF() bool {
	if s.aofrewrite {
		return false
	}
	s.aofrewrite = true
	s.lnoticef("Background append only file rewriting started")
	go func() {
		// We use one err variable for the entire process. When we encounter an
		// error we should assign this variable and return. Before calling
		// return there we should be in lock mode (s.mu.Lock()).
		var err error
		s.mu.Lock()
		defer func() {
			if err == nil {
				s.lnoticef("Background AOF rewrite finished successfully")
			} else {
				s.lnoticef("Background AOF rewrite failed: %v", err)
			}
			s.aofrewrite = false
			s.mu.Unlock()
		}()

		// Create a temporary aof file for writting the new commands to. If this
		// process is successful then this file will become the active AOF.

		tempName := path.Join(path.Dir(s.aofPath),
			fmt.Sprintf("temp-rewrite-%d.aof", os.Getpid()))
		var f *os.File
		f, err = os.Create(tempName)
		if err != nil {
			return
		}
		defer func() {
			// If this process was successful then the next close/removeall
			// calls are noops. Otherwise we need to cleanup.
			f.Close()
			os.RemoveAll(tempName)
		}()

		// We use a buffered writer instead of writing directly to the file.
		// Doing so keeps makes the process much quicker by avoiding too many
		// writes to the file.
		wr := bufio.NewWriter(f)

		// Get the size of the active AOF file, and get the last DB num that was
		// used when the previous command was written. These both will be used
		// to return to the active AOF file and sync the remaining commands
		// which reflect changes that have occured since the start of the
		// rewrite.
		var lastpos int64
		lastpos, err = s.aof.Seek(0, 1)
		if err != nil {
			return
		}
		lastdbnum := s.aofdbnum
		s.ldebugf("AOF starting pos: %v, dbnum: %v", lastpos, lastdbnum)
		s.mu.Unlock()

		// From here until near the end of the process we will only use read
		// locks since we are not mutating the database during the rewrite.
		// This will allow for other clients to do reading commands such as
		// GET without penalty. Writing command such as SET will see a slight
		// delay, but we refresh the read locks often enough that the won't
		// be much of a performance hit.

		// Read all dbs into a local array
		s.mu.RLock()
		dbs := make([]*database, len(s.dbs))
		var i int
		for _, db := range s.dbs {
			dbs[i] = db
			i++
		}
		s.mu.RUnlock()

		// Sort the dbs by number.
		sort.Sort(dbsByNumber(dbs))

		dbnum := -1

		// Use single time for all expires.
		now := time.Now()

		// Loop though local db array proessing each one. Since we are iterating
		// a map there the order of the databases will be random.
		for _, db := range dbs {
			s.mu.RLock()
			if len(db.items) == 0 {
				s.mu.RUnlock()
				continue // skip empty databases
			}
			// write a SELECT command. If the first command is `SELECT 0` then
			// skip this write.
			if !(dbnum == -1 && db.num == 0) {
				writeMultiBulk(wr, "SELECT", db.num)
			}
			dbnum = db.num
			// collect db items (keys) into local variables
			var msets []interface{}

			keys := make([]string, len(db.items))
			items := make([]dbItem, len(db.items))
			expires := make(map[string]time.Time)
			expireKeys := make([]string, len(expires))
			i := 0
			for key, item := range db.items {
				items[i] = item
				keys[i] = key
				i++
			}
			i = 0
			for key, t := range db.expires {
				expires[key] = t
				expireKeys[i] = key
				i++
			}
			// Sort the keys and let the lock breath for a moment
			s.mu.RUnlock()
			sort.Strings(expireKeys)
			sort.Sort(&keyItemByKey{keys, items})
			s.mu.RLock()
			for i := 0; i < len(keys); i++ {
				key := keys[i]
				item := items[i]
				if i%100 == 0 {
					// let the lock breath for a moment
					s.mu.RUnlock()
					err = wr.Flush()
					if err != nil {
						s.mu.Lock() // lock write on error
						return
					}
					s.mu.RLock()
				}
				expired := false
				if item.expires {
					if t, ok := expires[key]; ok {
						seconds := int((t.Sub(now) / time.Second) + 1)
						if seconds <= 0 {
							expired = true
						}
					}
				}
				if !expired {
					switch v := item.value.(type) {
					default:
						s.mu.RUnlock() // unlock read
						s.mu.Lock()    // lock write on error
						err = errors.New("invalid type in database")
					case string:
						if len(msets) == 0 {
							msets = append(msets, "MSET", key, v)
						} else {
							msets = append(msets, key, v)
						}
						if len(msets) >= 20 {
							writeMultiBulk(wr, msets...)
							msets = nil
						}
					case *list:
						var strs []interface{}
						v.ascend(func(v string) bool {
							if len(strs) == 0 {
								strs = append(strs, "RPUSH", v)
							} else {
								strs = append(strs, v)
							}
							if len(strs) >= 20 {
								writeMultiBulk(wr, strs...)
								strs = nil
							}
							return true
						})
						if len(strs) != 0 {
							writeMultiBulk(wr, strs...)
							strs = nil
						}
					case *set:
						var strs []interface{}
						v.ascend(func(v string) bool {
							if len(strs) == 0 {
								strs = append(strs, "SADD", v)
							} else {
								strs = append(strs, v)
							}
							if len(strs) >= 20 {
								writeMultiBulk(wr, strs...)
								strs = nil
							}
							return true
						})
						if len(strs) != 0 {
							writeMultiBulk(wr, strs...)
							strs = nil
						}
					}
				}
			}
			if len(msets) != 0 {
				writeMultiBulk(wr, msets...)
				msets = nil
			}
			// write expires
			for _, key := range expireKeys {
				t := expires[key]
				seconds := int((t.Sub(now) / time.Second) + 1)
				if seconds > 0 {
					writeMultiBulk(wr, "EXPIRE", key, seconds)
				}
			}
			s.mu.RUnlock()
		}
		err = wr.Flush()
		if err != nil {
			s.mu.Lock() // relock on error
			return
		}
		// time.Sleep(time.Second * 10) // artifical delay
		s.mu.Lock()

		// The base aof has been rewritten. There may have been new aof
		// commands since the start of the rewrite. Let's find out!
		// First flush the current aof.
		s.flushAOF()

		// Then skip to the position in the live aof.
		var cf *os.File
		cf, err = os.Open(s.aofPath)
		if err != nil {
			return
		}
		defer cf.Close()
		var ln int64
		ln, err = cf.Seek(0, 2)
		if err != nil {
			return
		}

		// Write out the new commands that were added since the rewrite began.
		if ln != lastpos {
			if lastdbnum != dbnum {
				writeMultiBulk(wr, "SELECT", lastdbnum)
			}
			err = wr.Flush()
			if err != nil {
				return
			}
			_, err = cf.Seek(lastpos, 0)
			if err != nil {
				return
			}
			_, err = io.Copy(f, cf)
			if err != nil {
				return
			}
			s.lnoticef("Residual parent diff successfully flushed to the "+
				"rewritten AOF (%0.2f MB)", float64(ln-lastpos)/1024.0/1024.0)
		}

		// Close the all of temp file resources.
		if err = cf.Close(); err != nil {
			return
		}
		if err = f.Close(); err != nil {
			return
		}

		// Finally switch out the aof files, failures here can by sucky
		if err = os.Rename(tempName, s.aofPath); err != nil {
			return
		}

		var nf *os.File
		nf, err = os.OpenFile(s.aofPath, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			s.fatalError(err)
			return
		}
		if _, err = nf.Seek(0, 2); err != nil {
			s.fatalError(err)
			return
		}
		s.aof.Close()
		s.aof = nf

		// We are really really done. Celebrate with a bag of Funyuns!

	}()
	return true
}

// flushAOF flushes the
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
			if _, err := s.aof.WriteString("*2\r\n$6\r\nSELECT\r\n$" + lenstr +
				"\r\n" + selstr + "\r\n"); err != nil {
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
	c.db = s.selectDB(0)
	defer func() {
		s.aofdbnum = c.db.num
	}()
	var read int
	for {
		raw, args, _, err := rd.readCommand()
		if err != nil {
			if err == io.EOF {
				break
			}
			s.lwarningf("%v", err)
			return err
		}
		c.args = args
		c.raw = raw
		commandName := autocase(args[0])
		if cmd, ok := s.cmds[commandName]; ok {
			cmd.funct(c)
		} else {
			return errors.New("unknown command '" + args[0] + "'")
		}
		read++
	}
	s.lnoticef("DB loaded from disk: %.3f seconds",
		float64(time.Now().Sub(start))/float64(time.Second))
	return nil
}
