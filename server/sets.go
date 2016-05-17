package server

import "strconv"

type set struct {
	m map[string]bool
}

func newSet() *set {
	s := &set{make(map[string]bool)}
	return s
}

func (s *set) add(member string) bool {
	if !s.m[member] {
		s.m[member] = true
		return true
	}
	return false
}

func (s *set) del(member string) bool {
	if s.m[member] {
		delete(s.m, member)
		return true
	}
	return false
}

func (s *set) ascend(iterator func(s string) bool) {
	for v := range s.m {
		if !iterator(v) {
			return
		}
	}
}

func (s1 *set) diff(s2 *set) *set {
	s3 := newSet()
	for v1 := range s1.m {
		found := false
		for v2 := range s2.m {
			if v1 == v2 {
				found = true
				break
			}
		}
		if !found {
			s3.m[v1] = true
		}
	}
	return s3
}

func (s1 *set) inter(s2 *set) *set {
	s3 := newSet()
	for v1 := range s1.m {
		found := false
		for v2 := range s2.m {
			if v1 == v2 {
				found = true
				break
			}
		}
		if found {
			s3.m[v1] = true
		}
	}
	return s3
}

func (s1 *set) union(s2 *set) *set {
	s3 := newSet()
	for v := range s1.m {
		s3.m[v] = true
	}
	for v := range s2.m {
		s3.m[v] = true
	}
	return s3
}

func (s *set) popRand(count int, pop bool) []string {
	many := false
	if count < 0 {
		if pop {
			return nil
		} else {
			count *= -1
			many = true
		}
	}
	var res []string
	if count > 1024 {
		res = make([]string, 0, 1024)
	} else {
		res = make([]string, 0, count)
	}
	for {
		for key := range s.m {
			if count <= 0 {
				break
			}
			if pop {
				delete(s.m, key)
			}
			res = append(res, key)
			count--
		}
		if !many || count == 0 {
			break
		}
	}
	return res
}
func (s *set) pop(count int) []string {
	return s.popRand(count, true)
}
func (s *set) rand(count int) []string {
	return s.popRand(count, false)
}

func (s *set) isMember(member string) bool {
	return s.m[member]
}

func (s *set) len() int {
	return len(s.m)
}

func saddCommand(c *client) {
	if len(c.args) < 3 {
		c.replyAritryError()
		return
	}
	st, ok := c.db.getSet(c.args[1], true)
	if !ok {
		c.replyTypeError()
		return
	}
	count := 0
	for i := 2; i < len(c.args); i++ {
		if st.add(c.args[i]) {
			c.dirty++
			count++
		}
	}
	c.replyInt(count)

}

func scardCommand(c *client) {
	if len(c.args) != 2 {
		c.replyAritryError()
		return
	}
	st, ok := c.db.getSet(c.args[1], false)
	if !ok {
		c.replyTypeError()
		return
	}
	if st == nil {
		c.replyInt(0)
		return
	}
	c.replyInt(st.len())
}
func smembersCommand(c *client) {
	if len(c.args) != 2 {
		c.replyAritryError()
		return
	}
	st, ok := c.db.getSet(c.args[1], false)
	if !ok {
		c.replyTypeError()
		return
	}
	if st == nil {
		c.replyMultiBulkLen(0)
		return
	}
	c.replyMultiBulkLen(st.len())
	st.ascend(func(s string) bool {
		c.replyBulk(s)
		return true
	})
}
func sismembersCommand(c *client) {
	if len(c.args) != 3 {
		c.replyAritryError()
		return
	}
	st, ok := c.db.getSet(c.args[1], false)
	if !ok {
		c.replyTypeError()
		return
	}
	if st == nil {
		c.replyInt(0)
		return
	}
	if st.isMember(c.args[2]) {
		c.replyInt(1)
	} else {
		c.replyInt(0)
	}
}

func sdiffinterunionGenericCommand(c *client, diff, union bool, store bool) {
	if (!store && len(c.args) < 2) || (store && len(c.args) < 3) {
		c.replyAritryError()
		return
	}
	basei := 1
	if store {
		basei = 2
	}
	var st *set
	for i := basei; i < len(c.args); i++ {
		stt, ok := c.db.getSet(c.args[i], false)
		if !ok {
			c.replyTypeError()
			return
		}
		if stt == nil {
			if diff || union {
				continue
			} else {
				st = nil
				break
			}
		}
		if st == nil {
			st = stt
		} else {
			if diff {
				st = st.diff(stt)
			} else if union {
				st = st.union(stt)
			} else {
				st = st.inter(stt)
			}
		}
	}
	if store {
		if st == nil || st.len() == 0 {
			_, ok := c.db.del(c.args[1])
			if ok {
				c.dirty++
			}
			c.replyInt(0)
		} else {
			c.db.set(c.args[1], st)
			c.dirty++
			c.replyInt(st.len())
		}
	} else {
		if st == nil {
			c.replyMultiBulkLen(0)
			return
		}
		c.replyMultiBulkLen(st.len())
		st.ascend(func(s string) bool {
			c.replyBulk(s)
			return true
		})
	}
}
func sdiffCommand(c *client) {
	sdiffinterunionGenericCommand(c, true, false, false)
}
func sinterCommand(c *client) {
	sdiffinterunionGenericCommand(c, false, false, false)
}
func sunionCommand(c *client) {
	sdiffinterunionGenericCommand(c, false, true, false)
}
func sdiffstoreCommand(c *client) {
	sdiffinterunionGenericCommand(c, true, false, true)
}
func sinterstoreCommand(c *client) {
	sdiffinterunionGenericCommand(c, false, false, true)
}
func sunionstoreCommand(c *client) {
	sdiffinterunionGenericCommand(c, false, true, true)
}

func srandmemberpopGenericCommand(c *client, pop bool) {
	if len(c.args) < 2 || len(c.args) > 3 {
		c.replyAritryError()
		return
	}
	countSpecified := false
	count := 1
	if len(c.args) > 2 {
		n, err := strconv.ParseInt(c.args[2], 10, 64)
		if err != nil {
			c.replyInvalidIntError()
			return
		}
		if pop && n < 0 {
			c.replyError("index out of range")
			return
		}
		count = int(n)
		countSpecified = true
	}
	st, ok := c.db.getSet(c.args[1], false)
	if !ok {
		c.replyTypeError()
		return
	}
	if st == nil {
		if countSpecified {
			c.replyMultiBulkLen(0)
		} else {
			c.replyNull()
		}
		return
	}
	var res []string
	if pop {
		res = st.pop(count)
		c.dirty += len(res)
	} else {
		res = st.rand(count)
	}
	if countSpecified {
		c.replyMultiBulkLen(len(res))
	} else if len(res) == 0 {
		c.replyNull()
	}
	for _, s := range res {
		c.replyBulk(s)
		if !countSpecified {
			break
		}
	}
	if pop && st.len() == 0 {
		c.db.del(c.args[1])
	}
}

func srandmemberCommand(c *client) {
	srandmemberpopGenericCommand(c, false)
}

func spopCommand(c *client) {
	srandmemberpopGenericCommand(c, true)
}

func sremCommand(c *client) {
	if len(c.args) < 3 {
		c.replyAritryError()
		return
	}
	st, ok := c.db.getSet(c.args[1], false)
	if !ok {
		c.replyTypeError()
		return
	}
	if st == nil {
		c.replyInt(0)
		return
	}
	var count int
	for i := 2; i < len(c.args); i++ {
		if st.del(c.args[i]) {
			count++
			c.dirty++
		}
	}
	if st.len() == 0 {
		c.db.del(c.args[1])
	}
	c.replyInt(count)
}

func smoveCommand(c *client) {
	if len(c.args) != 4 {
		c.replyAritryError()
		return
	}
	src, ok := c.db.getSet(c.args[1], false)
	if !ok {
		c.replyTypeError()
		return
	}
	dst, ok := c.db.getSet(c.args[2], false)
	if !ok {
		c.replyTypeError()
		return
	}
	if src == nil {
		c.replyInt(0)
		return
	}
	if !src.del(c.args[3]) {
		c.replyInt(0)
		return
	}
	if dst == nil {
		dst = newSet()
		dst.add(c.args[3])
		c.db.set(c.args[2], dst)
		c.replyInt(1)
		c.dirty++
		return
	}
	dst.add(c.args[3])
	c.replyInt(1)
	c.dirty++
}
