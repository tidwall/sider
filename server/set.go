package server

import "strconv"

type setT struct {
	m map[string]bool
}

func NewSet() *setT {
	s := &setT{make(map[string]bool)}
	return s
}

func (s *setT) Add(member string) bool {
	if !s.m[member] {
		s.m[member] = true
		return true
	}
	return false
}

func (s *setT) Del(member string) bool {
	if s.m[member] {
		delete(s.m, member)
		return true
	}
	return false
}

func (s *setT) Ascend(iterator func(s string) bool) {
	for v := range s.m {
		if !iterator(v) {
			return
		}
	}
}

func (s1 *setT) Diff(s2 *setT) *setT {
	s3 := NewSet()
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

func (s1 *setT) Inter(s2 *setT) *setT {
	s3 := NewSet()
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

func (s1 *setT) Union(s2 *setT) *setT {
	s3 := NewSet()
	for v := range s1.m {
		s3.m[v] = true
	}
	for v := range s2.m {
		s3.m[v] = true
	}
	return s3
}

func (s *setT) popRand(count int, pop bool) []string {
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
func (s *setT) Pop(count int) []string {
	return s.popRand(count, true)
}
func (s *setT) Rand(count int) []string {
	return s.popRand(count, false)
}

func (s *setT) IsMember(member string) bool {
	return s.m[member]
}

func (s *setT) Len() int {
	return len(s.m)
}

func saddCommand(c *client) {
	if len(c.args) < 3 {
		c.ReplyAritryError()
		return
	}
	st, ok := c.db.GetSet(c.args[1], true)
	if !ok {
		c.ReplyTypeError()
		return
	}
	count := 0
	for i := 2; i < len(c.args); i++ {
		if st.Add(c.args[i]) {
			c.dirty++
			count++
		}
	}
	c.ReplyInt(count)

}

func scardCommand(c *client) {
	if len(c.args) != 2 {
		c.ReplyAritryError()
		return
	}
	st, ok := c.db.GetSet(c.args[1], false)
	if !ok {
		c.ReplyTypeError()
		return
	}
	if st == nil {
		c.ReplyInt(0)
		return
	}
	c.ReplyInt(st.Len())
}
func smembersCommand(c *client) {
	if len(c.args) != 2 {
		c.ReplyAritryError()
		return
	}
	st, ok := c.db.GetSet(c.args[1], false)
	if !ok {
		c.ReplyTypeError()
		return
	}
	if st == nil {
		c.ReplyMultiBulkLen(0)
		return
	}
	c.ReplyMultiBulkLen(st.Len())
	st.Ascend(func(s string) bool {
		c.ReplyBulk(s)
		return true
	})
}
func sismembersCommand(c *client) {
	if len(c.args) != 3 {
		c.ReplyAritryError()
		return
	}
	st, ok := c.db.GetSet(c.args[1], false)
	if !ok {
		c.ReplyTypeError()
		return
	}
	if st == nil {
		c.ReplyInt(0)
		return
	}
	if st.IsMember(c.args[2]) {
		c.ReplyInt(1)
	} else {
		c.ReplyInt(0)
	}
}

func sdiffinterunionGenericCommand(c *client, diff, union bool, store bool) {
	if (!store && len(c.args) < 2) || (store && len(c.args) < 3) {
		c.ReplyAritryError()
		return
	}
	basei := 1
	if store {
		basei = 2
	}
	var st *setT
	for i := basei; i < len(c.args); i++ {
		stt, ok := c.db.GetSet(c.args[i], false)
		if !ok {
			c.ReplyTypeError()
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
				st = st.Diff(stt)
			} else if union {
				st = st.Union(stt)
			} else {
				st = st.Inter(stt)
			}
		}
	}
	if store {
		if st == nil || st.Len() == 0 {
			_, ok := c.db.Del(c.args[1])
			if ok {
				c.dirty++
			}
			c.ReplyInt(0)
		} else {
			c.db.Set(c.args[1], st)
			c.dirty++
			c.ReplyInt(st.Len())
		}
	} else {
		if st == nil {
			c.ReplyMultiBulkLen(0)
			return
		}
		c.ReplyMultiBulkLen(st.Len())
		st.Ascend(func(s string) bool {
			c.ReplyBulk(s)
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
		c.ReplyAritryError()
		return
	}
	countSpecified := false
	count := 1
	if len(c.args) > 2 {
		n, err := strconv.ParseInt(c.args[2], 10, 64)
		if err != nil {
			c.ReplyInvalidIntError()
			return
		}
		if pop && n < 0 {
			c.ReplyError("index out of range")
			return
		}
		count = int(n)
		countSpecified = true
	}
	st, ok := c.db.GetSet(c.args[1], false)
	if !ok {
		c.ReplyTypeError()
		return
	}
	if st == nil {
		if countSpecified {
			c.ReplyMultiBulkLen(0)
		} else {
			c.ReplyNull()
		}
		return
	}
	var res []string
	if pop {
		res = st.Pop(count)
		c.dirty += len(res)
	} else {
		res = st.Rand(count)
	}
	if countSpecified {
		c.ReplyMultiBulkLen(len(res))
	} else if len(res) == 0 {
		c.ReplyNull()
	}
	for _, s := range res {
		c.ReplyBulk(s)
		if !countSpecified {
			break
		}
	}
	if pop && st.Len() == 0 {
		c.db.Del(c.args[1])
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
		c.ReplyAritryError()
		return
	}
	st, ok := c.db.GetSet(c.args[1], false)
	if !ok {
		c.ReplyTypeError()
		return
	}
	if st == nil {
		c.ReplyInt(0)
		return
	}
	var count int
	for i := 2; i < len(c.args); i++ {
		if st.Del(c.args[i]) {
			count++
			c.dirty++
		}
	}
	if st.Len() == 0 {
		c.db.Del(c.args[1])
	}
	c.ReplyInt(count)
}

func smoveCommand(c *client) {
	if len(c.args) != 4 {
		c.ReplyAritryError()
		return
	}
	src, ok := c.db.GetSet(c.args[1], false)
	if !ok {
		c.ReplyTypeError()
		return
	}
	dst, ok := c.db.GetSet(c.args[2], false)
	if !ok {
		c.ReplyTypeError()
		return
	}
	if src == nil {
		c.ReplyInt(0)
		return
	}
	if !src.Del(c.args[3]) {
		c.ReplyInt(0)
		return
	}
	if dst == nil {
		dst = NewSet()
		dst.Add(c.args[3])
		c.db.Set(c.args[2], dst)
		c.ReplyInt(1)
		c.dirty++
		return
	}
	dst.Add(c.args[3])
	c.ReplyInt(1)
	c.dirty++
}
