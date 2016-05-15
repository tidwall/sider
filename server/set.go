package server

import "strconv"

type Set struct {
	m map[string]bool
}

func NewSet() *Set {
	s := &Set{make(map[string]bool)}
	return s
}

func (s *Set) Add(member string) bool {
	if !s.m[member] {
		s.m[member] = true
		return true
	}
	return false
}
func (s *Set) Ascend(iterator func(s string) bool) {
	for v := range s.m {
		if !iterator(v) {
			return
		}
	}
}

func (s1 *Set) Diff(s2 *Set) *Set {
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

func (s1 *Set) Inter(s2 *Set) *Set {
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
func (s *Set) popRand(count int, pop bool) []string {
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
func (s *Set) Pop(count int) []string {
	return s.popRand(count, true)
}
func (s *Set) Rand(count int) []string {
	return s.popRand(count, false)
}

func (s *Set) IsMember(member string) bool {
	return s.m[member]
}

func (s *Set) Len() int {
	return len(s.m)
}

func saddCommand(client *Client) {
	if len(client.args) < 3 {
		client.ReplyAritryError()
		return
	}
	st, ok := client.server.GetKeySet(client.args[1], true)
	if !ok {
		client.ReplyTypeError()
		return
	}
	count := 0
	for i := 2; i < len(client.args); i++ {
		if st.Add(client.args[i]) {
			client.dirty++
			count++
		}
	}
	client.ReplyInt(count)

}

func scardCommand(client *Client) {
	if len(client.args) != 2 {
		client.ReplyAritryError()
		return
	}
	st, ok := client.server.GetKeySet(client.args[1], false)
	if !ok {
		client.ReplyTypeError()
		return
	}
	if st == nil {
		client.ReplyInt(0)
		return
	}
	client.ReplyInt(st.Len())
}
func smembersCommand(client *Client) {
	if len(client.args) != 2 {
		client.ReplyAritryError()
		return
	}
	st, ok := client.server.GetKeySet(client.args[1], false)
	if !ok {
		client.ReplyTypeError()
		return
	}
	if st == nil {
		client.ReplyMultiBulkLen(0)
		return
	}
	client.ReplyMultiBulkLen(st.Len())
	st.Ascend(func(s string) bool {
		client.ReplyBulk(s)
		return true
	})
}
func sismembersCommand(client *Client) {
	if len(client.args) != 3 {
		client.ReplyAritryError()
		return
	}
	st, ok := client.server.GetKeySet(client.args[1], false)
	if !ok {
		client.ReplyTypeError()
		return
	}
	if st == nil {
		client.ReplyInt(0)
		return
	}
	if st.IsMember(client.args[2]) {
		client.ReplyInt(1)
	} else {
		client.ReplyInt(0)
	}
}

func sdiffinterGenericCommand(client *Client, diff bool) {
	if len(client.args) < 2 {
		client.ReplyAritryError()
		return
	}
	var st *Set
	for i := 1; i < len(client.args); i++ {
		stt, ok := client.server.GetKeySet(client.args[i], false)
		if !ok {
			client.ReplyTypeError()
			return
		}
		if stt == nil {
			if diff {
				continue
			} else {
				st = nil
				break
			}
		}
		if i == 1 {
			st = stt
		} else {
			if diff {
				st = st.Diff(stt)
			} else {
				st = st.Inter(stt)
			}
		}
	}
	if st == nil {
		client.ReplyMultiBulkLen(0)
		return
	}
	client.ReplyMultiBulkLen(st.Len())
	st.Ascend(func(s string) bool {
		client.ReplyBulk(s)
		return true
	})
}
func sdiffCommand(client *Client) {
	sdiffinterGenericCommand(client, true)
}
func sinterCommand(client *Client) {
	sdiffinterGenericCommand(client, false)
}
func spopCommand(client *Client) {
	srandmemberpopGenericCommand(client, true)
}
func srandmemberpopGenericCommand(client *Client, pop bool) {
	if len(client.args) < 2 || len(client.args) > 3 {
		client.ReplyAritryError()
		return
	}
	countSpecified := false
	count := 1
	if len(client.args) > 2 {
		n, err := strconv.ParseInt(client.args[2], 10, 64)
		if err != nil {
			client.ReplyInvalidIntError()
			return
		}
		if pop && n < 0 {
			client.ReplyError("index out of range")
			return
		}
		count = int(n)
		countSpecified = true
	}
	st, ok := client.server.GetKeySet(client.args[1], false)
	if !ok {
		client.ReplyTypeError()
		return
	}
	if st == nil {
		if countSpecified {
			client.ReplyMultiBulkLen(0)
		} else {
			client.ReplyNull()
		}
		return
	}
	var res []string
	if pop {
		res = st.Pop(count)
		client.dirty += len(res)
	} else {
		res = st.Rand(count)
	}
	if countSpecified {
		client.ReplyMultiBulkLen(len(res))
	} else if len(res) == 0 {
		client.ReplyNull()
	}
	for _, s := range res {
		client.ReplyBulk(s)
		if !countSpecified {
			break
		}
	}
	if pop && st.Len() == 0 {
		client.server.DelKey(client.args[1])
	}
}

func srandmemberCommand(client *Client) {
	srandmemberpopGenericCommand(client, false)
}
