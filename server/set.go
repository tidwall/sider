package server

type Set map[string]bool

func NewSet() Set {
	s := Set(make(map[string]bool))
	return s
}

func (s Set) Add(member string) bool {
	if !s[member] {
		s[member] = true
		return true
	}
	return false
}

func (s1 Set) Diff(s2 Set) Set {
	s3 := NewSet()
	for v1 := range s1 {
		found := false
		for v2 := range s2 {
			if v1 == v2 {
				found = true
				break
			}
		}
		if !found {
			s3[v1] = true
		}
	}
	return s3
}

func (s1 Set) Inter(s2 Set) Set {
	s3 := NewSet()
	for v1 := range s1 {
		found := false
		for v2 := range s2 {
			if v1 == v2 {
				found = true
				break
			}
		}
		if found {
			s3[v1] = true
		}
	}
	return s3
}

func (s Set) IsMember(member string) bool {
	return s[member]
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
	if st.Add(client.args[2]) {
		client.ReplyInt(1)
		client.dirty++
	} else {
		client.ReplyInt(0)
	}
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
	client.ReplyInt(len(st))
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
	client.ReplyMultiBulkLen(len(st))
	for member := range st {
		client.ReplyBulk(member)
	}
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
	if st[client.args[2]] {
		client.ReplyInt(1)
	} else {
		client.ReplyInt(0)
	}
}
