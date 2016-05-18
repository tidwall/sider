package server

import (
	"fmt"
	"strconv"
	"strings"
)

type listItem struct {
	value string
	prev  *listItem
	next  *listItem
}
type list struct {
	count int
	front *listItem
	back  *listItem
}

func newList() *list {
	return &list{}
}

// ridx resolves an index, the input could be a negative number.
// The output is always a valid index.
// The return bool is false when the index could not be resolved.
// An input of -1 is equal to l.len()-1
// When the 'outside' param is true, an index outside the range is accepted.
// Returns the resolved index and a bool indicating that the resolved index is within range.
func (l *list) ridx(idx int, outside bool) (resolvedRange, resolvedAny int, ok bool) {
	var oidx int
	if l.count == 0 {
		return 0, 0, false
	}
	if idx < 0 {
		idx = l.count + idx
		oidx = idx
		if idx < 0 {
			if !outside {
				return 0, 0, false
			} else {
				idx = 0
			}
		}
	} else if idx >= l.count {
		oidx = idx
		if !outside {
			return 0, 0, false
		} else {
			idx = l.count - 1
		}
	} else {
		oidx = idx
	}
	return idx, oidx, true
}

func (l *list) lindex(idx int) (value string, ok bool) {
	if idx, _, ok = l.ridx(idx, false); !ok {
		return "", false
	}
	if idx < l.count/2 {
		i := 0
		el := l.front
		for el != nil {
			if i == idx {
				return el.value, true
			}
			el = el.next
			i++
		}
	} else {
		i := l.count - 1
		el := l.back
		for el != nil {
			if i == idx {
				return el.value, true
			}
			el = el.prev
			i--
		}
	}
	return "", false
}

func (l *list) len() int {
	return l.count
}

func (l *list) lpop() (value string, ok bool) {
	if l.count == 0 {
		return "", false
	}
	el := l.front
	l.front = el.next
	if l.front == nil {
		l.back = nil
	} else {
		l.front.prev = nil
	}
	l.count--
	return el.value, true
}

func (l *list) rpop() (value string, ok bool) {
	if l.count == 0 {
		return "", false
	}
	el := l.back
	l.back = el.prev
	if l.back == nil {
		l.front = nil
	} else {
		l.back.next = nil
	}
	l.count--
	return el.value, true
}

func (l *list) lpush(values ...string) int {
	for _, value := range values {
		el := &listItem{value: value}
		if l.front == nil {
			l.back = el
			l.front = el
		} else {
			l.front.prev = el
			el.next = l.front
			l.front = el
		}
	}
	l.count += len(values)
	return l.count
}

func (l *list) rpush(values ...string) int {
	for _, value := range values {
		el := &listItem{value: value}
		if l.back == nil {
			l.back = el
			l.front = el
		} else {
			l.back.next = el
			el.prev = l.back
			l.back = el
		}
	}
	l.count += len(values)
	return l.count
}

func (l *list) set(idx int, value string) bool {
	var ok bool
	if idx, _, ok = l.ridx(idx, false); !ok {
		return false
	}
	if idx < l.count/2 {
		i := 0
		el := l.front
		for el != nil {
			if i == idx {
				el.value = value
				return true
			}
			el = el.next
			i++
		}
	} else {
		i := l.count - 1
		el := l.back
		for el != nil {
			if i == idx {
				el.value = value
				return true
			}
			el = el.prev
			i--
		}
	}
	return false
}

func (l *list) rem(count int, value string) int {
	if count < 0 {
		return 0
	}
	n := 0
	el := l.front
	for el != nil {
		if n == count {
			break
		}
		nel := el.next
		if el.value == value {
			if el.prev == nil {
				if el.next == nil {
					l.front, l.back = nil, nil
				} else {
					el.next.prev = nil
					l.front = el.next
				}
			} else if el.next == nil {
				el.prev.next = nil
				l.back = el.prev
			} else {
				el.prev.next = el.next
				el.next.prev = el.prev
			}
			l.count--
			n++
		}
		el = nel
	}
	return n
}

func (l *list) ascend(iterator func(value string) bool) {
	el := l.front
	for el != nil {
		if !iterator(el.value) {
			return
		}
		el = el.next
	}
}

func (l *list) lrange(start, stop int, count func(n int), iterator func(value string) bool) {
	var ok bool
	if start, _, ok = l.ridx(start, true); !ok {
		count(0)
		return
	}
	if stop, _, ok = l.ridx(stop, true); !ok {
		count(0)
		return
	}
	if start > stop {
		count(0)
		return
	}
	n := stop - start + 1
	count(n)
	var el *listItem
	if start < l.count/2 {
		i := 0
		el = l.front
		for el != nil {
			if i == start {
				break
			}
			el = el.next
			i++
		}
	} else {
		i := l.count - 1
		el = l.back
		for el != nil {
			if i == start {
				break
			}
			el = el.prev
			i--
		}
	}
	i := 0
	for el != nil {
		if i == n {
			break
		}
		if !iterator(el.value) {
			return
		}
		el = el.next
		i++
	}
}

func (l *list) clear() {
	l.front = nil
	l.back = nil
	l.count = 0
}

func (l *list) findel(idx int) *listItem {
	if idx < l.count/2 {
		i := 0
		el := l.front
		for el != nil {
			if i == idx {
				return el
			}
			el = el.next
			i++
		}
	} else {
		i := l.count - 1
		el := l.back
		for el != nil {
			if i == idx {
				return el
			}
			el = el.prev
			i--
		}
	}
	return nil
}

func (l *list) strArr() []string {
	i := 0
	arr := make([]string, l.count)
	el := l.front
	for el != nil {
		arr[i] = el.value
		el = el.next
		i++
	}
	return arr
}

func (l *list) numArr() []float64 {
	i := 0
	arr := make([]float64, l.count)
	el := l.front
	for el != nil {
		n, err := strconv.ParseFloat(el.value, 64)
		if err != nil {
			return nil
		}
		arr[i] = n
		el = el.next
		i++
	}
	return arr
}

func (l *list) trim(start, stop int) {
	var ok bool
	var ostart, ostop int
	if start, ostart, ok = l.ridx(start, true); !ok {
		l.clear()
		return
	}
	if stop, ostop, ok = l.ridx(stop, true); !ok {
		l.clear()
		return
	}

	if ostart > ostop ||
		((ostart < 0 || ostart >= l.count) && (ostop < 0 || ostop >= l.count)) {
		l.clear()
		return
	}
	n := stop - start + 1
	if n == l.count {
		// nothing to trim
		return
	}

	// find the start element
	startEl := l.findel(start)
	stopEl := l.findel(stop)

	l.front = startEl
	l.front.prev = nil
	l.back = stopEl
	l.back.next = nil

	l.count = n
}

func (l *list) String() string {
	s := ""
	el := l.front
	for el != nil {
		s += fmt.Sprintf("%v ", el.value)
		el = el.next
	}
	return strings.TrimSpace(s)
}

/* commands */
func lpushCommand(c *client) {
	if len(c.args) < 3 {
		c.replyAritryError()
		return
	}
	l, ok := c.db.getList(c.args[1], true)
	if !ok {
		c.replyTypeError()
		return
	}
	l.lpush(c.args[2:]...)
	c.replyInt(l.len())
	c.dirty++
}

func rpushCommand(c *client) {
	if len(c.args) < 3 {
		c.replyAritryError()
		return
	}
	l, ok := c.db.getList(c.args[1], true)
	if !ok {
		c.replyTypeError()
		return
	}
	l.rpush(c.args[2:]...)
	c.replyInt(l.len())
	c.dirty++
}

func lrangeCommand(c *client) {
	if len(c.args) != 4 {
		c.replyAritryError()
		return
	}
	start, err := strconv.ParseInt(c.args[2], 10, 64)
	if err != nil {
		c.replyInvalidIntError()
		return
	}
	stop, err := strconv.ParseInt(c.args[3], 10, 64)
	if err != nil {
		c.replyInvalidIntError()
		return
	}

	l, ok := c.db.getList(c.args[1], false)
	if !ok {
		c.replyTypeError()
		return
	}
	if l == nil {
		c.replyMultiBulkLen(0)
		return
	}
	l.lrange(int(start), int(stop), func(n int) {
		c.replyMultiBulkLen(n)
	}, func(value string) bool {
		c.replyBulk(value)
		return true
	})
}

func llenCommand(c *client) {
	if len(c.args) != 2 {
		c.replyAritryError()
		return
	}
	l, ok := c.db.getList(c.args[1], false)
	if !ok {
		c.replyTypeError()
		return
	}
	if l == nil {
		c.replyInt(0)
		return
	}
	c.replyInt(l.len())
}

func lpopCommand(c *client) {
	if len(c.args) != 2 {
		c.replyAritryError()
		return
	}
	l, ok := c.db.getList(c.args[1], false)
	if !ok {
		c.replyTypeError()
		return
	}
	if l == nil {
		c.replyNull()
		return
	}
	value, ok := l.lpop()
	if !ok {
		c.replyNull()
		return
	}
	if l.len() == 0 {
		c.db.del(c.args[1])
	}
	c.replyBulk(value)
	c.dirty++

}

func rpopCommand(c *client) {
	if len(c.args) != 2 {
		c.replyAritryError()
		return
	}
	l, ok := c.db.getList(c.args[1], false)
	if !ok {
		c.replyTypeError()
		return
	}
	if l == nil {
		c.replyNull()
		return
	}
	value, ok := l.rpop()
	if !ok {
		c.replyNull()
		return
	}
	if l.len() == 0 {
		c.db.del(c.args[1])
	}
	c.replyBulk(value)
	c.dirty++
}

func lindexCommand(c *client) {
	if len(c.args) != 3 {
		c.replyAritryError()
		return
	}
	idx, err := strconv.ParseInt(c.args[2], 10, 64)
	if err != nil {
		c.replyInvalidIntError()
		return
	}
	l, ok := c.db.getList(c.args[1], false)
	if !ok {
		c.replyTypeError()
		return
	}
	if l == nil {
		c.replyNull()
		return
	}
	value, ok := l.lindex(int(idx))
	if !ok {
		c.replyNull()
		return
	}
	c.replyBulk(value)
}

func lremCommand(c *client) {
	if len(c.args) != 4 {
		c.replyAritryError()
		return
	}
	count, err := strconv.ParseInt(c.args[2], 10, 64)
	if err != nil {
		c.replyInvalidIntError()
		return
	}
	l, ok := c.db.getList(c.args[1], false)
	if !ok {
		c.replyTypeError()
		return
	}
	if l == nil {
		c.replyInt(0)
		return
	}
	n := l.rem(int(count), c.args[3])
	if l.len() == 0 {
		c.db.del(c.args[1])
	}
	c.dirty += n
	c.replyInt(n)
}

func lsetCommand(c *client) {
	if len(c.args) != 4 {
		c.replyAritryError()
		return
	}
	idx, err := strconv.ParseInt(c.args[2], 10, 64)
	if err != nil {
		c.replyInvalidIntError()
		return
	}
	l, ok := c.db.getList(c.args[1], false)
	if !ok {
		c.replyTypeError()
		return
	}
	if l == nil {
		c.replyNoSuchKeyError()
		return
	}
	ok = l.set(int(idx), c.args[3])
	if !ok {
		c.replyError("index out of range")
		return
	}
	c.replyString("OK")
	c.dirty++
}

func ltrimCommand(c *client) {
	if len(c.args) != 4 {
		c.replyAritryError()
		return
	}
	start, err := strconv.ParseInt(c.args[2], 10, 64)
	if err != nil {
		c.replyInvalidIntError()
		return
	}
	stop, err := strconv.ParseInt(c.args[3], 10, 64)
	if err != nil {
		c.replyInvalidIntError()
		return
	}

	l, ok := c.db.getList(c.args[1], false)
	if !ok {
		c.replyTypeError()
		return
	}
	if l == nil {
		c.replyString("OK")
		return
	}

	llen := l.len()
	l.trim(int(start), int(stop))
	if llen != l.len() {
		c.dirty++
	}
	if l.len() == 0 {
		c.db.del(c.args[1])
	}
	c.replyString("OK")
}
