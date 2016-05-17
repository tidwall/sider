package server

import "testing"

func makeSimpleList(t testing.TB) *list {
	l := newList()
	l.rpush("1")
	l.rpush("2")
	l.rpush("3")
	l.rpush("4")
	l.lpush("a")
	l.lpush("b")
	l.lpush("c")
	l.lpush("d")
	l.rpush("a", "b", "c", "d")
	l.lpush("1", "2", "3", "4")
	//              - 6 5 4 3 2 1 0 9 8 7 6 5 4 3 2 1
	//                0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5
	if l.String() != "4 3 2 1 d c b a 1 2 3 4 a b c d" {
		t.Fatal("simple list failure")
	}
	return l
}

func testListValue(t *testing.T, l *list, idx int, expect string, ok bool) {
	value, tok := l.lindex(idx)
	if tok != ok || value != expect {
		t.Fatalf("expected value='%v', ok='%v', got value='%v', ok='%v'", expect, ok, value, tok)
	}
}

func testListLen(t *testing.T, l *list, expect int) {
	if l.len() != expect {
		t.Fatalf("expected %v, got %v", expect, l.len())
	}
}

func testListLpop(t *testing.T, l *list, expect string, ok bool) {
	value, tok := l.lpop()
	if tok != ok || value != expect {
		t.Fatalf("expected value='%v', ok='%v', got value='%v', ok='%v'", expect, ok, value, tok)
	}
}
func testListRpop(t *testing.T, l *list, expect string, ok bool) {
	value, tok := l.rpop()
	if tok != ok || value != expect {
		t.Fatalf("expected value='%v', ok='%v', got value='%v', ok='%v'", expect, ok, value, tok)
	}
}

func testListSet(t *testing.T, l *list, idx int, value string, ok bool) {
	got := l.set(idx, value)
	if got != ok {
		t.Fatalf("expected '%v', got '%v'", ok, got)
	}
}

func testListRem(t *testing.T, l *list, count int, value string, expect int) {
	got := l.rem(count, value)
	if got != expect {
		t.Fatalf("expected '%v', got '%v'", expect, got)
	}
}

func testListString(t *testing.T, l *list, expect string) {
	got := l.String()
	if got != expect {
		t.Fatalf("expected '%v', got '%v'", expect, got)
	}
}

func TestList(t *testing.T) {
	l := makeSimpleList(t)
	testListValue(t, l, 0, "4", true)
	testListValue(t, l, 1, "3", true)
	testListValue(t, l, 7, "a", true)
	testListValue(t, l, 8, "1", true)
	testListValue(t, l, 9, "2", true)
	testListValue(t, l, 15, "d", true)
	testListValue(t, l, 16, "", false)
	testListValue(t, l, -1, "d", true)
	testListValue(t, l, -7, "2", true)
	testListValue(t, l, -8, "1", true)
	testListValue(t, l, -9, "a", true)
	testListValue(t, l, -15, "3", true)
	testListValue(t, l, -16, "4", true)
	testListValue(t, l, -17, "", false)

	testListString(t, l, "4 3 2 1 d c b a 1 2 3 4 a b c d")

	testListLen(t, l, 16)
	testListLpop(t, l, "4", true)
	testListLen(t, l, 15)
	testListLpop(t, l, "3", true)
	testListLen(t, l, 14)
	testListRpop(t, l, "d", true)
	testListLen(t, l, 13)
	testListRpop(t, l, "c", true)
	testListLen(t, l, 12)

	testListString(t, l, "2 1 d c b a 1 2 3 4 a b")

	testListSet(t, l, 0, "3", true)
	testListSet(t, l, 1, "4", true)
	testListSet(t, l, 2, "5", true)
	testListSet(t, l, -1, "Z", true)
	testListSet(t, l, 12, "?", false)
	testListSet(t, l, -12, "A", true)
	testListSet(t, l, -13, "?", false)

	testListString(t, l, "A 4 5 c b a 1 2 3 4 a Z")

	testListRem(t, l, 3, "a", 2)
	testListRem(t, l, 3, "a", 0)
	testListRem(t, l, 3, "A", 1)
	testListRem(t, l, 3, "Z", 1)
	testListRem(t, l, -1, "2", 0)

	testListString(t, l, "4 5 c b 1 2 3 4")

	nl := newList()
	l.lrange(1, -2, func(n int) {}, func(v string) bool {
		nl.rpush(v)
		return true
	})
	l = nl
	testListString(t, l, "5 c b 1 2 3")

	l.trim(1, -2)
	testListString(t, l, "c b 1 2")
	l.trim(1, -3)
	testListString(t, l, "b")
	l.trim(1, -1)
	testListString(t, l, "")

	l.rpush("1", "2", "3", "4", "5", "6", "7", "8")
	testListString(t, l, "1 2 3 4 5 6 7 8")
	l.trim(-1, 500)
	testListString(t, l, "8")
	l.trim(500, 501)
	testListString(t, l, "")
	l.rpush("1", "2", "3", "4", "5", "6", "7", "8")
	l.trim(-5, -3)
	testListString(t, l, "4 5 6")

	l.clear()
	l.rpush("1", "2", "3", "4", "5", "6", "7", "8")
	l.trim(-12, -8)
	testListString(t, l, "1")

}
