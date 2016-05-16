package server

import "path"

type pattern struct {
	value          string
	all            bool
	glob           bool
	greaterOrEqual string
	lessThan       string
}

func parsePattern(value string) *pattern {
	if value == "*" {
		return &pattern{value: "*", all: true, glob: true}
	}
	if value == "" {
		return &pattern{value: ""}
	}
	p := &pattern{
		value: value,
	}
	for i, c := range value {
		if c == '[' || c == '*' || c == '?' {
			p.greaterOrEqual = value[:i]
			p.glob = true
			break
		}
	}
	if !p.glob {
		p.greaterOrEqual = value
	} else if p.greaterOrEqual != "" {
		c := p.greaterOrEqual[len(p.greaterOrEqual)-1]
		if c == 0xFF {
			p.lessThan = p.greaterOrEqual + string(0)
		} else {
			p.lessThan = p.greaterOrEqual[:len(p.greaterOrEqual)-1] + string(c+1)
		}
	}
	return p
}

func (p *pattern) match(s string) bool {
	if p.all {
		return true
	}
	matched, _ := path.Match(p.value, s)
	return matched
}
