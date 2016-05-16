package server

import "path"

type Pattern struct {
	Value          string
	All            bool
	Glob           bool
	GreaterOrEqual string
	LessThan       string
}

func parsePattern(pattern string) *Pattern {
	if pattern == "*" {
		return &Pattern{Value: "*", All: true, Glob: true}
	}
	if pattern == "" {
		return &Pattern{Value: ""}
	}
	p := &Pattern{
		Value: pattern,
	}
	for i, c := range pattern {
		if c == '[' || c == '*' || c == '?' {
			p.GreaterOrEqual = pattern[:i]
			p.Glob = true
			break
		}
	}
	if !p.Glob {
		p.GreaterOrEqual = pattern
	} else if p.GreaterOrEqual != "" {
		c := p.GreaterOrEqual[len(p.GreaterOrEqual)-1]
		if c == 0xFF {
			p.LessThan = p.GreaterOrEqual + string(0)
		} else {
			p.LessThan = p.GreaterOrEqual[:len(p.GreaterOrEqual)-1] + string(c+1)
		}
	}
	return p
}

func (p *Pattern) Match(s string) bool {
	if p.All {
		return true
	}
	matched, _ := path.Match(p.Value, s)
	return matched
}
