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

// const (
// 	greaterThanEqualTo = 1
// 	lessThanEqualTo    = 2
// )

// // returns the starting pattern greater-than-or-equal-to pivot
// func patternPivot(pattern string) string {
// 	if pattern == "*" {
// 		return ""
// 	}
// 	for i, c := range pattern {
// 		if c == '[' || c == '*' || c == '?' {
// 			return pattern[:i]
// 		}
// 	}
// 	return pattern
// }

// func patternPivotEnd(pattern string, dir int) string {
// 	pivot := patternPivot(pattern)
// 	if pivot == "" {
// 		return ""
// 	}

// 	if pattern == "*" {
// 		return ""
// 	}
// 	for i, c := range pattern {
// 		if c == '[' || c == '*' || c == '?' {
// 			return pattern[:i]
// 		}
// 	}
// 	return pattern
// }

// func patternMatch(pattern, str string) bool {
// 	if pattern == "*" {
// 		return true
// 	}
// 	return path.Match(pattern, str)
// }

// func patternIsPattern(pattern string) bool {
// 	for i := 0; i < len(pattern); i++ {
// 		switch pattern[i] {
// 		case '[', '*', '?':
// 			_, err := globMatch(pattern, "whatever")
// 			return err == nil
// 		}
// 	}
// 	return false
// }

// func patternEnd(pattern string) string {

// }
