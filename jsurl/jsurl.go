// Package jsurl is a port of the jsurl encoding scheme found at
// https://github.com/Sage/jsurl to Go (parsing only).  It is used in the
// Boomerang Errors plugin to encode exceptions.
package jsurl

import (
	"errors"
	"regexp"
	"strconv"
)

// Copyright retained from jsurl repo:
/**
 * Copyright (c) 2011 Bruno Jouhier <bruno.jouhier@sage.com>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

var reserved = map[string]interface{}{
	"true":  true,
	"false": false,
	"null":  nil,
}

var quoteURLEncoded = regexp.MustCompile(`%(25)*27`)
var nonSpecialChar = regexp.MustCompile(`[^)~]`)

// Parse the given JSURL-encoded string into a deserialized value of type
// interface{}.  If the serialized object is a JSON list, the returned value
// will be []interface{}.  If it is a JSON object, the returned value will be
// have an underlying type of map[string]interface{}.  All JSON numbers are
// deserialized as float64, in keeping with the convention of the Go json
// deserializer.
//
// See https://github.com/Sage/jsurl for an explanation of JSURL.
func Parse(in string) (interface{}, error) {
	s := []rune(in)
	if len(s) == 0 {
		return nil, nil
	}

	s = []rune(quoteURLEncoded.ReplaceAllString(string(s), "'"))
	i := 0

	eat := func(expected rune) error {
		if i >= len(s) {
			return errors.New("exceeded string boundary")
		}
		if s[i] != expected {
			return errors.New("bad JSURL syntax: expected " + string(expected) + ", got " + string(s[i]))
		}
		i++
		return nil
	}

	decode := func() ([]rune, error) {
		beg := i
		ch := s[i]
		r := make([]rune, 0)
		for i < len(s) && ch != '~' && ch != ')' {
			switch ch {
			case '*':
				if beg < i {
					r = append(r, s[beg:i]...)
				}

				if s[i+1] == '*' {
					code, err := strconv.ParseInt(string(s[i+2:i+6]), 16, 64)
					if err != nil {
						return nil, err
					}
					r = append(r, rune(code))
					i += 6
					beg = i
				} else {
					code, err := strconv.ParseInt(string(s[i+1:i+3]), 16, 64)
					if err != nil {
						return nil, err
					}
					r = append(r, rune(code))
					i += 3
					beg = i
				}
			case '!':
				if beg < i {
					r = append(r, s[beg:i]...)
				}
				r = append(r, '$')
				i++
				beg = i
			default:
				i++
			}
			if i >= len(s) {
				break
			}
			ch = s[i]
		}
		return append(r, s[beg:i]...), nil
	}

	var parseOne func() (interface{}, error)
	parseOne = func() (interface{}, error) {
		var result interface{}
		var beg int

		if err := eat('~'); err != nil {
			return nil, err
		}

		if i >= len(s) {
			return nil, errors.New("unexpected end of string")
		}

		ch := s[i]
		switch ch {
		case '(':
			i++
			if s[i] == '~' {
				result = make([]interface{}, 0)
				if i < len(s)-1 && s[i+1] == ')' {
					i++
				} else {
					for {
						parsed, err := parseOne()
						if err != nil {
							return nil, err
						}
						result = append(result.([]interface{}), parsed)
						if i >= len(s) || s[i] != '~' {
							break
						}
					}
				}
			} else {
				result = make(map[string]interface{})
				if s[i] != ')' {
					for {
						key, err := decode()
						if err != nil {
							return nil, err
						}
						parsed, err := parseOne()
						if err != nil {
							return nil, err
						}
						result.(map[string]interface{})[string(key)] = parsed
						if i >= len(s) || s[i] != '~' {
							break
						}
						i++
					}
				}
			}
			err := eat(')')
			if err != nil {
				return nil, err
			}
		case '\'':
			i++
			var err error
			result, err = decode()
			if err != nil {
				return nil, err
			}
			result = string(result.([]rune))
		default:
			beg = i
			i++
			for i < len(s) && nonSpecialChar.MatchString(string(s[i:i+1])) {
				i++
			}
			sub := s[beg:i]
			if asFloat, err := strconv.ParseFloat(string(sub), 64); err == nil {
				result = asFloat
			} else {
				var ok bool
				result, ok = reserved[string(sub)]
				if !ok {
					return nil, errors.New("bad value keyword: " + string(sub))
				}
			}
		}
		return result, nil
	}

	return parseOne()
}
