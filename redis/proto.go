package redis

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
)

var (
	DEBUG = false
)

type Message struct {
	Kind    byte
	Value   []byte
	Integer int64       // parsed integer of msg.Value
	Values  [][]byte
}

func Encode(m *Message) ([]byte, error) {
    var buf bytes.Buffer

    switch m.Kind {
    case '+', '-':
        fmt.Fprintf(&buf, "%c%s\r\n", m.Kind, m.Value)
    case ':':
        fmt.Fprintf(&buf, ":%d\r\n", m.Integer)
    case '$':
        if m.Value == nil {
            fmt.Fprintf(&buf, "$-1\r\n")
        } else {
            fmt.Fprintf(&buf, "$%d\r\n%s\r\n", len(m.Value), m.Value)
        }
    case '*':
        fmt.Fprintf(&buf, "*%d\r\n", len(m.Values))
        for _, v := range m.Values {
            if v == nil {
                fmt.Fprintf(&buf, "$-1\r\n")
            } else {
                fmt.Fprintf(&buf, "$%d\r\n%s\r\n", len(v), v)
            }
        }
    default:
        return nil, errors.New("unexpected message type")
    }

    return buf.Bytes(), nil
}


// Redis unified protocol parser
func Parse(r *bufio.Reader) (m *Message, err error) {
    defer func() {
        if e := recover(); e != nil {
            m = nil
            err = e.(error)
        }
    }()

	m = &Message{}

    kind, err := r.ReadByte()
    if err != nil {
        panic(err)
    }

    m.Kind = kind

	if DEBUG {
		log.Printf("Kind: %q", m.Kind)
	}
	switch m.Kind {
	case '+', '-', ':':
		m.Value = parseLine(r)
		if m.Kind == ':' { // Integer msg
			m.Integer, err = strconv.ParseInt(string(m.Value), 10, 64)
			if err != nil {
			    panic(err)
			}
		}
	case '$':
		m.Value = parseBulk(r)
	case '*':
		m.Values = parseMulti(r)
    default:
        panic(errors.New("unexpected kind of msg"))
	}

	if r.Buffered() != 0 {
	    panic(errors.New("Unconsumed bytes in buffer"))
    }
	return m, nil
}

func parseLine(r *bufio.Reader) []byte {
    var buf bytes.Buffer

	for {
        line, isPrefix, err := r.ReadLine() // trailing CRLF is removed
        if err != nil {
            panic(err)
        } 

        for len(line) > 0 {
            n, err := buf.Write(line)
            if err != nil {
                panic(err)
            }
            line = line[n:]
        }
        if !isPrefix {  // whole line is read
            break
        }
    }

	if DEBUG {
		log.Printf("Single Line: %q", buf.Bytes())
	}
	return buf.Bytes()
}

func parseBulk(r *bufio.Reader) []byte {
	line := parseLine(r)
	l, _ := strconv.Atoi(string(line))
	if l >= 0 {
		read := make([]byte, l+2) // trailing CRLF
        n, err := io.ReadFull(r, read)
        if err != nil {
            panic(err)
        }
        if n != l+2 {
            panic(errors.New("partial bulk read"))
        }
        if read[l] != '\r' || read[l+1] != '\n' {
            panic(errors.New("missing trailing CRLF"))
        }
		return read[:l]
	}
	return nil
}

func parseMulti(r *bufio.Reader) [][]byte {
	line := parseLine(r)
	cnt, err := strconv.Atoi(string(line))
	if err != nil {
	    panic(err)
	}
	res := make([][]byte, cnt)
	v := []byte{}
	for j := 0; j < cnt; j++ {
		kind, err := r.ReadByte()
		if err != nil {
		    panic(err)
		}
		if DEBUG {
			log.Printf("Kind: %q", kind)
		}
		if kind == '$' {
			v = parseBulk(r)
			res[j] = v
			if DEBUG {
				if v == nil {
					log.Printf("parseBulk> result nil")
				} else {
					log.Printf("parseBulk> result len(%d)", len(v))
				}
			}
		}
	}
	return res
}
