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

type Reply struct {
	Kind    byte
	Value   []byte
	Integer int64       // parsed integer of Reply.Value
	Values  [][]byte
}

// Pack requests using Redis Unified Protocol
func pack(args ...[]byte) []byte {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "*%d\r\n", len(args))
	for _, arg := range args {
		fmt.Fprintf(&buf, "$%d\r\n%s\r\n", len(arg), arg)
	}
	return buf.Bytes()
}

// Redis unified protocol parser
func Parse(r *bufio.Reader) *Reply {
	reply := &Reply{}
    kind, err := r.ReadByte()
    if err != nil {
        panic(err)
    }
    reply.Kind = kind

	if DEBUG {
		log.Printf("Kind: %q", reply.Kind)
	}
	switch reply.Kind {
	case '+', '-', ':':
		reply.Value = parseLine(r)
		if reply.Kind == ':' { // Integer reply
			reply.Integer, _ = strconv.ParseInt(string(reply.Value), 10, 64)
		}
	case '$':
		reply.Value = parseBulk(r)
	case '*':
		reply.Values = parseMulti(r)
    default:
        panic(errors.New("unexpected kind of reply"))
	}

	if r.Buffered() != 0 {
	    panic(errors.New("Unconsumed bytes in buffer"))
    }
	return reply
}

func parseLine(r *bufio.Reader) []byte {
    var buf bytes.Buffer

	for {
        line, isPrefix, err := r.ReadLine() // trailing CRLF is removed
        if err != nil {
            panic(err)
        } 

        for {
            n, err := buf.Write(line)
            if err != nil {
                panic(err)
            }
            if n == len(line) {
                break
            }
            line = line[n:]
        }

        if isPrefix {   // read partial line
            continue    // continue to read more
        }
        break
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
	cnt, _ := strconv.Atoi(string(line))
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
