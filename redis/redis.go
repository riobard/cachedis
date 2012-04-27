package redis

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
)

var (
	DEBUG = false
)

type Redis struct {
	Addr string
	conn net.Conn
	r    *bufio.Reader
}

type Reply struct {
	Kind    byte
	Value   []byte
	Integer int64       // int64 value of Value
	Values  [][]byte
}

func Open(addr string) (*Redis, error) {
	if addr == "" {
		addr = "localhost:6379"
	}
	r := &Redis{Addr: addr}
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	r.conn = conn
	r.r = bufio.NewReader(conn)
	return r, nil
}

func (r Redis) Mget(keys ...string) map[string][]byte {
	m := make(map[string][]byte, len(keys))
	if len(keys) == 0 {
	    return m
	}

	args := make([][]byte, 1+len(keys))
	args[0] = []byte("MGET")
	for i, k := range keys {
		args[i+1] = []byte(k)
	}
	r.send(args...)

	reply := r.parse()
	if reply.Kind == '*' {
		for i, v := range reply.Values {
			if v != nil {
				m[keys[i]] = v
			}
		}
	} else {
	    panic("unexpected kind of reply")
	}
	return m
}


func (r Redis) Mset(m map[string][]byte) error {
    if len(m) == 0 {
        return nil
    }
	args := make([][]byte, 1+2*len(m))
	args[0] = []byte("MSET")
	i := 1
	for k, v := range m {
		args[i], args[i+1] = []byte(k), v
		i += 2
	}
	r.send(args...)
	reply := r.parse()
    s := string(reply.Value)
	if DEBUG {
		log.Printf("%q\n", s)
	}
	if s == "OK" {
        return nil
    }
    return errors.New(s)
}

func (r Redis) Set(key string, value []byte) error {
	args := [][]byte{[]byte("SET"), []byte(key), value}
	r.send(args...)
	reply := r.parse()
	if DEBUG {
		log.Printf(">>>%q", reply.Value)
	}
    s := string(reply.Value)
	if s == "OK" {
        return nil
	}
    return errors.New(s)
}

func (r Redis) Get(key string) []byte {
	args := [][]byte{[]byte("GET"), []byte(key)}
    r.send(args...)
	reply := r.parse()
	if DEBUG {
		log.Printf(">>>len = %d", len(reply.Value))
	}
	return reply.Value
}

func (r Redis) Del(keys... string) int64 {
    if len(keys) == 0 {
        return 0
    }
	args := make([][]byte, 1 + len(keys))
	args[0] = []byte("DEL")
    for i, k := range keys {
        args[i+1] = []byte(k)
    }
    r.send(args...)
	reply := r.parse()
	if DEBUG {
		log.Printf(">>> %d keys deleted", reply.Integer)
	}
	return reply.Integer
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

func (r Redis) send(args ...[]byte) {
	packed := pack(args...)
	if DEBUG {
		log.Printf("REQUEST> %q\n", packed)
	}
	r.conn.Write(packed)
}


// Parse replies using Redis Unified Protocol
func (r Redis) parse() *Reply {
	reply := &Reply{}
	reply.Kind, _ = r.r.ReadByte()
	if DEBUG {
		log.Printf("Kind: %q", reply.Kind)
	}
	switch reply.Kind {
	case '+', '-', ':':
		reply.Value = r.parseLine()
		if reply.Kind == ':' { // Integer reply
			reply.Integer, _ = strconv.ParseInt(string(reply.Value), 10, 64)
		}
	case '$':
		reply.Value = r.parseBulk()
	case '*':
		reply.Values = r.parseMulti()
    default:
        panic("unexpected kind of reply")
	}

	if r.r.Buffered() != 0 {
	    panic("more bytes in buffer")
    }
	return reply
}

func (r Redis) parseLine() []byte {
	line, _, _ := r.r.ReadLine() // trailing CRLF is removed

	if DEBUG {
		log.Printf("Single Line: %q", line)
	}
	return line
}

func (r Redis) parseBulk() []byte {
	line := r.parseLine()
	l, _ := strconv.Atoi(string(line))
	if l >= 0 {
		read := make([]byte, l+2) // trailing CRLF
        n, err := io.ReadFull(r.r, read)
        if err != nil {
            panic(err)
        }
        if n != l+2 {
            panic("partial bulk read")
        }
        if read[l] != '\r' || read[l+1] != '\n' {
            panic("missing trailing CRLF")
        }
		return read[:l]
	}
	return nil
}

func (r Redis) parseMulti() [][]byte {
	line := r.parseLine()
	cnt, _ := strconv.Atoi(string(line))
	res := make([][]byte, cnt)
	v := []byte{}
	for j := 0; j < cnt; j++ {
		kind, _ := r.r.ReadByte()
		if DEBUG {
			log.Printf("Kind: %q", kind)
		}
		if kind == '$' {
			v = r.parseBulk()
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
