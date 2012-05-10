/* A minimal Redis client 

This client supports just the commands needed to implement bsic sharding. 
*/

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
	Integer int64       // parsed integer of Reply.Value
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

func (r Redis) Mget(ks ...string) (m map[string][]byte, err error) {
    defer func() {
        if e := recover(); e != nil {
            m = nil
            err = e.(error)
        }
    }()

	m = make(map[string][]byte, len(ks))
	if len(ks) == 0 {
	    return m, nil
	}

	args := make([][]byte, 1+len(ks))
	args[0] = []byte("MGET")
	for i, k := range ks {
		args[i+1] = []byte(k)
	}
	if err = r.send(args...); err != nil {
	    return nil, err
	}

	reply := r.parse()
	if reply.Kind != '*' {
	    return nil, errors.New("unexpected kind of reply")
	}

    for i, v := range reply.Values {
        if v != nil {
            m[ks[i]] = v
        }
    }
    return m, nil
}


func (r Redis) Mset(m map[string][]byte) (err error) {
    defer func() {
        if e := recover(); e != nil {
            err = e.(error)
        }
    }()

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
	if err = r.send(args...); err != nil {
	    return err
	}

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

func (r Redis) Set(k string, value []byte) (err error) {
    defer func() {
        if e := recover(); e != nil {
            err = e.(error)
        }
    }()

	args := [][]byte{[]byte("SET"), []byte(k), value}
	if err = r.send(args...); err != nil {
	    return err
	}

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

func (r Redis) Get(k string) (v []byte, err error) {
    defer func() {
        if e := recover(); e != nil {
            v = nil
            err = e.(error)
        }
    }()

	args := [][]byte{[]byte("GET"), []byte(k)}
    if err = r.send(args...); err != nil {
        return nil, err
    }

	reply := r.parse()
	if DEBUG {
		log.Printf(">>>len = %d", len(reply.Value))
	}
	return reply.Value, nil
}

func (r Redis) Del(ks... string) (n int, err error) {
    defer func() {
        if e := recover(); e != nil {
            n = -1
            err = e.(error)
        }
    }()

    if len(ks) == 0 {
        return 0, nil
    }

	args := make([][]byte, 1+len(ks))
	args[0] = []byte("DEL")
    for i, k := range ks {
        args[i+1] = []byte(k)
    }
    if err = r.send(args...); err != nil {
        return -1, err
    }

	reply := r.parse()
	if DEBUG {
		log.Printf(">>> %d keys deleted", reply.Integer)
	}
	return int(reply.Integer), nil
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

func (r Redis) send(args ...[]byte) error {
	packed := pack(args...)
	if DEBUG {
		log.Printf("REQUEST> %q\n", packed)
	}

	// write all bytes
	for {
        n, err := r.conn.Write(packed)
        if err != nil {
            return err
        } else if n == len(packed) {
            break
        }
        packed = packed[n:]
    }
    return nil
}


// Parse replies using Redis Unified Protocol
func (r Redis) parse() *Reply {
	reply := &Reply{}
    kind, err := r.r.ReadByte()
    if err != nil {
        panic(err)
    }
    reply.Kind = kind

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
    var buf bytes.Buffer

	for {
        line, isPrefix, err := r.r.ReadLine() // trailing CRLF is removed
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
		kind, err := r.r.ReadByte()
		if err != nil {
		    panic(err)
		}
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
