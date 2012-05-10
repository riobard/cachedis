/* A minimal Redis client 

This client supports just the commands needed to implement bsic sharding. 
*/

package redis

import (
	"bufio"
	"errors"
	"log"
	"net"
)

type Redis struct {
	Addr string
	conn net.Conn
	r    *bufio.Reader
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

	reply := Parse(r.r)
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

	reply := Parse(r.r)
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

	reply := Parse(r.r)
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

	reply := Parse(r.r)
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

	reply := Parse(r.r)
	if DEBUG {
		log.Printf(">>> %d keys deleted", reply.Integer)
	}
	return int(reply.Integer), nil
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
