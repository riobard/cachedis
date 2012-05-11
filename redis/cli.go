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

	msg, err := Parse(r.r)
	if err != nil {
	    return nil, err
	}
	if msg.Kind != '*' {
	    return nil, errors.New("unexpected kind of msg")
	}

    for i, v := range msg.Values {
        if v != nil {
            m[ks[i]] = v
        }
    }
    return m, nil
}


func (r Redis) Mset(m map[string][]byte) (err error) {
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

	msg, err := Parse(r.r)
	if err != nil {
	    return err
	}

    s := string(msg.Value)
	if DEBUG {
		log.Printf("%q\n", s)
	}
	if s == "OK" {
        return nil
    }
    return errors.New(s)
}

func (r Redis) Set(k string, value []byte) (err error) {
	args := [][]byte{[]byte("SET"), []byte(k), value}
	if err = r.send(args...); err != nil {
	    return err
	}

	msg, err := Parse(r.r)
	if err != nil {
	    return err
	}

	if DEBUG {
		log.Printf(">>>%q", msg.Value)
	}

    s := string(msg.Value)
	if s == "OK" {
        return nil
	}
    return errors.New(s)
}

func (r Redis) Get(k string) (v []byte, err error) {
	args := [][]byte{[]byte("GET"), []byte(k)}
    if err = r.send(args...); err != nil {
        return nil, err
    }

	msg, err := Parse(r.r)
	if err != nil {
	    return nil, err
	}

	if DEBUG {
		log.Printf(">>>len = %d", len(msg.Value))
	}

	return msg.Value, nil
}

func (r Redis) Del(ks... string) (n int, err error) {
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

	msg, err := Parse(r.r)
	if err != nil {
	    return -1, err
	}

	if DEBUG {
		log.Printf(">>> %d keys deleted", msg.Integer)
	}

	return int(msg.Integer), nil
}

func (r Redis) send(args ...[]byte) error {
    m := &Message{Kind: '*', Values: args}
    b, err := Encode(m)
    if err != nil {
        return err
    }

	if DEBUG {
		log.Printf("REQUEST> %q\n", b)
	}

	for len(b) > 0 {
        n, err := r.conn.Write(b)
        if err != nil {
            return err
        }
        b = b[n:]
    }
    return nil
}
