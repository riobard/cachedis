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

func (r Redis) Mget(ks ...[]byte) (vs [][]byte, err error) {
	if len(ks) == 0 {
	    return nil, nil
	}

	args := make([][]byte, 1+len(ks))
	args[0] = []byte("MGET")
    copy(args[1:], ks)
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

    return msg.Values, nil
}


type KVPair struct {
    K []byte
    V []byte
}


func (r Redis) Mset(pairs ...KVPair) (err error) {
    if len(pairs) == 0 {
        return nil
    }

	args := make([][]byte, 1+2*len(pairs))
	args[0] = []byte("MSET")
	i := 1
	for _, pair := range pairs {
		args[i], args[i+1] = pair.K, pair.V
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

func (r Redis) Set(k []byte, v []byte) (err error) {
	args := [][]byte{[]byte("SET"), k, v}
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

func (r Redis) Get(k []byte) (v []byte, err error) {
	args := [][]byte{[]byte("GET"), k}
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

func (r Redis) Del(ks ...[]byte) (n int, err error) {
    if len(ks) == 0 {
        return 0, nil
    }

	args := make([][]byte, 1+len(ks))
	args[0] = []byte("DEL")
    copy(args[1:], ks)

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
