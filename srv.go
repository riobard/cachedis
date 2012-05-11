package main

import (
	"./redis"
	"./shardis"
	"net"
	"log"
	"bufio"
)


var (
    E_INCOMPLETE  = []byte("Incomplete request")
    E_UNSUPPORTED = []byte("Unsupported request")
    E_PROXY       = []byte("Proxy error")
)


type Proxy struct {
    s *shardis.Shardis
}


func ProxyOpen(shards ...string) (*Proxy, error) {
    s, err := shardis.Open(
        "localhost:6379",
        "localhost:6379",
        "localhost:6379",
        "localhost:6379",
    )
    if err != nil {
        return nil, err
    }
    return &Proxy{s: s}, nil

}


func (p Proxy) proxy(req *redis.Message) (rsp *redis.Message) {
    rsp = &redis.Message{}

    if req.Kind == '*' {
        if len(req.Values) > 1 {
            switch string(req.Values[0]) {
            case "GET":
                p.proxyGet(req, rsp)
            case "SET":
                p.proxySet(req, rsp)
            case "DEL":
                p.proxyDel(req, rsp)
            case "MGET":
                p.proxyMget(req, rsp)
            case "MSET":
                p.proxyMset(req, rsp)
            default:
                rsp.Kind = '-'
                rsp.Value = E_UNSUPPORTED
            }
        } else {
            rsp.Kind = '-'
            rsp.Value = E_INCOMPLETE
        }
    } else {
        rsp.Kind = '-'
        rsp.Value = E_UNSUPPORTED
    }

    return rsp
}


func (p Proxy) proxyGet(req, rsp *redis.Message) {
    b, _ := p.s.Get(string(req.Values[1]))
    rsp.Kind = '$'
    rsp.Value = b
}

func (p Proxy) proxySet(req, rsp *redis.Message) {
    if len(req.Values) > 2 {
        err := p.s.Set(string(req.Values[1]), req.Values[2])
        if err == nil {
            rsp.Kind = '+'
            rsp.Value = []byte("OK")
        } else {
            rsp.Kind = '-'
            rsp.Value = []byte(err.Error())
        }
    } else {
        rsp.Kind = '-'
        rsp.Value = E_INCOMPLETE
    }
}

func (p Proxy) proxyDel(req, rsp *redis.Message) {
    ks := make([]string, len(req.Values[1:]))
    for i, v := range req.Values[1:] {
        ks[i] = string(v)
    }
    n, _ := p.s.Del(ks...)
    rsp.Kind = ':'
    rsp.Integer = int64(n)
}

func (p Proxy) proxyMget(req, rsp *redis.Message) {
    ks := make([]string, len(req.Values[1:]))
    for i, v := range req.Values[1:] {
        ks[i] = string(v)
    }
    m, err := p.s.Mget(ks...)

    if err != nil {
        rsp.Kind = '-'
        rsp.Value = E_PROXY
    } else {
        rsp.Kind = '*'
        rsp.Values = make([][]byte, len(ks))
        for i, k := range ks {
            rsp.Values[i] = m[k]
        }
    }
}

func (p Proxy) proxyMset(req, rsp *redis.Message) {
    n := len(req.Values[1:])/2
    ks := make([]string, n)
    vs := make([][]byte, n)
    for i, v := range req.Values[1:] {
        if i % 2 == 0 {
            ks[i/2] = string(v)
        } else {
            vs[i/2] = v
        }
    }
    kvm := make(map[string][]byte, n)
    for i := 0; i < n; i++ {
        kvm[string(ks[i])] = vs[i]
    }

    m := p.s.Mset(kvm)
    for _, err := range m {
        if err != nil {
            rsp.Kind = '-'
            rsp.Value = []byte("MSET failed")
            return
        }
    }
    rsp.Kind = '+'
    rsp.Value = []byte("OK")
}

func (p Proxy) process(conn net.Conn) {
    defer func() {
        if e := recover(); e != nil {
            log.Printf("recovered from %q\n", e)
        }
    }()

    r := bufio.NewReader(conn)
    w := bufio.NewWriter(conn)

    for {
        req, err := redis.Parse(r)
        if err != nil {
            log.Printf("%s disconnected: %s\n", conn.RemoteAddr(), err)
            break
        } 

        reply := p.proxy(req)
        rs, _ := redis.Encode(reply)
        w.Write(rs)
        w.Flush()
    }
    conn.Close()
}

func server(socktype string, addr string) {
    l, err := net.Listen(socktype, addr)
    if err != nil {
        log.Fatalf("Failed to listen at %q", addr)
    }
    log.Printf("Listening on %s://%s\n", socktype, addr)

    proxy, err := ProxyOpen(
        "localhost:6379",
        "localhost:6379",
        "localhost:6379",
        "localhost:6379",
    )

    for {
        conn, err := l.Accept()
        if err != nil {
            log.Printf("Error accepting: %q\n", err)
        } else {
            log.Printf("%s connected\n", conn.RemoteAddr())
            go proxy.process(conn)
        }
    }
}
