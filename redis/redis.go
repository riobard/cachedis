package redis

import (
    "bytes"
    "fmt"
    "log"
    "net"
    "strconv"
)

const (
    BUFSIZE = 4<<10
)


var (
    CRLF = []byte("\r\n")
    DEBUG = true
)


type Redis struct {
    Addr string
    conn net.Conn
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
    return r, nil
}


// Send arguments using Redis Unified Request Protocol
func pack(args... []byte) []byte {
    var buf bytes.Buffer
    fmt.Fprintf(&buf, "*%d\r\n", len(args))
    for _, arg := range args {
        fmt.Fprintf(&buf, "$%d\r\n%s\r\n", len(arg), arg)
    }
    return buf.Bytes()
}


type Reply struct {
    Kind uint8
    Value []byte
    Integer int64
    Values [][]byte
}

func parseReply(buf []byte) *Reply {
    r := &Reply{}

    kind := buf[0]
    buf = buf[1:]

    switch kind {
    case '+', '-', ':':
        r.Kind = kind
        r.Value = parseSingleLineReply(buf)
        if r.Kind == ':' {
            r.Integer, _ = strconv.ParseInt(string(r.Value), 10, 64)
        }
    case '$':
        r.Kind = kind
        r.Value, _ = parseBulkReply(buf)
    case '*':
        r.Kind = kind
        r.Values = parseMultiBulkReply(buf)
    default:
        r.Kind = kind
    }
    return r
}

func parseSingleLineReply(buf []byte) []byte {
    i := bytes.Index(buf, CRLF)
    return buf[:i]
}

func parseBulkReply(buf []byte) (read []byte, unread []byte) {
    i := bytes.Index(buf, CRLF)
    l, _ := strconv.Atoi(string(buf[:i]))
    if l >= 0 {
        return buf[i+2:i+2+l], buf[i+2+l+2:]
    }
    return nil, buf[i+2:]
}

func parseMultiBulkReply(buf []byte) [][]byte {
    i := bytes.Index(buf, CRLF)
    cnt, _ := strconv.Atoi(string(buf[:i]))
    res := make([][]byte, cnt)
    buf = buf[i+2:]
    v := []byte{}
    for j := 0; j < cnt; j++ {
        kind := buf[0]
        if kind == '$' {
            v, buf = parseBulkReply(buf[1:])
            res[j] = v
            if DEBUG {
                if v == nil {
                    log.Printf("parseBulkReply> result nil")
                } else {
                    log.Printf("parseBulkReply> result %q", v)
                }
            }
        }
    }
    return res
}



func (r Redis) send(args...[]byte) {
    packed := pack(args...)
    if DEBUG {
        log.Printf("REQUEST> %q\n", packed)
    }
    r.conn.Write(packed)
}

func (r Redis) recv() []byte {
    buf := make([]byte, BUFSIZE)
    n, err := r.conn.Read(buf)
    if err != nil {
        fmt.Println(err)
        return nil
    }
    return buf[:n]
}

func (r Redis) Mget(keys... string) map[string][]byte {
    args := make([][]byte, 1 + len(keys))
    args[0] = []byte("MGET")
    for i, k := range keys {
        args[i+1] = []byte(k)
    }
    r.send(args...)

    m := make(map[string][]byte)
    rsp := r.recv()
    if DEBUG {
        log.Printf("REPLY> %q\n", rsp)
    }

    reply := parseReply(rsp)
    if reply.Kind == '*' {
        for i, v := range reply.Values {
            if v != nil {
                m[keys[i]] = v
            }
        }
    }
    return m
}


func (r Redis) Mset(m map[string][]byte) {
    args := make([][]byte, 1 + 2 * len(m))
    args[0] = []byte("MSET")
    i := 1
    for k, v := range m {
        args[i], args[i+1] = []byte(k), v
        i += 2
    }
    r.send(args...)

    rsp := r.recv()
    reply := parseReply(rsp)
    if DEBUG {
        log.Printf(">>>%q\n", rsp)
        log.Printf("%q\n", reply.Value)
    }
}


func (r Redis) Set(key string, value []byte) {
    args := [][]byte{[]byte("SET"), []byte(key), value}
    r.send(args...)
    rsp := r.recv()
    reply := parseReply(rsp)
    if DEBUG {
        log.Printf(">>>%q\n", rsp)
        log.Printf(">>>%q", reply.Value)
    }
}


func (r Redis) Get(key string) []byte {
    args := [][]byte{[]byte("GET"), []byte(key)}
    r.send(args...)
    rsp := r.recv()
    reply := parseReply(rsp)
    if DEBUG {
        log.Printf(">>>%q\n", rsp)
        log.Printf(">>>%q", reply.Value)
    }
    return reply.Value
}
