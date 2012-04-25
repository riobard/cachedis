package redis

import (
    "bytes"
    "fmt"
    "log"
    "net"
    "strconv"
)

const (
    DEBUG = true
    BUFSIZE = 4<<10
)


var (
    CRLF = []byte("\r\n")
)


type Redis struct {
    Addr string
    conn net.Conn
}


func Open(addr string) *Redis {
    if addr == "" {
        addr = "localhost:6379"
    }
    r := &Redis{Addr: addr}
    conn, err := net.Dial("tcp", addr)
    if err != nil {
        fmt.Println("Failed to connect")
        return nil
    }
    r.conn = conn
    return r
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


func parseReply(buf []byte) {
    switch buf[0] {
    case '+':
        parseStatusReply(buf[1:])
    /*
    case '-':
        parseErrorReply(buf[1:])
    case ':':
        parseIntergerReply(buf[1:])
    case '$':
        parseBulkReply(buf[1:])
    case '*':
        parseMultiBulkReply(buf[1:])
    default:
        parseError()
    */
    }
}

func parseStatusReply(buf []byte) []byte {
    i := bytes.Index(buf, CRLF)
    return buf[:i]
}

func parseBulkReply(buf []byte) (read []byte, pos int) {
    i := bytes.Index(buf, CRLF)
    l, _ := strconv.Atoi(string(buf[:i]))
    if l > 0 {
        read = buf[i+2:i+2+l]
        return read, i+2+l+3
    }
    return nil, i+3
}


func parseMultiBulkReply(buf []byte) [][]byte {
    i := bytes.Index(buf, CRLF)
    cnt, _ := strconv.Atoi(string(buf[:i]))
    res := make([][]byte, cnt)
    buf = buf[i+2:]
    for j := 0; j < cnt; j++ {
        kind := buf[0]
        if kind == '$' {
            v, pos := parseBulkReply(buf[1:])
            res[j] = v
            if DEBUG {
                if v == nil {
                    log.Printf("parseBulkReply> result nil")
                } else {
                    log.Printf("parseBulkReply> result %q", v)
                }
            }
            buf = buf[pos:]
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

    kind := rsp[0]
    if kind == '*' {
        for i, v := range parseMultiBulkReply(rsp[1:]) {
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

    buf := make([]byte, 50)
    r.conn.Read(buf)
    if DEBUG {
        log.Printf("%q\n", buf)
        log.Printf("%q\n", parseStatusReply(buf))
    }
}
