package redis

import (
    "bufio"
    "bytes"
    "fmt"
    "log"
    "net"
    "strconv"
)


var (
    DEBUG = true
)


type Redis struct {
    Addr    string
    conn    net.Conn
    r       *bufio.Reader
}


type Reply struct {
    Kind uint8
    Value []byte
    Integer int64
    Values [][]byte
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




func (r Redis) send(args...[]byte) {
    packed := pack(args...)
    if DEBUG {
        log.Printf("REQUEST> %q\n", packed)
    }
    r.conn.Write(packed)
}

func (r Redis) Mget(keys... string) map[string][]byte {
    args := make([][]byte, 1 + len(keys))
    args[0] = []byte("MGET")
    for i, k := range keys {
        args[i+1] = []byte(k)
    }
    r.send(args...)

    m := make(map[string][]byte)

    reply := r.parseReply()
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

    reply := r.parseReply()
    if DEBUG {
        log.Printf("%q\n", reply.Value)
    }
}


func (r Redis) Set(key string, value []byte) {
    args := [][]byte{[]byte("SET"), []byte(key), value}
    r.send(args...)
    reply := r.parseReply()
    if DEBUG {
        log.Printf(">>>%q", reply.Value)
    }
}


func (r Redis) Get(key string) []byte {
    args := [][]byte{[]byte("GET"), []byte(key)}
    r.send(args...)
    reply := r.parseReply()
    if DEBUG {
        log.Printf(">>>%q", reply.Value)
    }
    return reply.Value
}




// Pack requests using Redis Unified Protocol
func pack(args... []byte) []byte {
    var buf bytes.Buffer
    fmt.Fprintf(&buf, "*%d\r\n", len(args))
    for _, arg := range args {
        fmt.Fprintf(&buf, "$%d\r\n%s\r\n", len(arg), arg)
    }
    return buf.Bytes()
}


// Parse replies using Redis Unified Protocol
func (r Redis) parseReply() *Reply {
    reply := &Reply{}
    reply.Kind, _ = r.r.ReadByte()
    if DEBUG {
        log.Printf("Kind: %q", reply.Kind)
    }
    switch reply.Kind {
    case '+', '-', ':':
        reply.Value = r.parseSingleLineReply()
        if reply.Kind == ':' {  // Integer reply
            reply.Integer, _ = strconv.ParseInt(string(reply.Value), 10, 64)
        }
    case '$':
        reply.Value = r.parseBulkReply()
    case '*':
        reply.Values = r.parseMultiBulkReply()
    }
    return reply
}


func (r Redis) parseSingleLineReply() []byte {
    line, _, _ := r.r.ReadLine()    // trailing CRLF is removed
    if DEBUG {
        log.Printf("Single Line: %q", line)
    }
    return line
}

func (r Redis) parseBulkReply() []byte {
    line := r.parseSingleLineReply()
    l, _ := strconv.Atoi(string(line))
    if l >= 0 {
        read := make([]byte, l + 2) // trailing CRLF
        r.r.Read(read)
        if DEBUG {
            log.Printf("Bulk: %q", read[:l])
        }
        return read[:l]
    }
    return nil
}

func (r Redis) parseMultiBulkReply() [][]byte {
    line := r.parseSingleLineReply()
    cnt, _ := strconv.Atoi(string(line))
    res := make([][]byte, cnt)
    v := []byte{}
    for j := 0; j < cnt; j++ {
        kind, _ := r.r.ReadByte()
        if DEBUG {
            log.Printf("Kind: %q", kind)
        }
        if kind == '$' {
            v = r.parseBulkReply()
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
