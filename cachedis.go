package main

import (
    "bytes"
    "net"
    "fmt"
    "strconv"
)


const (
    BUFSIZE = 4<<10
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

func (r Redis) send(cmd string, args...string) {
    fmt.Fprintf(r.conn, "*%d\r\n$%d\r\n%s\r\n", 1+len(args), len(cmd), cmd)
    for _, arg := range args {
        fmt.Fprintf(r.conn, "$%d\r\n%s\r\n", len(arg), arg)
    }
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

func (r Redis) Mget(keys... string) map[string]string {
    r.send("MGET", keys...)
    m := make(map[string]string)
    rsp := r.recv()
    fmt.Printf("%s\n", rsp)

    kind := rsp[0]
    CRLF := []byte("\r\n")
    if kind == '*' {
        i := bytes.Index(rsp, CRLF)
        cnt, _ := strconv.Atoi(string(rsp[1:i]))
        rsp = rsp[i+2:]
        fmt.Printf("cnt = %d\n", cnt)

        for j := 0; j < cnt; j++ {
            kind = rsp[0]
            if kind == '$' {
                i = bytes.Index(rsp, CRLF)
                l, _ := strconv.Atoi(string(rsp[1:i]))
                if l >= 0 {
                    k, v := keys[j], rsp[i+2:i+2+l]
                    fmt.Printf(">>> %q => %q\n", k, v)
                    rsp = rsp[i+2+l+2:]
                    m[string(k)] = string(v)
                } else {
                    k := keys[j]
                    fmt.Printf(">>> %q => nil\n", k)
                    rsp = rsp[i+2:]
                }
            }
        }
    }
    return m
}


func main() {
    r := Open("")
    m := r.Mget("foo", "bar", "spam", "egg")
    fmt.Printf("\n\nmap ===> %v\n", m)
}
