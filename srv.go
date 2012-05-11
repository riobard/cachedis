package main

import (
	"./redis"
	"net"
	"log"
	"bufio"
)


func process(conn net.Conn) {
    defer func() {
        if e := recover(); e != nil {
            log.Printf("recovered from %q\n", e)
        }
    }()

    r := bufio.NewReader(conn)
    w := bufio.NewWriter(conn)

    req, err := redis.Parse(r)
    if err != nil {
        log.Printf("err: %q\n", err)
        return
    } 

    log.Printf("%q\n", req)
    //w.WriteString("$3\r\nbar\r\n")
    w.WriteString("$0\r\n")
    w.Flush()

    conn.Close()
}

func server(socktype string, addr string) {
    l, err := net.Listen(socktype, addr)
    if err != nil {
        log.Fatalf("Failed to listen at %q", addr)
    }
    log.Printf("Listening on %s://%s\n", socktype, addr)

    for {
        conn, err := l.Accept()
        if err != nil {
            log.Printf("Error accepting: %q\n", err)
        } else {
            log.Printf("%s connected\n", conn.RemoteAddr())
            go process(conn)
        }
    }
}
