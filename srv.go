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

    reply := redis.Parse(r)
    log.Printf("%q", reply)
    w.WriteString("-error\r\n")
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
