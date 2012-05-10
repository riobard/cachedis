package main

import (
	"net"
	"log"
	//"./shardis"
)


func process(conn net.Conn) {
    log.Printf("Processing %q \n", conn)
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
            go process(conn)
        }
    }
}
