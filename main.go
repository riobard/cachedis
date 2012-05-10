package main

import (
	"flag"
)

var addr = flag.String("addr", "localhost:5354", "Server address")

func init() {
	flag.Parse()
}

func main() {
    server("tcp", *addr)
}
