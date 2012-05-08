package main

import (
	"./shardis"
	"flag"
)

var addr = flag.String("addr", "localhost:5354", "Server address")

func init() {
	flag.Parse()
}

func main() {
    c, _ := shardis.Open([]string{
        "localhost:6379", 
        "localhost:6379", 
        "localhost:6379", 
        "localhost:6379", 
    })
}
