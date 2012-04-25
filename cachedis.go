package main

import (
    "fmt"
    "flag"
    "./redis"
)


var addr = flag.String("addr", "localhost:6379", "http service address")

func main() {
    r := redis.Open(*addr)
    r.Mset(map[string][]byte{
        "foo": []byte("bar"),
        "spam": []byte("egg"),
    })
    m := r.Mget("foo", "bar", "spam", "egg", "blah", "ahah")
    fmt.Printf("===> %q\n", m)
}
