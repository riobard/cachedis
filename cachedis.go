package main

import (
    "fmt"
    "flag"
    "./redis"
)


var addr = flag.String("addr", "localhost:6379", "http service address")

func main() {
    flag.Parse()
    r, err := redis.Open(*addr)
    if err != nil {
        fmt.Println("Failed to connect")
        return
    }
    r.Mset(map[string][]byte{
        "foo": []byte("bar"),
        "spam": []byte("egg"),
    })
    m := r.Mget("foo", "spam", "hello", "blah", "ahah")
    fmt.Printf("===> %q\n", m)

    r.Set("hello", []byte(""))
    fmt.Printf("hello = %q\n", r.Get("hello"))
    fmt.Printf("hell = %q\n", r.Get("hell"))
}
