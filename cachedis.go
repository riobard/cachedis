package main

import (
    "fmt"
    "./redis"
)



func main() {
    r := redis.Open(":6400")
    r.Mset(map[string][]byte{
        "blah": []byte("blur"),
        "ahah": []byte("ahey"),
    })
    m := r.Mget("foo", "bar", "spam", "egg", "blah", "ahah")
    fmt.Printf("===> %q\n", m)
}
