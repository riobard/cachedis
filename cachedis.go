package main

import (
	"./redis"
	"./shardis"
	"flag"
	"fmt"
	"time"
)

var addr = flag.String("addr", "localhost:6379", "http service address")

func init() {
	flag.Parse()
}

func main() {
    c := shardis.Open([]string{
        "localhost:6379", 
        "localhost:6379", 
        "localhost:6379", 
        "localhost:6379", 
    })

    N := 1000
    m := make(map[string][]byte, N)

    keys := []string{}
    for i := 0; i < N; i++ {
        k := fmt.Sprintf("uid:%d", i)
        keys = append(keys, k)
        v := make([]byte, 4<<10)
        m[k] = v
    }

    msett0 := time.Now().UnixNano()
    msetres := c.Mset(m)
    msettd := time.Now().UnixNano() - msett0
    fmt.Printf("MSET (%d): %.3f ms\n", len(msetres), float64(msettd)/1e6)

    mgett0 := time.Now().UnixNano()
    mgetres := c.Mget(keys...)
    mgettd := time.Now().UnixNano() - mgett0
    fmt.Printf("MGET (%d): %.3f ms\n", len(mgetres), float64(mgettd)/1e6)
}
